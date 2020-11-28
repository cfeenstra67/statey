import copy
import dataclasses as dc
import sys
import textwrap as tw
from itertools import chain, product
from typing import Any, Union, Iterator, Sequence, Callable

import networkx as nx
from networkx.algorithms.dag import (
    is_directed_acyclic_graph,
    topological_sort,
)

import statey as st
from statey import exc
from statey.syms import types, utils, session, impl, Object, stack


class PythonNamespace(session.Namespace):
    """
    Pure python namespace implementation
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.types = {}

    def new(self, key: str, type: types.Type) -> Object:
        """
        Create a new symbol for the given key and schema and add it to the current namespace
        """
        self.path_parser.validate_name(key)
        if key in self.types:
            raise exc.DuplicateSymbolKey(key, self)
        self.types[key] = type
        ref = self.ref(key)
        return Object(ref, frame=stack.frame_snapshot(1))

    def delete(self, key: str) -> None:
        self.resolve(key)
        del self.types[key]

    def keys(self) -> Iterator[str]:
        return self.types.keys()

    def resolve(self, key: str) -> types.Type:
        """
        Get the type of the given key, raising an error if it is not in the current schema
        """
        base, *rel_path = self.path_parser.split(key)
        if base not in self.types:
            raise exc.SymbolKeyError(key, self)

        base_semantics = self.registry.get_semantics(self.types[base])
        try:
            semantics = base_semantics.path_semantics(rel_path)
        except AttributeError as err:
            raise exc.SymbolKeyError(key, self) from err

        return semantics.type

    def clone(self) -> session.Namespace:
        new_inst = PythonNamespace(self.registry, self.path_parser)
        new_inst.types = self.types.copy()
        return new_inst


@dc.dataclass
class ResolutionStack:
    """
    A resolution stack object contains logic to format stack information
    for resolution errors
    """

    dag: nx.DiGraph
    resolving_obj_id: Any
    current_obj_stack: Sequence[Any] = ()

    def __post_init__(self) -> None:
        self.current_obj_stack = list(self.current_obj_stack)

    def get_object(self, symbol_id: Any) -> Object:
        return self.dag.nodes[symbol_id]["symbol"]

    def get_object_stack(self) -> Sequence[stack.FrameSnapshot]:
        """
        Get a list of FrameSnapshots for objects between the resolving object and the error object
        in the DAG.
        """
        if not self.current_obj_stack:
            obj = self.get_object()
            return [(obj._frame, [obj])]

        frame_path = []
        objects = []
        last_frame = None

        full_path = list(
            reversed(
                nx.shortest_path(
                    self.dag, self.current_obj_stack[0], self.resolving_obj_id
                )
            )
        )
        if not full_path:
            full_path.append(self.current_obj_stack[0])
        else:
            full_path.extend(self.current_obj_stack[1:])

        for obj_id in full_path:
            obj = self.get_object(obj_id)
            frame = obj._frame
            if frame != last_frame:
                if objects:
                    frame_path.append((last_frame, objects))
                objects = []

            objects.append(obj)
            last_frame = frame

        if last_frame and objects:
            frame_path.append((last_frame, objects))

        return frame_path

    def push(self, obj_id: Any) -> None:
        """"""
        self.current_obj_stack.append(obj_id)

    def pop(self) -> None:
        """"""
        self.current_obj_stack.pop()


class PythonSession(session.Session):
    """
    A pure python session implementation for resolving objects.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.data = {}

    def set_data(self, key: str, data: Any) -> None:
        """
        Set the given data for the given key.
        """
        typ = self.ns.resolve(key)

        encoder = self.ns.registry.get_encoder(typ, serializable=True)
        try:
            encoded_data = encoder.encode(data)

            root, *rel_path = self.ns.path_parser.split(key)
            if rel_path:
                raise ValueError("Data can only be set on the root level.")
        except Exception as err:
            raise exc.InvalidDataError(key, err) from err

        self.data[key] = encoded_data

    def delete_data(self, key: str) -> None:
        if key not in self.data:
            raise exc.SymbolKeyError(key, self.ns)
        del self.data[key]

    def get_encoded_data(self, key: str) -> Any:
        """
        Return the data or symbol at the given key, with a boolean is_symbol also returned indicating
        whether that key points to a symbol. If the key has not been set, syms.exc.MissingDataError
        will be raised
        """
        typ = self.ns.resolve(key)
        base, *rel_path = self.ns.path_parser.split(key)
        if base not in self.data:
            raise exc.MissingDataError(key, typ, self)

        base_type = self.ns.resolve(base)
        base_semantics = self.ns.registry.get_semantics(base_type)
        value = self.data[base]
        for idx, attr in enumerate(rel_path):
            try:
                value = base_semantics.get_attr(value, attr)
            except (KeyError, AttributeError) as err:
                sub_path = [base] + list(rel_path)[: idx + 1]
                sub_key = self.ns.path_parser.join(sub_path)
                raise exc.MissingDataError(key, base_type, self) from err
            finally:
                base_semantics = base_semantics.attr_semantics(attr)

        return value

    @stack.resolutionstart
    def _build_symbol_dag(
        self,
        symbol: Object,
        graph: nx.DiGraph,
        resolution_stack: ResolutionStack,
        check_dag: bool = True,
    ) -> nx.DiGraph:

        syms = [(symbol, ())]
        added = set()

        while syms:
            sym, downstreams = syms.pop(0)
            resolution_stack.push(sym._impl.id)
            try:
                processed = False
                if sym._impl.id in graph.nodes:
                    processed = True
                else:
                    graph.add_node(sym._impl.id, symbol=sym)
                    added.add(sym._impl.id)

                for symbol_id in downstreams:
                    if (sym._impl.id, symbol_id) not in graph.edges:
                        graph.add_edge(sym._impl.id, symbol_id)
                        added.add(symbol_id)
                        added.add(sym._impl.id)

                if processed:
                    continue

                for upstream in sym._inst.depends_on(self):
                    syms.append((upstream, (sym._impl.id,)))
            except Exception as err:
                _, _, tb = sys.exc_info()
                raise exc.ResolutionError(resolution_stack, err, tb) from err
            except BaseException:
                raise
            else:
                resolution_stack.pop()

        if check_dag and added:
            # Check that this is in fact a DAG and has no cycles
            self._raise_not_dag(graph, list(added))

        return added

    def _raise_not_dag(self, graph: nx.DiGraph, sources=None) -> None:
        try:
            nx.find_cycle(graph, sources)
        except nx.NetworkXNoCycle:
            pass
        else:
            raise ValueError(f"Graph is not a DAG!")

    @stack.internalcode
    def _process_symbol_dag(
        self,
        obj_id: Any,
        dag: nx.DiGraph,
        resolution_stack: ResolutionStack,
        allow_unknowns: bool = False,
    ) -> None:

        # @stack.internalcode
        @stack.resolutionstart
        def handle_symbol_id(symbol_id):

            sym = dag.nodes[symbol_id]["symbol"]
            if "result" in dag.nodes[symbol_id]:
                return dag.nodes[symbol_id]["result"]

            try:
                result = sym._inst.apply(dag, self)
            except exc.UnknownError as err:
                if not allow_unknowns:
                    raise
                if err.expected is utils.MISSING:
                    result = Object(impl.Unknown(sym))
                else:
                    expected = Object(err.expected, sym._type, sym._registry)
                    result = self.resolve(expected, decode=False, allow_unknowns=True)
            else:
                semantics = self.ns.registry.get_semantics(sym._type)
                expanded_result = semantics.expand(result)

                def resolve_child(x):
                    added = self._build_symbol_dag(
                        x, dag, resolution_stack, check_dag=False
                    )
                    if (x._impl.id, symbol_id) not in dag.edges:
                        dag.add_edge(x._impl.id, symbol_id)
                        self._raise_not_dag(dag, list(added | {x._impl.id}))
                    elif added:
                        self._raise_not_dag(dag, list(added))

                    ancestors = set(dag.pred[x._impl.id])
                    for node in list(ancestors):
                        if "result" in dag.nodes[node]:
                            ancestors.remove(node)

                    for sym_id in list(topological_sort(dag.subgraph(ancestors))):
                        wrapped_handle_symbol_id(sym_id)

                    return wrapped_handle_symbol_id(x._impl.id)

                result = semantics.map_objects(resolve_child, expanded_result)

            dag.nodes[symbol_id]["result"] = result
            return result

        @stack.internalcode
        def wrapped_handle_symbol_id(symbol_id):
            exception, tb = None, None
            resolution_stack.push(symbol_id)
            try:
                return handle_symbol_id(symbol_id)
            except Exception as err:
                _, _, tb = sys.exc_info()
                exception = exc.ResolutionError(resolution_stack, err, tb)
            except BaseException:
                raise
            else:
                resolution_stack.pop()
            raise exception.with_traceback(tb)

        ancestors = set(nx.ancestors(dag, obj_id)) | {obj_id}
        for symbol_id in list(topological_sort(dag)):
            if symbol_id not in ancestors:
                continue
            wrapped_handle_symbol_id(symbol_id)

    def _digraph(self) -> nx.DiGraph:
        return nx.DiGraph()

    def resolve(
        self, symbol: Object, allow_unknowns: bool = False, decode: bool = True
    ) -> Any:

        graph = self._digraph()

        resolution_stack = ResolutionStack(graph, symbol._impl.id)
        with stack.rewrite_ctx(resolution_stack):
            self._build_symbol_dag(symbol, graph, resolution_stack)
            self._process_symbol_dag(
                symbol._impl.id, graph, resolution_stack, allow_unknowns
            )

        encoded = graph.nodes[symbol._impl.id]["result"]
        if not decode:
            return encoded

        encoder = self.ns.registry.get_encoder(symbol._type)
        return encoder.decode(encoded)

    def dependency_graph(self) -> nx.MultiDiGraph:
        graph = nx.MultiDiGraph()

        dag = self._digraph()

        key = None
        obj_ids = set()
        for key in self.ns.keys():
            ref = self.ns.ref(key)
            resolution_stack = ResolutionStack(dag, ref._impl.id)
            with stack.rewrite_ctx(resolution_stack):
                self._build_symbol_dag(ref, dag, resolution_stack, check_dag=False)
                self._process_symbol_dag(
                    ref._impl.id, dag, resolution_stack, allow_unknowns=True
                )

            obj_ids.add(ref._impl.id)

        self._raise_not_dag(graph, list(obj_ids))

        # Make a copy of `dag` since it will be mutated
        dag = dag.copy()

        ref_symbol_ids = set()

        for node in dag.nodes:
            if isinstance(dag.nodes[node]["symbol"]._impl, impl.Reference):
                ref_symbol_ids.add(node)

        utils.subgraph_retaining_dependencies(dag, ref_symbol_ids)

        for node in list(nx.topological_sort(dag)):
            ref = dag.nodes[node]["symbol"]

            base, *rel_path = self.ns.path_parser.split(ref._impl.path)
            graph.add_node(base)

            for pred in dag.pred[node]:
                pref_ref = dag.nodes[pred]["symbol"]
                pred_base, *pred_rel_path = self.ns.path_parser.split(
                    pref_ref._impl.path
                )

                graph.add_edge(
                    pred_base, base, from_path=pred_rel_path, to_path=rel_path
                )

        return graph

    def clone(self) -> session.Session:
        new_inst = PythonSession(self.ns.clone())
        # Since data can only be set at the root level and is immutable while in the
        # session, a shallow copy works fine here.
        new_inst.data = self.data.copy()
        for plugin in self.pm.get_plugins():
            if plugin is not self:
                new_inst.pm.register(plugin)
        return new_inst


class CachingPythonSession(PythonSession):
    """
    PythonSession that holds a single graph per instance
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.graph = nx.DiGraph()

    def clone(self) -> session.Session:
        new_inst = super().clone()
        new_inst.graph = self.graph.copy()
        return new_inst

    def _invalidate_symbol(self, obj_id: Any) -> None:
        if obj_id not in self.graph.nodes:
            return

        for node in list(nx.descendants(self.graph, obj_id)):
            if "result" in self.graph.nodes[node]:
                del self.graph.nodes["result"]
            # self.graph.remove_node(node)

        self.graph.remove_node(obj_id)

    def _clean_unknowns(self) -> None:
        for node in list(self.graph.nodes):
            if node not in self.graph.nodes:
                continue
            data = self.graph.nodes[node]
            if "result" in data and isinstance(data["result"], st.Object):
                self._invalidate_symbol(node)

    def resolve(
        self, symbol: Object, allow_unknowns: bool = False, decode: bool = True
    ) -> Any:
        res = super().resolve(symbol, allow_unknowns, decode)
        if allow_unknowns:
            self._clean_unknowns()
        return res

    def set_data(self, key: str, data: Any) -> None:
        invalidate = False
        if key in self.data:
            try:
                current = self.resolve(self.ns.ref(key))
            except st.exc.ResolutionError:
                invalidate = True

            if not invalidate and isinstance(data, Object):
                try:
                    new_value = self.resolve(data)
                except st.exc.ResolutionError:
                    invalidate = True
                else:
                    invalidate = new_value != current

        if invalidate:
            self._invalidate_symbol(self.ns.ref(key)._impl.id)

        super().set_data(key, data)

    def delete_data(self, key: str) -> None:
        super().delete_data(key, data)
        self._invalidate_key(key)


def create_session() -> session.Session:
    """
    Default factory for creating the best session given the runtime
    """
    return CachingPythonSession(PythonNamespace())
