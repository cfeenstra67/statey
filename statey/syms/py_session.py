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
        return self.ref(key)

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


@dc.dataclass(frozen=True)
class ResolutionStack:
    """
    A resolution stack object contains logic to format stack information
    for resolution errors
    """

    dag: nx.DiGraph
    symbol_id: Any

    def get_object(self, symbol_id: Any) -> Object:
        return self.dag.nodes[symbol_id]["symbol"]

    def _format_dag_stack(
        self,
        dag: nx.DiGraph,
        start: Any,
        repr: Callable[[Any], str] = repr,
        parent_size: int = 0,
        depth: int = 10,
    ) -> Sequence[str]:
        sym = self.get_object(start)
        lines = []
        successors = list(dag.succ[start])
        child_parent_size = len(successors)
        fmt = (lambda x: tw.indent(x, "  ")) if parent_size > 1 else (lambda x: x)
        if len(successors) > depth:
            lines.append(f"({len(successors) - depth} more objects collapsed...)")

        for sym_id in successors[:depth]:
            sym_stack = self._format_dag_stack(
                dag, sym_id, repr, child_parent_size, depth - 1
            )
            lines.append(fmt(sym_stack))
        prefix = "-" if child_parent_size > 0 else "*"
        lines.append(f"{prefix} {repr(sym)}")
        return "\n".join(lines)

    def format_dag_stack(
        self, dag: nx.DiGraph, repr: Callable[[Any], str] = repr, depth: int = 3
    ) -> str:
        """
        Format the given DAG into a stack string
        """
        return self._format_dag_stack(dag, self.symbol_id, repr=repr, depth=depth)

    def get_stack_dag(self) -> nx.DiGraph:
        """
        Get the part of the DAG we want to visualize in the stack
        """
        descendants = list(nx.descendants(self.dag, self.symbol_id)) + [self.symbol_id]
        dag_copy = self.dag.copy()
        try:
            utils.subgraph_retaining_dependencies(dag_copy, descendants)
        except nx.NetworkXUnfeasible:
            dag_copy = dag_copy.subgraph(descendants)
        return dag_copy

    def format_stack(self, repr: Callable[[Any], str] = repr, depth: int = 3) -> str:
        """
        Format this resolution stack into a string
        """
        dag = self.get_stack_dag()
        return self.format_dag_stack(dag, repr=repr, depth=depth)


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
        encoded_data = encoder.encode(data)

        root, *rel_path = self.ns.path_parser.split(key)
        if rel_path:
            raise ValueError("Data can only be set on the root level.")
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

    def _build_symbol_dag(self, symbol: Object, graph: nx.DiGraph, check_dag: bool = True) -> nx.DiGraph:

        syms = [(symbol, ())]
        added = set()

        while syms:
            sym, downstreams = syms.pop(0)
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
            raise ValueError(f'Graph is not a DAG!')

    @stack.internalcode
    def _process_symbol_dag(
        self, dag: nx.DiGraph, allow_unknowns: bool = False
    ) -> None:
        @stack.internalcode
        def handle_symbol_id(symbol_id):
            # if symbol_id in stack:
            #     raise ValueError('circular reference detected')

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
                    added = self._build_symbol_dag(x, dag, check_dag=False)
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
            try:
                return handle_symbol_id(symbol_id)
            except Exception as err:
                _, _, tb = sys.exc_info()
                resolution_stack = ResolutionStack(dag, symbol_id)
                exception = exc.ResolutionError(resolution_stack, err, tb)
            raise exception.with_traceback(tb)

        for symbol_id in list(topological_sort(dag)):
            wrapped_handle_symbol_id(symbol_id)

    def _digraph(self) -> nx.DiGraph:
        return nx.DiGraph()

    def resolve(
        self, symbol: Object, allow_unknowns: bool = False, decode: bool = True
    ) -> Any:

        graph = self._digraph()
        self._build_symbol_dag(symbol, graph)

        try:
            self._process_symbol_dag(graph, allow_unknowns)
        except Exception:
            stack.rewrite_tb(*sys.exc_info())

        encoded = graph.nodes[symbol._impl.id]["result"]
        if not decode:
            return encoded

        encoder = self.ns.registry.get_encoder(symbol._type)
        return encoder.decode(encoded)

    def dependency_graph(self) -> nx.MultiDiGraph:
        graph = nx.MultiDiGraph()

        dag = self._digraph()

        for key in self.ns.keys():
            ref = self.ns.ref(key)
            self._build_symbol_dag(ref, dag, check_dag=False)

        self._raise_not_dag(graph)

        try:
            self._process_symbol_dag(dag, allow_unknowns=True)
        except Exception:
            stack.rewrite_tb(*sys.exc_info())

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
