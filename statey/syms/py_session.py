import copy
from itertools import chain, product
from typing import Any, Union, Iterator

import networkx as nx
from networkx.algorithms.dag import (
    is_directed_acyclic_graph,
    topological_sort,
)

from statey import exc
from statey.syms import types, symbols, utils, session


class PythonNamespace(session.Namespace):
    """
	Pure python namespace implementation
	"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.types = {}

    def new(self, key: str, type: types.Type) -> symbols.Symbol:
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

        encoder = self.ns.registry.get_encoder(typ)
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

    def _build_symbol_dag(
        self, symbol: symbols.Symbol, graph: nx.DiGraph
    ) -> nx.DiGraph:

        syms = [(symbol, ())]

        while syms:
            sym, downstreams = syms.pop(0)
            processed = False
            if sym.symbol_id in graph.nodes:
                processed = True
            else:
                graph.add_node(sym.symbol_id, symbol=sym)

            for symbol_id in downstreams:
                graph.add_edge(sym.symbol_id, symbol_id)

            if processed:
                continue

            for upstream in sym._upstreams(self):
                syms.append((upstream, (sym.symbol_id,)))

        # Check that this is in fact a DAG and has no cycles
        if not is_directed_acyclic_graph(graph):
            raise ValueError(f"{graph} is not a DAG.")

        return graph

    def _process_symbol_dag(
        self, dag: nx.DiGraph, allow_unknowns: bool = False
    ) -> None:

        def handle_symbol_id(symbol_id):

            sym = dag.nodes[symbol_id]["symbol"]
            if 'result' in dag.nodes[symbol_id]:
                return dag.nodes[symbol_id]['result']

            try:
                result = sym._apply(dag, self)
            except exc.UnknownError as err:
                if not allow_unknowns:
                    raise
                if err.expected is not utils.MISSING:
                    result = err.expected
                else:
                    result = symbols.Unknown(sym)
            else:
                semantics = self.ns.registry.get_semantics(sym.type)
                expanded_result = semantics.expand(result)

                def resolve_child(x):
                    if not isinstance(x, symbols.Symbol) or x is sym:
                        return x

                    self._build_symbol_dag(x, dag)
                    dag.add_edge(x.symbol_id, symbol_id)

                    ancestors = set(nx.ancestors(dag, x.symbol_id))
                    for sym_id in topological_sort(dag.subgraph(ancestors)):
                        handle_symbol_id(sym_id)

                    return handle_symbol_id(x.symbol_id)

                result = semantics.map(resolve_child, expanded_result)

            dag.nodes[symbol_id]['result'] = result
            return result

        for symbol_id in list(topological_sort(dag)):
            handle_symbol_id(symbol_id)

    def resolve(
        self, symbol: symbols.Symbol, allow_unknowns: bool = False, decode: bool = True
    ) -> Any:

        graph = nx.DiGraph()
        dag = self._build_symbol_dag(symbol, graph)
        self._process_symbol_dag(dag, allow_unknowns)
        encoded = dag.nodes[symbol.symbol_id]["result"]
        if not decode:
            return encoded
        encoder = self.ns.registry.get_encoder(symbol.type)
        return encoder.decode(encoded)

    def dependency_graph(self) -> nx.MultiDiGraph:
        graph = nx.MultiDiGraph()

        dependent_symbols = []

        dag = nx.DiGraph()

        for key in self.ns.keys():
            ref = self.ns.ref(key)
            self._build_symbol_dag(ref, dag)

        self._process_symbol_dag(dag, allow_unknowns=True)

        ref_symbol_ids = set()

        for node in dag.nodes:
            if isinstance(dag.nodes[node]['symbol'], symbols.Reference):
                ref_symbol_ids.add(node)

        utils.subgraph_retaining_dependencies(dag, ref_symbol_ids)

        for node in list(nx.topological_sort(dag)):
            ref = dag.nodes[node]['symbol']

            base, *rel_path = self.ns.path_parser.split(ref.path)
            graph.add_node(base)

            for pred in dag.pred[node]:
                pref_ref = dag.nodes[pred]['symbol']
                pred_base, *pred_rel_path = self.ns.path_parser.split(pref_ref.path)

                graph.add_edge(pred_base, base, from_path=pred_rel_path, to_path=rel_path)

        return graph

    def clone(self) -> session.Session:
        new_inst = copy.copy(self)
        # Since data can only be set at the root level and is immutable while in the
        # session, a shallow copy works fine here.
        new_inst.data = new_inst.data.copy()
        return new_inst


def create_session() -> session.Session:
    """
	Default factory for creating the best session given the runtime
	"""
    return PythonSession(PythonNamespace())
