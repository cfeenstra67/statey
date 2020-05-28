import copy
from itertools import chain
from typing import Any, Union, Iterator

import networkx as nx
from networkx.algorithms.dag import (
    is_directed_acyclic_graph,
    topological_sort,
)

from statey.syms import types, symbols, exc, utils, session


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
        for attr in rel_path:
            base_semantics = base_semantics.attr_semantics(attr)
            if base_semantics is None:
                raise exc.SymbolKeyError(key, self)

        return base_semantics.type


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
        self, symbol: symbols.Symbol, allow_unknowns: bool = False
    ) -> nx.DiGraph:

        graph = nx.DiGraph()
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

            def collect_symbols(value):
                if isinstance(value, symbols.Symbol):
                    syms.append((value, (sym.symbol_id,)))
                return value

            semantics = self.ns.registry.get_semantics(sym.type)

            if isinstance(sym, symbols.Literal):
                semantics.map(collect_symbols, sym.value)
            elif isinstance(sym, symbols.Reference):
                value = self.get_encoded_data(sym.path)
                semantics.map(collect_symbols, value)
            elif isinstance(sym, symbols.Function):
                for sub_sym in chain(sym.args, sym.kwargs.values()):
                    syms.append((sub_sym, (sym.symbol_id,)))
            elif isinstance(sym, (symbols.Future, symbols.Unknown)):
                continue
            else:
                raise TypeError(f"Invalid symbol {sym}")

        # Check that this is in fact a DAG and has no cycles
        if not is_directed_acyclic_graph(graph):
            raise ValueError(f"{graph} is not a DAG.")

        return graph

    def _process_symbol_dag(
        self, dag: nx.DiGraph, allow_unknowns: bool = False
    ) -> None:
        def resolve_future(future):
            try:
                return future.get_result()
            except exc.FutureResultNotSet:
                if not allow_unknowns:
                    raise
                if future.expected is not utils.MISSING:
                    return future.expected
                return symbols.Unknown(sym, refs=future.refs)

        def resolve_unknown(unknown):
            if not allow_unknowns:
                raise exc.SessionError(f"Found an Unknown: {unknown} while resolving.")
            return unknown

        for symbol_id in topological_sort(dag):

            sym = dag.nodes[symbol_id]["symbol"]

            if isinstance(sym, symbols.Literal):
                value = sym.value

            elif isinstance(sym, symbols.Reference):
                value = self.get_encoded_data(sym.path)

            elif isinstance(sym, symbols.Function):
                args = []
                is_unknown = False
                unknown_refs = []
                for sub_sym in sym.args:
                    arg = dag.nodes[sub_sym.symbol_id]["result"]
                    if isinstance(arg, symbols.Unknown):
                        is_unknown = True
                        unknown_refs.extend(arg.refs)
                        continue
                    args.append(arg)
                kwargs = {}
                for key, sub_sym in sym.kwargs.items():
                    arg = dag.nodes[sub_sym.symbol_id]["result"]
                    if isinstance(arg, symbols.Unknown):
                        is_unknown = True
                        unknown_refs.extend(arg.refs)
                        continue
                    kwargs[key] = arg

                if is_unknown:
                    value = symbols.Unknown(sym, refs=tuple(unknown_refs))
                else:
                    value = sym.func(*args, **kwargs)

            elif isinstance(sym, symbols.Future):
                value = resolve_future(sym)
            elif isinstance(sym, symbols.Unknown):
                value = resolve_unknown(sym)
            else:
                raise TypeError(f"Invalid symbol {sym}.")

            def resolve_value(x):
                if isinstance(x, symbols.Future):
                    return resolve_future(x)
                if isinstance(x, symbols.Unknown):
                    return resolve_unknown(x)
                if isinstance(x, symbols.Symbol):
                    return dag.nodes[x.symbol_id]["result"]
                return x

            semantics = self.ns.registry.get_semantics(sym.type)
            resolved = semantics.map(resolve_value, value)
            dag.nodes[symbol_id]["result"] = resolved

    def resolve(
        self, symbol: symbols.Symbol, allow_unknowns: bool = False, decode: bool = True
    ) -> Any:
        dag = self._build_symbol_dag(symbol)
        self._process_symbol_dag(dag, allow_unknowns)
        encoded = dag.nodes[symbol.symbol_id]["result"]
        if not decode:
            return encoded
        encoder = self.ns.registry.get_encoder(symbol.type)
        return encoder.decode(encoded)

    def dependency_graph(self) -> nx.MultiDiGraph:
        graph = nx.MultiDiGraph()

        dependent_symbols = []

        def add_deps(path):
            def handle(x):
                if isinstance(x, symbols.Symbol):
                    dependent_symbols.append((x, path))
                return x

            return handle

        for key in self.ns.keys():
            try:
                data = self.get_encoded_data(key)
            except exc.SymbolKeyError:
                continue

            typ = self.ns.resolve(key)
            semantics = self.ns.registry.get_semantics(typ)

            semantics.map(add_deps(key), data)
            graph.add_node(key)

        while dependent_symbols:
            symbol, dependent_path = dependent_symbols.pop(0)

            if isinstance(symbol, symbols.Literal):
                semantics = self.ns.registry.get_semantics(symbol.type)
                semantics.map(add_deps(dependent_path), symbol.value)
            elif isinstance(symbol, symbols.Reference):
                base, *rel_path = self.ns.path_parser.split(symbol.path)
                graph.add_edge(base, dependent_path, path=rel_path)
            elif isinstance(symbol, symbols.Function):
                for sub_sym in chain(symbol.args, symbol.kwargs.values()):
                    dependent_symbols.append((sub_sym, dependent_path))
            elif isinstance(symbol, (symbols.Unknown, symbols.Future)):
                for sub_sym in symbol.refs:
                    dependent_symbols.append((sub_sym, dependent_path))
            else:
                raise TypeError(f"Invalid symbol {sym}")

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
