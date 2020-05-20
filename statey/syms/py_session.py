import abc
import dataclasses as dc
from functools import partial
from itertools import chain
from typing import Dict, Any, Optional, Tuple, Union, Sequence

import marshmallow as ma
import networkx as nx
from networkx.algorithms.dag import is_directed_acyclic_graph, topological_sort

from statey.syms import types, symbols, exc, utils, path, session


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
		return symbols.Reference(key, type, self)

	def resolve(self, key: str) -> types.Type:
		"""
		Get the type of the given key, raising an error if it is not in the current schema
		"""
		base, *rel_path = self.path_parser.split(key)
		if base not in self.types:
			raise exc.SymbolKeyError(key, self)

		base_type = self.types[base]
		for attr in rel_path:
			semantics = self.registry.get_semantics(base_type)
			base_type = semantics.attr_type(attr)
			if base_type is None:
				raise exc.SymbolKeyError(key, self)

		return base_type


@dc.dataclass(frozen=True)
class Unknown:
	"""
	Some value that is as-yet unknown. It may or may not be known at some
	time in the future. Note this is NOT a symbol
	"""
	symbol: symbols.Symbol


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
		encoder = self.registry.get_encoder(typ)
		data = encoder.encode(data)
		utils.dict_set_path(self.data, self.ns.path_parser.split(key), data)

	def get_encoded_data(self, key: str, allow_unknowns: bool = False) -> Any:
		"""
		Return the data or symbol at the given key, with a boolean is_symbol also returned indicating
		whether that key points to a symbol. If the key has not been set, syms.exc.MissingDataError
		will be raised
		"""
		typ = self.ns.resolve(key)
		base, *rel_path = self.ns.path_parser.split(key)
		if base not in self.data:
			if not allow_unknowns:
				raise exc.MissingDataError(key, typ, self)
			return Unknown(self.ns.ref(key))
		
		base_type = self.ns.resolve(base)
		value = self.data[base]
		for idx, attr in enumerate(rel_path):
			semantics = self.ns.registry.get_semantics(base_type)
			# Type is already resolved, so this will never be none
			base_type = semantics.attr_type(attr)
			try:
				value = semantics.get_attr(value, attr)
			except (KeyError, AttributeError) as err:
				if not allow_unknowns:
					sub_path = [base] + list(rel_path)[:idx + 1]
					sub_key = self.ns.path_parser.join(sub_path)
					raise exc.MissingDataError(key, base_type, self) from err
				return Unknown(self.ns.ref(key))

		return value

	def _build_symbol_dag(self, symbol: symbols.Symbol, allow_unknowns: bool = False) -> nx.DiGraph:

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
			else:
				raise TypeError(f"Invalid symbol {sym}")

		# Check that this is in fact a DAG and has no cycles
		if not is_directed_acyclic_graph(graph):
			raise ValueError(f'{graph} is not a DAG.')

		return graph

	def _process_symbol_dag(self, dag: nx.DiGraph, allow_unknowns: bool = False) -> None:

		for symbol_id in topological_sort(dag):

			sym = dag.nodes[symbol_id]['symbol']

			if isinstance(sym, symbols.Literal):
				value = sym.value

			elif isinstance(sym, symbols.Reference):
				value = self.get_encoded_data(sym.path, allow_unknowns)

			elif isinstance(sym, symbols.Function):
				args = []
				is_unknown = False
				for sub_sym in sym.args:
					arg = dag.nodes[sub_sym.symbol_id]['result']
					if isinstance(arg, Unknown):
						is_unknown = True
						break
					args.append(arg)
				kwargs = {}
				for key, sub_sym in sym.kwargs.items():
					arg = dag.nodes[sub_sym.symbol_id]['result']
					if isinstance(arg, Unknown):
						is_unknown = True
						break
					kwargs[key] = arg
				if is_unknown:
					value = Unknown(sym)
				else:
					value = sym.func(*args, **kwargs)
			else:
				raise TypeError(f'Invalid symbol {sym}.')

			def resolve_value(x):
				if isinstance(x, symbols.Symbol):
					return grapn.nodes[x.symbol_id]['result']
				return x

			semantics = self.ns.registry.get_semantics(sym.type)
			resolved = semantics.map(resolve_value, value)
			dag.nodes[symbol_id]['result'] = resolved

	def resolve(self, symbol: symbols.Symbol, allow_unknowns: bool = False) -> Any:
		dag = self._build_symbol_dag(symbol)
		self._process_symbol_dag(dag)
		encoded = dag.nodes[symbol.symbol_id]['result']
		encoder = self.registry.get_encoder(symbol.type)
		return encoder.decode(encoded)


def create_session() -> session.Session:
	"""
	Default factory for creating the best session given the runtime
	"""
	return PythonSession(PythonNamespace())
