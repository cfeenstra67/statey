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

	def _resolve_one(self, base: types.Type, comps: Sequence[Any], path: str) -> Tuple[types.Type, Sequence[Any]]:
		if not comps:
			return base
		
		head, *tail = comps
		if isinstance(base, types.StructType):
			return base[head].type, tail

		if not isinstance(base, types.ArrayType):
			raise exc.SymbolKeyError(path, self)

		if head is utils.EXPLODE:
			if not tail:
				return base, tail

			second, *tail = tail

			if not isinstance(base.element_type, types.StructType) or second not in base.element_type:
				raise exc.SymbolKeyError(path, self)

			second_type = base.element_type[second].type
			return types.ArrayType(second_type, base.nullable), tail

		if isinstance(head, slice):
			return base, tail

		if isinstance(head, int):
			return base.element_type, tail

		raise exc.SymbolKeyError(path, self)

	def resolve(self, key: str) -> types.Type:
		"""
		Get the type of the given key, raising an error if it is not in the current schema
		"""
		base, *rel_path = self.path_parser.split(key)
		if base not in self.types:
			raise exc.SymbolKeyError(key, self)

		base_type = self.types[base]
		while rel_path:
			base_type, rel_path = self._resolve_one(base_type, rel_path, key)

		return base_type

	# def __getitem__(self, attr: Union[slice, str]) -> Symbol:
	# 	out_path = self.ns.path_parser.join([self.path, attr])

	# 	def get_struct_field(typ):
	# 		fields_by_name = {field.name: field for field in typ.fields}
	# 		if attr not in fields_by_name:
	# 			raise exc.SymbolKeyError(out_path, self.ns)
	# 		field = fields_by_name[attr]
	# 		return field

	# 	if isinstance(self.type, types.StructType):
	# 		typ = get_struct_field(self.type).type
	# 	elif isinstance(self.type, types.ArrayType):
	# 		if isinstance(attr, slice):
	# 			typ = self.type
	# 		elif isinstance(attr, int):
	# 			typ = self.type.element_type
	# 		elif isinstance(self.type.element_type, types.StructType):
	# 			typ = types.ArrayType(get_struct_field(self.type.element_type).type, self.type.nullable)
	# 			out_path = self.ns.path_parser.join([self.path, utils.EXPLODE, attr])
	# 		else:
	# 			raise exc.SymbolKeyError(out_path, self.ns)
	# 	else:
	# 		raise exc.SymbolKeyError(out_path, self.ns)
	# 	return Reference(out_path, typ, self.ns)

		# base_ref = symbols.Reference(base, self.types[base], self)
		# print("HERE", base_ref, rel_path)
		# try:
		# 	return utils.dict_get_path(base_ref, rel_path, explode=False).type
		# except KeyError as err:
		# 	raise exc.SymbolKeyError(key, self) from err


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

	def get_encoded_data(self, key: str) -> Any:
		"""
		Return the data or symbol at the given key, with a boolean is_symbol also returned indicating
		whether that key points to a symbol. If the key has not been set, syms.exc.MissingDataError
		will be raised
		"""
		typ = self.ns.resolve(key)
		path = self.ns.path_parser.split(key)
		try:
			return utils.dict_get_path(self.data, path)
		except KeyError as err:
			raise exc.MissingDataError(key, typ, self) from err

	def _build_value_dag(self, graph: nx.DiGraph, symbol_id: Any, value: Any, type: types.Type) -> Any:
		if isinstance(value, symbols.Symbol):
			yield value, (symbol_id,)
			return

		if value is None:
			return

		if isinstance(type, types.ArrayType):
			for val in value:
				yield from self._build_value_dag(graph, symbol_id, val, type.element_type)

		elif isinstance(type, types.StructType):
			for key, val in value.items():
				yield from self._build_value_dag(graph, symbol_id, val, type[key].type)

	def _build_symbol_dag(self, symbol: symbols.Symbol) -> nx.DiGraph:

		graph = nx.DiGraph()
		syms = [(symbol, ())]

		while syms:
			sym, downstreams = syms.pop(0)
			processed = False
			if sym.symbol_id in graph.nodes:
				processed = True
			else:
				# print("SYMBOL", sym.symbol_id, sym)
				graph.add_node(sym.symbol_id, symbol=sym)

			for symbol_id in downstreams:
				# print("EDGE", sym.symbol_id, symbol_id)
				graph.add_edge(sym.symbol_id, symbol_id)

			if processed:
				continue

			if isinstance(sym, symbols.Literal):
				syms.extend(self._build_value_dag(graph, sym.symbol_id, sym.value, sym.type))
			elif isinstance(sym, symbols.Reference):
				value = self.get_encoded_data(sym.path)
				syms.extend(self._build_value_dag(graph, sym.symbol_id, value, sym.type))
			elif isinstance(sym, symbols.Function):
				for sub_sym in chain(sym.args, sym.kwargs.values()):
					syms.append((sub_sym, (sym.symbol_id,)))
			else:
				raise TypeError(f"Invalid symbol {sym}")

		# Check that this is in fact a DAG and has no cycles
		if not is_directed_acyclic_graph(graph):
			raise ValueError(f'{graph} is not a DAG.')

		# print("NODES", graph.nodes)

		return graph

	def _process_value_dag(self, graph: nx.DiGraph, value: Any, type: types.Type) -> Any:
		# print("VALUE", value, type)
		if value is None:
			return None

		if isinstance(value, symbols.Symbol):
			# print("SYMBOL2", value.symbol_id, value)
			sym_value = graph.nodes[value.symbol_id]['result']
			# print("SYM_VALUE", sym_value)
			return self._process_value_dag(graph, sym_value, value.type)

		if isinstance(type, types.ArrayType):
			return [self._process_value_dag(graph, item, type.element_type) for item in value]

		if isinstance(type, types.StructType):
			return {
				key: self._process_value_dag(graph, val, type[key].type)
				for key, val in value.items()
			}

		return value

	def _process_symbol_dag(self, dag: nx.DiGraph) -> None:
		for symbol_id in topological_sort(dag):
			sym = dag.nodes[symbol_id]['symbol']
			# print("PROCESSING", symbol_id, sym)
			if isinstance(sym, symbols.Literal):
				value = sym.value
			elif isinstance(sym, symbols.Reference):
				value = self.get_encoded_data(sym.path)
			elif isinstance(sym, symbols.Function):
				args = []
				for sub_sym in sym.args:
					args.append(dag.nodes[sub_sym.symbol_id]['result'])
				kwargs = {}
				for key, sub_sym in sym.kwargs.items():
					kwargs[key] = dag.nodes[sub_sym.symbol_id]['result']
				value = sym.func(*args, **kwargs)
			else:
				raise TypeError(f'Invalid symbol {sym}.')

			resolved = self._process_value_dag(dag, value, sym.type)
			dag.nodes[symbol_id]['result'] = resolved

	def resolve(self, symbol: symbols.Symbol) -> Any:
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
