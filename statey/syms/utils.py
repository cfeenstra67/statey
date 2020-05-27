import dataclasses as dc
import inspect
from contextlib import contextmanager
from functools import partial, reduce
from itertools import product
from typing import Type, Optional, Union, Any, Dict, Hashable, Sequence, Callable

import marshmallow as ma
import networkx as nx

from statey import NS
from statey.syms import exc


LRU_MAX_SIZE = 100


@dc.dataclass(frozen=True)
class Global:
	"""
	An arbitrary named value intended to be used at the global level
	"""
	name: str

	def __repr__(self) -> str:
		return self.name


MISSING = Global('MISSING')


EXPLODE = Global('EXPLODE')


def extract_optional_annotation(annotation: Any) -> Any:
	"""
	Determine if the given annotation is an Optional field
	"""
	if (
		hasattr(annotation, "__origin__")
		and annotation.__origin__ is Union
		and getattr(annotation, "__args__", None) is not None
		and len(annotation.__args__) == 2
		and annotation.__args__[-1] is type(None)
	):
		return extract_inner_annotation(annotation)
	return None


def extract_inner_annotation(annotation: Any) -> Any:
	if not getattr(annotation, '__args__', None):
		return None
	return annotation.__args__[0]


def dict_get_path(data: Dict[Hashable, Any], path: Sequence[Hashable], explode: bool = True) -> Any:
	"""
	Get the key at the given path from the dictionary
	"""
	current = data
	current_path = list(path)
	while current_path:
		comp = current_path.pop(0)
		if comp is EXPLODE and explode:
			return [dict_get_path(item, current_path) for item in current]
		elif comp is EXPLODE:
			continue
		current = current[comp]

	return current


def map_dims(func: Callable[[Any], Any], arr: Sequence[Any], ndims: int = 1) -> Sequence[Any]:
	"""
	Apply func to the elements of a multi-dimensional array with `ndims` dimensions
	"""
	if ndims == 0:
		return func(arr)
	return [map_dims(func, item, ndims - 1) for item in arr]


def dict_set_path(data: Dict[Hashable, Any], path: Sequence[Hashable], value: Any) -> Any:
	"""
	Set the given value at the given path in the dict
	"""
	current = data
	current_path = path[:-1]
	while current_path:
		comp = current_path.pop(0)
		if comp is EXPLODE:
			for item in current:
				dict_set_path(item, current_path, value)
			return
		current = current.setdefault(comp, {})

	current[path[-1]] = value


@contextmanager
def reraise_ma_validation_error():
	try:
		yield
	except ma.ValidationError as err:
		raise exc.InputValidationError(err.messages) from err


class PossiblySymbolicField(ma.fields.Field):
	"""
	A field that will validate the type if given a symbol or the value if given anything else
	"""
	def __init__(self, field: ma.fields.Field, type: 'Type', *args, **kwargs) -> None:
		super().__init__(*args, **kwargs)
		self.field = field
		self.type = type

	def _serialize(self, value: Any, attr: str, obj: str, **kwargs) -> Any:
		from statey.syms import symbols
		# Just pass this through when serializing
		if isinstance(value, symbols.Symbol):
			return value
		return self.field._serialize(value, attr, obj, **kwargs)

	def _deserialize(self, value: Any, attr: Optional[str] = None, data: Optional[Any] = None, **kwargs) -> Any:
		from statey.syms import symbols, types
		if not isinstance(value, symbols.Symbol):
			return self.field._deserialize(value, attr, data, **kwargs)

		if self.type != value.type:
			raise ma.ValidationError(f'Invalid symbol type (expected {self.type}, got {value.type}).')
		return value

	def __repr__(self) -> str:
		return f'{type(self).__name__}[{self.type}]({type(self.field).__name__}(...))'


class SingleValueFunction(ma.fields.Function):
	"""
	Use encoding and decoding functions to process a specific field
	"""
	_CHECK_ATTRIBUTE = True

	def _serialize(self, value, attr, obj, **kwargs):
		return self._call_or_raise(self.serialize_func, value, attr)


def filter_dict(keep: Callable[[Any], bool], data: Dict[Hashable, Any], and_sequences: bool = True) -> Dict[Hashable, Any]:
	"""
	Similar to the filter() function, but applied recursively to a dict
	"""
	out = {}
	for key, val in data.items():
		if isinstance(val, dict):
			filtered = filter_dict(keep, val, and_sequences)
			if filtered:
				out[key] = filtered
		if and_sequences and isinstance(val, Sequence) and and_sequences:
			items = out[key] = []
			for item in val:
				if isinstance(item, dict):
					item = filter_dict(keep, item, and_sequences)
					if item:
						items.append(item)
				elif keep(item):
					items.append(item)
			if items:
				out[key] = items
		elif keep(val):
			out[key] = val
	return out


def invert_filter(func: Callable[[Any], bool]) -> Callable[[Any], bool]:
	"""
	Return a new function that returns `not func(x)`
	"""
	def wrapper(x):
		return not func(x)
	return wrapper


def infer_annotation(obj: Any) -> Any:
	"""
	Attempt to infer an annotation from obj, falling back on `type(obj)`
	"""
	obj_type = type(obj)
	if isinstance(obj, Sequence) and obj and reduce(lambda x, y: x is y, map(type, obj)):
		return Sequence[infer_annotation(obj[0])]
	return obj_type


def wrap_function_call(registry: 'TypeRegistry', func: Callable[[Any], Any], *args, **kwargs) -> Any:
	"""
	Given a callable, wrap its parameters and return annotation in types.Type objects from
	the given registry

	NOTE: func must have a signature i.e. must be a valid input to inspect.signature()
	"""
	from statey.syms import symbols

	sig = inspect.signature(func)
	bound = sig.bind(*args, **kwargs)

	# Add defaults
	for param in sig.parameters.values():
		if param.name not in bound.arguments and param.default is not inspect._missing:
			bound.arguments[param.name] = param.default

	items = list(bound.arguments.items())

	args = []
	consumed = 0
	for key, arg in items:
		param = sig.parameters[key]
		# Break at the first keyword-only argument, until then all args can be passed
		# positionally
		if param.kind > 2:
			break
		if isinstance(arg, symbols.Symbol):
			args.append(arg)
		else:
			# VAR_POSITIONAL
			if param.kind == 1:
				typ = registry.get_type(param.annotation)
				args.extend(symbols.Literal(a, typ, registry) for a in arg)
			else:
				args.append(symbols.Literal(arg, registry.get_type(param.annotation), registry))
		consumed += 1

	kwargs = {}
	# The rest of the params can be treated as kwargs
	for key, arg in items[consumed:]:
		param = sig.parameters[key]
		if isinstance(arg, symbols.Symbol):
			kwargs[key] = arg
		else:
			kwargs[key] = symbols.Literal(arg, registry.get_type(param.annotation))

	return args, kwargs, registry.get_type(sig.return_annotation)


def encodeable_dataclass_fields(data: Any) -> Sequence[dc.field]:
	""".
	Respect the statey-specific field metadtaa and don't encode fields where statey.encode = False
	"""
	out = []
	for field in dc.fields(data):
		syms_meta = field.metadata.get(NS) or {}
		encode = syms_meta.get('encode', True)
		if encode:
			out.append(field)
	return out


class Cloneable:
	"""
	Mixin for dataclasses adding a clone() method
	"""
	def clone(self, **kwargs) -> 'Cloneable':
		"""
		Return a copy of the current object with the given attributes replaced. This current
		object should be an instance of a dataclass
		"""
		return dc.replace(self, **kwargs)


def subgraph_retaining_dependencies(dag: nx.DiGraph, keep_nodes: Sequence[str]) -> None:
	"""
	Remove nodes while retaining any indirect dependencies between them. Modifies the graph
	in place.
	"""
	drop_nodes = set(dag.nodes) - set(keep_nodes)

	for node in nx.topological_sort(dag.subgraph(drop_nodes)):
		for predecessor, successor in product(dag.pred[node], dag.succ[node]):
			dag.add_edge(predecessor, successor)
		dag.remove_node(node)
