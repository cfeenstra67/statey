import abc
import itertools
from functools import partial
from typing import Callable, Any, Tuple, Dict, Optional, Union

from .field import Field
from statey import exc


QueryRef = Union['Resource', str]


class Symbol(abc.ABC):
	"""
	A symbol is some logical value within statey, but we may have to
	perform some computation in the future to actually figure out what
	the value is. Examples could be references or function outputs
	"""
	@abc.abstractmethod
	def refs(self) -> Tuple[Tuple[QueryRef, str], ...]:
		"""
		Obtain a list of fields related to this symbol
		returns a tuple of (path, field) tuples
		"""
		raise NotImplementedError

	def resolve_partial(self, values: 'ResourceGraph') -> Any:
		"""
		Get a concrete value or another Symbol for this field. Optional.
		By default, the behavior is the same as resolve()
		"""
		return self.resolve(values)

	@abc.abstractmethod
	def resolve(self, values: 'ResourceGraph') -> Any:
		"""
		Get the concrete value for this field given all concrete field values
		"""
		raise NotImplementedError

	@abc.abstractmethod
	def type(self) -> Field:
		"""
		Return a Field instance with the type and properties of the expected
		concrete value from this function
		"""
		raise NotImplementedError


class Reference(Symbol):
	"""
	A reference is a specific attribute of some resource within
	a state.
	"""
	def __init__(self, resource: QueryRef, field_name: str, field: Field) -> None:
		"""
		`source_field` - a Field instance indicating the referenced field
		"""
		self.resource = resource
		self.field_name = field_name
		self.field = field

	def refs(self) -> Tuple[Tuple[str, str], ...]:
		return (self.resource, self.field_name),

	def resolve_partial(self, values: 'ResourceGraph') -> Any:
		resource = values.query(self.resource)
		value = resource[self.field_name]
		return value

	def resolve(self, values: 'ResourceGraph') -> Any:
		value = self.resolve_partial(values)
		if isinstance(value, Symbol):
			raise exc.ResolutionError(
				f'Field {repr(self.field_name)} in resource {repr(self.resource)}'
				f' resolves to a symbol, this is not allowed.'
			)
		return value

	def type(self) -> Field:
		return self.field


class FuncMeta(abc.ABCMeta):
	"""
	Metaclass for the Func class. enables __getitem__ support
	"""
	def __getitem__(cls, annotation: Any) -> Callable[[Any], 'Func']:
		"""
		Enable expressions like Func[int](fibonacci).

		Also for easy type conversion, enable expressions like Func[[int]]
		to make a Func instance with the same type annotation and function
		"""
		if isinstance(annotation, list) and len(annotation) == 1:
			(func,), = annotation
			return Func(func, func)

		def factory(func, *args, **kwargs):
			return Func(func, annotation, *args, **kwargs)
		return factory


class Func(Symbol, metaclass=FuncMeta):
	"""
	A Func object wraps a function and operates the same as the original
	function, except any of its arguments can be `Symbol` objects. The functions
	will be executed as part of the computational graph.

	WARNING: Func objects should wrap pure functions ideally, as no guarantees
	are made about how many times it will be called
	"""
	def __init__(
			self,
			func: Callable[[Any], Any],
			annotation: Optional[Any],
			*args: Tuple[Any, ...],
			**kwargs: Dict[str, Any]
	) -> None:
		"""
		`func` - function object to wrap
		`args` - optional, specify arguments as in partial()
		`kwargs` - optional, specify keyword arguments as in partial()
		"""
		self.func = func
		self.annotation = annotation
		self.args = args
		self.kwargs = kwargs
		self._return_type = None

		if self.annotation is None:
			self._infer_annotation()
		self._infer_return_type()

	def _infer_annotation(self) -> None:
		annotations = getattr(self.func, '__annotations__', None)
		if annotations is None:
			raise exc.MissingReturnType(
				f'No return annotation provided, and none was found in'
				f' the provided function {repr(self.func)}.'
			)

		try:
			self.annotation = annotations['return']
		except KeyError as error:
			raise exc.MissingReturnType(
				f'No return annotation provided, and none was found in'
				f' the provided function {repr(self.func)}.'
			) from error

	def _infer_return_type(self) -> None:
		try:
			field = Field[self.annotation]
		except KeyError as error:
			raise exc.MissingReturnType(
				f'Unable to infer a field type from the given return annotation'
				f' "{self.annotation}" in the provided function {repr(self.func)}.'
			) from error

		self._return_type = field

	def refs(self) -> Tuple[Tuple[QueryRef, str], ...]:
		out = []

		for value in filter(
			lambda x: isinstance(x, Symbol),
			itertools.chain(self.args, self.kwargs.values())
		):
			for ref in value.refs():
				if ref not in out:
					out.append(ref)

		return tuple(out)

	def _resolve_args_partial(self, values: 'ResourceGraph') -> Tuple[Tuple[Any, ...], Dict[str, Any]]:
		args = []
		for arg in self.args:
			value = arg.resolve_partial(values) if isinstance(arg, Symbol) else arg
			args.append(value)

		kwargs = {}
		for key, val in self.kwargs.items():
			value = val.resolve_partial(values) if isinstance(val, Symbol) else val
			kwargs[key] = value

		return tuple(args), kwargs

	def resolve_partial(self, values: 'ResourceGraph') -> Any:
		args, kwargs = self._resolve_args_partial(values)
		contains_symbols = False

		for arg in args:
			if isinstance(arg, Symbol):
				contains_symbols = True
				break

		if not contains_symbols:
			for val in kwargs.values():
				if isinstance(val, Symbol):
					contains_symbols = True
					break

		if contains_symbols:
			return self(*args, **kwargs)
		return self.func(*args, **kwargs)

	def resolve(self, values: 'ResourceGraph') -> Any:
		value = self.resolve_partial(values)
		if isinstance(value, Symbol):
			raise exc.ResolutionError(f'Function {repr(self)} resolves to a symbol, this is not allowed.')
		return value

	def type(self) -> Field:
		return self._return_type

	def partial(self, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> 'Func':
		"""
		Similar to functools.partial. Set some default arguments. Chainable.
		"""
		return type(self)(partial(self.func, *args, **kwargs), self.annotation, *self.args, **self.kwargs)

	def __repr__(self) -> str:
		return (
			f'''{repr(self.func)}({", ".join(map(repr, self.args))}, {", ".join(
				"=".join(map(repr, tup)) for tup in self.kwargs.items())
			})'''
		)

	def __call__(self, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> 'Func':
		"""
		The __call__() method of func just returns a new `Func` object
		with the passed arguments appended to the existing ones
		"""
		return type(self)(self.func, self.annotation, *args, **kwargs)


class Literal(Symbol):
	"""
	A literal value, optionally with some references associated with it.
	"""
	def __init__(self, value: Any, type: Field, refs: Tuple[Reference] = ()) -> None:
		self.refs = refs
		self.value = value
		self._type = type

	def refs(self) -> Tuple[Tuple[QueryRef, str], ...]:
		return tuple((ref.resource, ref.field_name) for ref in self.refs)

	def type(self) -> Field:
		return self._type

	def resolve(self) -> Any:
		return self.value
