"""
Symbols provide a way to create dependencies between resources and evaluate those
depedencies as concrete values are resolved.
"""
import abc
import itertools
from functools import partial
from typing import Callable, Any, Tuple, Dict, Optional, Union

from statey import exc
from .field import Field


QueryRef = Union["Resource", str]


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

    def resolve_partial(self, values: "ResourceGraph") -> Any:
        """
		Get a concrete value or another Symbol for this field. Optional.
		By default, the behavior is the same as resolve()
		"""
        return self.resolve(values)

    @abc.abstractmethod
    def resolve(self, values: "ResourceGraph") -> Any:
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

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, type(self)):
            return False
        return other.resource == self.resource and other.field_name == self.field_name

    def refs(self) -> Tuple[Tuple[str, str], ...]:
        return ((self.resource, self.field_name),)

    def resolve_partial(self, values: "ResourceGraph") -> Any:
        resource = values.query(self.resource)
        if resource is None:
            return self
        value = resource[self.field_name]
        return value

    def resolve(self, values: "ResourceGraph") -> Any:
        value = self.resolve_partial(values)
        if isinstance(value, Symbol):
            raise exc.ResolutionError(
                f"Field {repr(self.field_name)} in resource {repr(self.resource)}"
                f" resolves to a symbol, this is not allowed."
            )
        return value

    def type(self) -> Field:
        return self.field


class FuncMeta(abc.ABCMeta):
    """
	Metaclass for the Func class. enables __getitem__ support
	"""

    def __getitem__(cls, annotation: Any) -> Callable[[Any], "Func"]:
        """
		Enable expressions like Func[int](fibonacci).

		Also for easy type conversion, enable expressions like Func[[int]]
		to make a Func instance with the same type annotation and function
		"""
        if isinstance(annotation, list) and len(annotation) == 1:
            ((func,),) = annotation
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

    @classmethod
    def refs_for_value(cls, value: Any) -> Tuple[Tuple[QueryRef, str], ...]:
        """
		Given any value, get associated references. Handles basic python collections
		such as lists and dicts properly
		"""
        if isinstance(value, Symbol):
            return value.refs()
        if isinstance(value, (list, tuple)):
            refs = map(cls.refs_for_value, value)
            refs = itertools.chain.from_iterable(refs)
            return tuple(refs)
        if isinstance(value, dict):
            refs = map(cls.refs_for_value, value.values())
            refs = itertools.chain.from_iterable(refs)
            return tuple(refs)
        return ()

    @classmethod
    def resolve_value_partial(cls, value: Any, graph: "ResourceGraph") -> Any:
        """
		Resolve a single value. Handles python collections properly (tuple, list, dict)
		"""
        _resolve_value = partial(cls.resolve_value_partial, graph=graph)

        if isinstance(value, Symbol):
            return value.resolve_partial(graph)
        if isinstance(value, (list, tuple)):
            args = tuple(map(_resolve_value, value))
            symbols = sum(1 for arg in args if isinstance(arg, Symbol))
            if symbols > 0:
                # We can use any type of field here--arguments to Func objects are not type-checked.
                # We just want the argument to be a Symbol
                return cls[str](lambda *args: type(value)(args))(*args)
            return type(value)(args)
        if isinstance(value, dict):
            kwargs = {k: _resolve_value(v) for k, v in value.items()}
            symbols = sum(1 for value in kwargs.values() if isinstance(value, Symbol))
            if symbols > 0:
                # Same as above, type does not matter here
                return cls[str](lambda **kwargs: kwargs)(**kwargs)
            return kwargs
        return value

    def __init__(
        self,
        func: Callable[[Any], Any],
        annotation: Optional[Any],
        *args: Tuple[Any, ...],
        **kwargs: Dict[str, Any],
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

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, type(self)):
            return False
        return (
            other.func == self.func
            and tuple(other.args) == tuple(self.args)
            and other.kwargs == self.kwargs
        )

    def _infer_annotation(self) -> None:
        annotations = getattr(self.func, "__annotations__", None)
        if annotations is None:
            raise exc.MissingReturnType(
                f"No return annotation provided, and none was found in"
                f" the provided function {repr(self.func)}."
            )

        try:
            self.annotation = annotations["return"]
        except KeyError as error:
            raise exc.MissingReturnType(
                f"No return annotation provided, and none was found in"
                f" the provided function {repr(self.func)}."
            ) from error

    def _infer_return_type(self) -> None:
        try:
            field = Field[self.annotation]()
        except KeyError as error:
            raise exc.MissingReturnType(
                f"Unable to infer a field type from the given return annotation"
                f' "{self.annotation}" in the provided function {repr(self.func)}.'
            ) from error

        self._return_type = field

    def refs(self) -> Tuple[Tuple[QueryRef, str], ...]:
        out = []

        for ref in itertools.chain.from_iterable(
            map(type(self).refs_for_value, itertools.chain(self.args, self.kwargs.values()),)
        ):
            if ref not in out:
                out.append(ref)

        return tuple(out)

    def _resolve_args_partial(
        self, values: "ResourceGraph", args: Tuple[Any, ...], kwargs: Dict[str, Any]
    ) -> Tuple[Tuple[Any, ...], Dict[str, Any], int]:
        out_args = []
        updated = 0

        for arg in args:
            value = type(self).resolve_value_partial(arg, values)
            if value != arg:
                updated += 1
            out_args.append(value)

        out_kwargs = {}
        for key, val in kwargs.items():
            value = type(self).resolve_value_partial(val, values)
            if value != val:
                updated += 1
            out_kwargs[key] = value

        return tuple(out_args), out_kwargs, updated

    def resolve_partial(self, values: "ResourceGraph") -> Any:
        updated = 1
        args, kwargs = self.args, self.kwargs
        while updated > 0:
            args, kwargs, updated = self._resolve_args_partial(values, args, kwargs)
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

        if tuple(args) == tuple(self.args) and kwargs == self.kwargs:
            return self
        if contains_symbols:
            return self(*args, **kwargs)
        return self.func(*args, **kwargs)

    def resolve(self, values: "ResourceGraph") -> Any:
        value = self.resolve_partial(values)
        if isinstance(value, Symbol):
            raise exc.ResolutionError(f"Function {self} resolves to a symbol, this is not allowed.")
        return value

    def type(self) -> Field:
        return self._return_type

    def partial(self, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> "Func":
        """
		Similar to functools.partial. Set some default arguments. Chainable.
		"""
        return type(self)(
            partial(self.func, *args, **kwargs), self.annotation, *self.args, **self.kwargs,
        )

    def __str__(self) -> str:
        return f"""{type(self).__name__}({self.func}({", ".join(map(str, self.args))}, {", ".join(
				"=".join(map(str, tup)) for tup in self.kwargs.items())
			}))"""

    def __call__(self, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> "Func":
        """
		The __call__() method of func just returns a new `Func` object
		with the passed arguments appended to the existing ones
		"""
        return type(self)(self.func, self.annotation, *args, **kwargs)


class Literal(Symbol):
    """
	A literal value, optionally with some references associated with it.
	"""

    # pylint: disable=redefined-builtin
    def __init__(self, value: Any, type: Field, refs: Tuple[Reference] = ()) -> None:
        self._refs = refs
        self.value = value
        self._type = type

    def refs(self) -> Tuple[Tuple[QueryRef, str], ...]:
        return tuple((ref.resource, ref.field_name) for ref in self._refs)

    def type(self) -> Field:
        return self._type

    def resolve(self, values: "ResourceGraph") -> Any:
        return self.value
