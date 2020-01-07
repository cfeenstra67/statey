"""
Symbols provide a way to create dependencies between resources and evaluate those
depedencies as concrete values are resolved.
"""
import abc
import itertools
import operator
from functools import partial, wraps
from typing import Callable, Any, Tuple, Dict, Optional, Union, Type

import networkx as nx

from statey import exc
from statey.utils.helpers import truncate_string
from .field import Field, NestedField  # pylint: disable=cyclic-import
from .helpers import detect_circular_symbol_references


QueryRef = Union["Resource", str]


def binary_operator_handler(
    func: Callable[[Any, Any], Any],
    name: str,
    return_annotation: Optional[Any] = None,
    check_annotation_method: bool = True,
) -> Callable[[Any, Any], Any]:
    """
    Wrap a binary operator in a proper method
    """

    @wraps(func)
    def wrapper(self, other):
        # If none specified, assume both args have to be the same type and
        # the return type is the same
        annotation = return_annotation
        if annotation is None:
            my_type = self.type()
            if isinstance(other, Symbol):
                other_type = other.type()
                if not isinstance(other_type, type(my_type)):
                    raise exc.SymbolTypeError(
                        f"Attempting to perform operation on object with a "
                        f"different field type ({type(other_type).__name__},"
                        f" expected {type(my_type).__name__})."
                    )

            annotation = my_type.annotation

        if check_annotation_method:
            if not hasattr(annotation, name):
                raise exc.SymbolTypeError(
                    f"Unsupported type for method {name}: {annotation.__name__}."
                )

        return Func[annotation](func)(self, other)

    wrapper.__name__ = name
    return wrapper


def unary_operator_handler(
    func: Callable[[Any], Any],
    name: str,
    return_annotation: Optional[Any] = None,
    check_annotation_method: bool = True,
) -> Callable[[Any], Any]:
    """
    Wrap a unary operator in a proper method
    """

    @wraps(func)
    def wrapper(self):
        annotation = return_annotation
        if annotation is None:
            annotation = self.type().annotation

        if check_annotation_method:
            if not hasattr(annotation, name):
                raise exc.SymbolTypeError(
                    f"Unsupported type for method {name}: {annotation.__name__}."
                )

        return Func[annotation](func)(self)

    wrapper.__name__ = name
    return wrapper


class Symbol(abc.ABC):
    """
	A symbol is some logical value within statey, but we may have to
	perform some computation in the future to actually figure out what
	the value is. Examples could be references or function outputs
	"""

    __add__ = binary_operator_handler(operator.add, "__add__")
    __sub__ = binary_operator_handler(operator.sub, "__sub__")
    __mul__ = binary_operator_handler(operator.mul, "__mul__")
    __truediv__ = binary_operator_handler(operator.truediv, "__truediv__")
    __floordiv__ = binary_operator_handler(operator.floordiv, "__floordiv__")
    __mod__ = binary_operator_handler(operator.mod, "__mod__")
    __pow__ = binary_operator_handler(operator.pow, "__pow__")
    __lt__ = binary_operator_handler(operator.lt, "__lt__", bool)
    __gt__ = binary_operator_handler(operator.gt, "__gt__", bool)
    __le__ = binary_operator_handler(operator.le, "__le__", bool)
    __eq__ = binary_operator_handler(operator.eq, "__eq__", bool)
    __ne__ = binary_operator_handler(operator.ne, "__ne__", bool)
    __lshift__ = binary_operator_handler(operator.lshift, "__lshift__")
    __rshift__ = binary_operator_handler(operator.rshift, "__rshift__")
    __neg__ = unary_operator_handler(operator.neg, "__neg__")
    __pos__ = unary_operator_handler(operator.pos, "__pos__")
    __invert__ = unary_operator_handler(operator.invert, "__invert__")

    @abc.abstractmethod
    def refs(self) -> Tuple[Tuple[QueryRef, Tuple[str, ...]], ...]:
        """
		Obtain a list of fields related to this symbol
		returns a tuple of (path, field) tuples
		"""
        raise NotImplementedError

    @abc.abstractmethod
    def compute_graph(
        self, values: "ResourceGraph", cache: Optional["CacheManager"] = None,
    ) -> nx.MultiDiGraph:
        """
        Return a compute graph describing this symbol and all of its dependencies
        """
        raise NotImplementedError

    def resolve_partial(
        self,
        values: "ResourceGraph",
        cache: Optional["CacheManager"] = None,
        check_circular: bool = True,
    ) -> Any:
        """
		Get a concrete value or another Symbol for this field. Optional.
		By default, the behavior is the same as resolve()
		"""
        return self.resolve(values, cache, check_circular)

    @abc.abstractmethod
    def resolve(
        self,
        values: "ResourceGraph",
        cache: Optional["CacheManager"] = None,
        check_circular: bool = False,
    ) -> Any:
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


# pylint: disable=too-few-public-methods
class Cache:
    """
    Base class for Cache implementations. These are really symbol-specific,
    so no logic here right now
    """

    @classmethod
    def get_id(cls, instance: Any) -> str:
        """
        Get a unique identifier for this instance
        """
        return str(id(instance))

    def __init__(self, symbol: Symbol) -> None:
        self.symbol = symbol
        self.computed_graph = False


class CacheManager:
    """
    Manage caches for symbols in a graph
    """

    handlers = {}

    @classmethod
    def handles(cls, symbol_type: Type[Symbol]) -> Callable[[Type[Cache]], Type[Cache]]:
        """
        Register a Cache class for a symbol type
        """

        def dec(cache_type):
            cls.handlers[symbol_type] = cache_type
            return cache_type

        return dec

    def __init__(self) -> None:
        self.caches = {}
        self.graph = nx.MultiDiGraph()

    def _handler_cls(self, symbol: Symbol) -> Type[Cache]:
        registered = filter(
            partial(isinstance, symbol),
            sorted(type(self).handlers, key=lambda x: x.__name__),
        )
        result = next(registered, None)
        return result if result is None else type(self).handlers[result]

    def get(self, symbol: Symbol) -> Cache:
        """
        Get an appropriate cache instacne for this symbol
        """
        symbol_type = type(symbol)
        handler_cls = self._handler_cls(symbol)
        if handler_cls is None:
            return None
        symbol_id = handler_cls.get_id(symbol)
        if (symbol_type, symbol_id) not in self.caches:
            self.caches[symbol_type, symbol_id] = handler_cls(symbol)
        return self.caches[symbol_type, symbol_id]


class Reference(Symbol):
    """
	A reference is a specific attribute of some resource within
	a state.
	"""

    def __init__(
        self,
        resource: QueryRef,
        field_name: str,
        field: Field,
        nested_path: Tuple[str, ...] = (),
    ) -> None:
        """
		`source_field` - a Field instance indicating the referenced field
		"""
        self.resource = resource
        self.field_name = field_name
        self.nested_path = nested_path
        self.field = field

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, type(self)):
            return False
        return other.resource == self.resource and other.field_name == self.field_name

    def _query_value(self, values: "ResourceGraph") -> Any:
        resource = values.query(self.resource)
        if resource is None:
            return self

        obj = resource
        for component in self.nested_path:
            obj = obj[component]

        return obj[self.field_name]

    def refs(self) -> Tuple[Tuple[QueryRef, Tuple[str, ...]], ...]:
        path = self.nested_path + (self.field_name,)
        return ((self.resource, path),)

    def compute_graph(
        self, values: "ResourceGraph", cache: Optional["CacheManager"] = None,
    ) -> nx.MultiDiGraph:
        cache = CacheManager() if cache is None else cache
        my_cache = cache.get(self)
        if my_cache.computed_graph:
            return cache.graph

        graph = cache.graph

        if id(self) not in graph:
            graph.add_node(id(self), obj=self)

        resolved = self._query_value(values)
        if isinstance(resolved, Symbol) and resolved is not self:
            if id(resolved) not in graph:
                graph = resolved.compute_graph(values, cache)

            if id(resolved) not in graph[id(self)]:
                graph.add_edge(id(self), id(resolved))

        my_cache.computed_graph = True
        return graph

    def resolve_partial(
        self,
        values: "ResourceGraph",
        cache: Optional["CacheManager"] = None,
        check_circular: bool = True,
    ) -> Any:
        cache = CacheManager() if cache is None else cache
        graph = self.compute_graph(values, cache)

        if len(graph[id(self)]) == 0:
            return self._query_value(values)

        items = list(graph[id(self)].items())
        if len(items) > 1 or len(items[0][1]) > 1:
            raise exc.GraphIntegrityError(
                "References should have at most one downstream edge."
            )

        obj_id = items[0][0]
        obj_node = graph.nodes[obj_id]
        obj = obj_node["obj"]

        if check_circular:
            detect_circular_symbol_references(graph)
        resolved = obj.resolve_partial(values, cache, check_circular=False)
        return resolved

    def resolve(
        self,
        values: "ResourceGraph",
        cache: Optional["CacheManager"] = None,
        check_circular: bool = True,
    ) -> Any:
        value = self.resolve_partial(values, cache, check_circular)
        if isinstance(value, Symbol):
            raise exc.ResolutionError(
                f"Field {repr(self.field_name)} in resource {self.resource}"
                f" resolves to a symbol ({value}), this is not allowed."
            )
        return value

    def type(self) -> Field:
        return self.field

    def __repr__(self) -> str:
        name_or_str = lambda x: getattr(x, "__name__", str(x))
        return (
            f"{type(self).__name__}[{name_or_str(self.field.annotation)}]"
            f"(resource={self.resource}, field_name={self.field_name}, "
            f"nested_path={self.nested_path})"
        )


@CacheManager.handles(Reference)
class ReferenceCache(Cache):
    """
    Cache class for references
    """


class RefAccessor:
    """
    Helper to allow smart attribute access for references
    """

    def __init__(
        self,
        resource: "Resource",
        schema: Optional[Type["Schema"]] = None,
        nested_path: Tuple[str, ...] = (),
    ) -> None:
        if schema is None:
            schema = resource.Schema
        self.resource = resource
        self.schema = schema
        self.nested_path = nested_path

    def __getattr__(self, name: str) -> Any:
        """
        Get a reference to a field in the resource's schema
        """
        field = getattr(self.schema, name)
        return field.reference_factory(self.resource, name, self.nested_path)

    def __getitem__(self, key: str) -> Any:
        """
        Enable using the attribute accessor with __getitem__ syntax
        """
        try:
            return getattr(self, key)
        except AttributeError as exc:
            raise KeyError(key) from exc


class SchemaReference(Reference):
    """
    A reference to an entire schema
    """

    def __init__(
        self,
        resource: "Resource",
        field: NestedField,
        nested_path: Tuple[str, ...] = (),
    ) -> None:
        super().__init__(resource, None, field, nested_path)
        self.attrs = RefAccessor(resource, field.annotation, nested_path)

    def _query_value(self, values: "ResourceGraph") -> Any:
        resource = values.query(self.resource)
        if resource is None:
            return self

        obj = resource
        for component in self.nested_path:
            obj = obj[component]

        return obj


CacheManager.handles(SchemaReference)(ReferenceCache)


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
        func = None
        if isinstance(annotation, list) and len(annotation) == 1:
            (func,) = annotation
            annotation = func

        def factory(func, *args, **kwargs):
            func_obj = Func(func, *args, **kwargs)
            func_obj.annotation = annotation
            return func_obj

        if func is not None:
            return factory(func)

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
        if isinstance(value, (list, tuple, set)):
            refs = map(cls.refs_for_value, value)
            refs = itertools.chain.from_iterable(refs)
            return tuple(refs)
        if isinstance(value, dict):
            refs = map(cls.refs_for_value, value.values())
            refs = itertools.chain.from_iterable(refs)
            return tuple(refs)
        return ()

    def __init__(
        self,
        func: Callable[[Any], Any],
        *args: Tuple[Any, ...],
        **kwargs: Dict[str, Any],
    ) -> None:
        """
		`func` - function object to wrap
		`args` - optional, specify arguments as in partial()
		`kwargs` - optional, specify keyword arguments as in partial()
		"""
        self.func = func
        self.args = args
        self.kwargs = kwargs
        # This can be set manually or specified with Func[annotation](func)
        self._annotation = None
        self._return_type = None

        self._infer_annotation()
        self._detect_nested_symbols()

    ### PRIVATE METHODS

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
            return
        self.annotation = annotations.get("return")

    def _infer_return_type(self) -> None:
        self._return_type = None
        if self.annotation is None:
            return

        try:
            field = Field[self.annotation]()
        except KeyError as error:
            raise exc.MissingReturnType(
                f"Unable to infer a field type from the given return annotation"
                f' "{self.annotation}" in the provided function {repr(self.func)}.'
            ) from error

        self._return_type = field

    @staticmethod
    def _detect_nested_symbols_value(value: Any) -> Any:
        if isinstance(value, Symbol):
            return value
        if isinstance(value, (list, tuple, set)):
            symbols = sum(1 for arg in value if isinstance(arg, Symbol))
            if symbols > 0:
                # We can use any type of field here--arguments to Func objects are not
                # type-checked. We just want the argument to be a Symbol
                return Func(lambda *args: type(value)(args))(*value)
        if isinstance(value, dict):
            symbols = sum(1 for arg in value.values() if isinstance(arg, Symbol))
            if symbols > 0:
                # Same as above, type does not matter here
                return Func(lambda **kwargs: kwargs)(**value)
        return value

    def _detect_nested_symbols(self):
        self.args = tuple(map(self._detect_nested_symbols_value, self.args))
        keys, values = zip(*self.kwargs.items()) if len(self.kwargs) > 0 else ((), ())
        self.kwargs = dict(zip(keys, map(self._detect_nested_symbols_value, values)))

    def _resolve_args(self, values: "ResourceGraph", cache: CacheManager) -> Any:
        graph = cache.graph

        args = {}
        kwargs = {}
        for obj_id, edges in graph[id(self)].items():
            obj_node = graph.nodes[obj_id]
            obj = obj_node["obj"]

            value = obj.resolve_partial(values, cache, check_circular=False)
            for edge in edges.values():
                if "arg" in edge:
                    args[edge["arg"]] = value
                elif "kwarg" in edge:
                    kwargs[edge["kwarg"]] = value

        return args, kwargs

    def __repr__(self) -> str:
        type_name = type(self).__name__
        args_string = ""
        if len(self.args) > 0:
            args_string = ", ".join(map(truncate_string, map(str, self.args)))

        kwargs_string = ""
        if len(self.kwargs) > 0:
            kwargs_string = ", ".join(
                "=".join((key, truncate_string(str(val))))
                for key, val in self.kwargs.items()
            )

        arguments = ", ".join(filter(None, [args_string, kwargs_string]))

        name_or_str = lambda x: getattr(x, "__name__", str(x))
        annotation_str = ""
        if self.annotation is not None:
            annotation_str = f"[{name_or_str(self.annotation)}]"
        return f"{type_name}{annotation_str}({name_or_str(self.func)})({arguments})"

    ### PUBLIC METHODS

    @property
    def annotation(self) -> Any:
        """
        annotation property
        """
        return self._annotation

    @annotation.setter
    def annotation(self, value: Any) -> None:
        """
        annotation property setter
        """
        self._annotation = value
        self._infer_return_type()

    def contains_symbols(self) -> bool:
        """
        Indicate whether the arguments to this Func contain any symbolsl (including
        nested)
        """
        for arg in self.args:
            if isinstance(arg, Symbol):
                return True
        for val in self.kwargs.values():
            if isinstance(val, Symbol):
                return True
        return False

    def refs(self) -> Tuple[Tuple[QueryRef, Tuple[str, ...]], ...]:
        out = []

        for ref in itertools.chain.from_iterable(
            map(
                type(self).refs_for_value,
                itertools.chain(self.args, self.kwargs.values()),
            )
        ):
            if ref not in out:
                out.append(ref)

        return tuple(out)

    def compute_graph(
        self, values: "ResourceGraph", cache: Optional["CacheManager"] = None,
    ) -> nx.MultiDiGraph:

        cache = CacheManager() if cache is None else cache
        my_cache = cache.get(self)
        if my_cache.computed_graph:
            return cache.graph

        graph = cache.graph

        if id(self) not in graph:
            graph.add_node(id(self), obj=self)

        for idx, arg in enumerate(self.args):
            if not isinstance(arg, Symbol):
                continue
            if id(arg) not in graph:
                graph = arg.compute_graph(values, cache)

            if id(arg) not in graph[id(self)] or not any(
                edge.get("arg") == idx for edge in graph[id(self)][id(arg)].values()
            ):
                graph.add_edge(id(self), id(arg), arg=idx)

        for key, val in self.kwargs.items():
            if not isinstance(val, Symbol):
                continue
            if id(val) not in graph:
                graph = val.compute_graph(values, cache)

            if id(val) not in graph[id(self)] or not any(
                edge.get("kwarg") == key for edge in graph[id(self)][id(val)].values()
            ):
                graph.add_edge(id(self), id(val), kwarg=key)

        my_cache.computed_graph = True
        return graph

    def resolve_partial(
        self,
        values: "ResourceGraph",
        cache: Optional[CacheManager] = None,
        check_circular: bool = True,
    ) -> Any:
        cache = CacheManager() if cache is None else cache
        my_cache = cache.get(self)
        graph = self.compute_graph(values, cache)

        if not self.contains_symbols():
            return self.func(*self.args, **self.kwargs)

        if len(my_cache.args) + len(my_cache.kwargs) > 0:
            args, kwargs = my_cache.args, my_cache.kwargs
        else:
            new_args, new_kwargs = self._resolve_args(values, cache)
            args = tuple(new_args.get(idx, arg) for idx, arg in enumerate(self.args))
            kwargs = {k: new_kwargs.get(k, v) for k, v in self.kwargs.items()}
            my_cache.args, my_cache.kwargs = args, kwargs

        if check_circular:
            detect_circular_symbol_references(graph)

        if tuple(args) == tuple(self.args) and kwargs == self.kwargs:
            resolved = self
        else:
            resolved = self(*args, **kwargs).resolve_partial(
                values, cache, check_circular=False
            )

        return resolved

    def resolve(
        self,
        values: "ResourceGraph",
        cache: Optional[CacheManager] = None,
        check_circular: bool = True,
    ) -> Any:
        value = self.resolve_partial(values, cache, check_circular)
        if isinstance(value, Symbol):
            raise exc.ResolutionError(
                f"Function {self} resolves to a symbol ({value}), this is not allowed."
            )
        return value

    def type(self) -> Field:
        return self._return_type

    def partial(self, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> "Func":
        """
		Similar to functools.partial. Set some default arguments. Chainable.
		"""
        instance = type(self)(
            partial(self.func, *args, **kwargs), *self.args, **self.kwargs,
        )
        instance.annotation = self.annotation
        return instance

    def __call__(self, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> "Func":
        """
		The __call__() method of func just returns a new `Func` object
		with the passed arguments appended to the existing ones
		"""
        instance = type(self)(self.func, *args, **kwargs)
        instance.annotation = self.annotation
        return instance


@CacheManager.handles(Func)
class FuncCache(Cache):
    """
    Simple cache object for the Func symbol
    """

    def __init__(self, symbol: Symbol) -> None:
        super().__init__(symbol)
        self.args = ()
        self.kwargs = {}


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
        refs = map(lambda x: x.refs(), self._refs)
        refs = itertools.chain.from_iterable(refs)
        return tuple(set(refs))

    def compute_graph(
        self, values: "ResourceGraph", cache: Optional["CacheManager"] = None,
    ) -> nx.MultiDiGraph:
        cache = CacheManager() if cache is None else cache
        my_cache = cache.get(self)
        if my_cache.computed_graph:
            return cache.graph

        graph = cache.graph

        if id(self) not in graph:
            graph.add_node(id(self), obj=self)

        if isinstance(self.value, Symbol):
            if id(self.value) not in graph:
                graph = self.value.compute_graph(values, cache)

            if id(self.value) not in graph[id(self)]:
                graph.add_edge(id(self), id(self.value))

        my_cache.computed_graph = True
        return graph

    def type(self) -> Field:
        return self._type

    def resolve_partial(
        self,
        values: "ResourceGraph",
        cache: Optional[CacheManager] = None,
        check_circular: bool = True,
    ) -> Any:
        cache = CacheManager() if cache is None else cache
        graph = self.compute_graph(values, cache)

        if len(graph[id(self)]) == 0:
            if check_circular:
                detect_circular_symbol_references(graph)
            return self.value

        items = list(graph[id(self)].items())
        if len(items) > 1 or len(items[0][1]) > 1:
            raise exc.GraphIntegrityError(
                "Literals should have at most one downstream edge."
            )

        obj_id = items[0][0]
        obj_node = graph.nodes[obj_id]
        obj = obj_node["obj"]

        if check_circular:
            detect_circular_symbol_references(graph)
        resolved = obj.resolve_partial(values, cache, check_circular=False)
        return resolved

    def resolve(
        self,
        values: "ResourceGraph",
        cache: Optional[CacheManager] = None,
        check_circular: bool = True,
    ) -> Any:
        value = self.resolve_partial(values, cache, check_circular)
        if isinstance(value, Symbol):
            raise exc.ResolutionError(
                f"Literal {self} resolves to a symbol ({value}), this is not allowed."
            )
        return value


@CacheManager.handles(Literal)
class LiteralCache(Cache):
    """
    Cache class for literals
    """
