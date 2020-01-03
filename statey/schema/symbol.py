"""
Symbols provide a way to create dependencies between resources and evaluate those
depedencies as concrete values are resolved.
"""
import abc
import itertools
from functools import partial
from typing import Callable, Any, Tuple, Dict, Optional, Union, Type

import networkx as nx

from statey import exc
from .field import Field
from .helpers import detect_circular_references


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

    @abc.abstractmethod
    def compute_graph(
        self,
        values: "ResourceGraph",
        instance: Optional[nx.MultiDiGraph] = None,
        cache: Optional["CacheManager"] = None,
    ) -> nx.MultiDiGraph:
        """
        Return a compute graph describing this symbol and all of its dependencies
        """
        raise NotImplementedError

    def resolve_partial(
        self,
        values: "ResourceGraph",
        graph: Optional[nx.MultiDiGraph] = None,
        cache: Optional["CacheManager"] = None,
    ) -> Any:
        """
		Get a concrete value or another Symbol for this field. Optional.
		By default, the behavior is the same as resolve()
		"""
        return self.resolve(values, graph, cache)

    @abc.abstractmethod
    def resolve(
        self,
        values: "ResourceGraph",
        graph: Optional[nx.MultiDiGraph] = None,
        cache: Optional["CacheManager"] = None,
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
class Cache(abc.ABC):
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

    def _handler_cls(self, symbol: Symbol) -> Type[Cache]:
        registered = filter(
            partial(isinstance, symbol), sorted(type(self).handlers, key=lambda x: x.__name__)
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

    def _query_value(self, values: "ResourceGraph") -> Optional["SchemaSnapshot"]:
        resource = values.query(self.resource)
        if resource is None:
            return self
        value = resource[self.field_name]
        return value

    def refs(self) -> Tuple[Tuple[str, str], ...]:
        return ((self.resource, self.field_name),)

    def compute_graph(
        self,
        values: "ResourceGraph",
        instance: Optional[nx.MultiDiGraph] = None,
        cache: Optional["CacheManager"] = None,
    ) -> nx.MultiDiGraph:
        cache = CacheManager() if cache is None else cache
        my_cache = cache.get(self)
        if my_cache.compute_graph is not my_cache.NOT_SET:
            return my_cache.compute_graph

        graph = nx.MultiDiGraph() if instance is None else instance

        if id(self) not in graph:
            graph.add_node(id(self), obj=self)

        resolved = self._query_value(values)
        if isinstance(resolved, Symbol) and resolved is not self:
            graph = resolved.compute_graph(values, graph, cache)

            if id(resolved) not in graph[id(self)]:
                graph.add_edge(id(self), id(resolved))

        my_cache.compute_graph = graph
        return graph

    def resolve_partial(
        self,
        values: "ResourceGraph",
        graph: Optional[nx.MultiDiGraph] = None,
        cache: Optional["CacheManager"] = None,
    ) -> Any:
        cache = CacheManager() if cache is None else cache
        if graph is None:
            graph = self.compute_graph(values, cache=cache)
            detect_circular_references(graph)

        if len(graph[id(self)]) == 0:
            return self._query_value(values)

        items = list(graph[id(self)].items())
        if len(items) > 1 or len(items[0][1]) > 1:
            raise exc.GraphIntegrityError("References should have at most one downstream edge.")

        obj_id = items[0][0]
        obj_node = graph.nodes[obj_id]
        obj = obj_node["obj"]

        return obj.resolve_partial(values, graph, cache)

    def resolve(
        self,
        values: "ResourceGraph",
        graph: Optional[nx.MultiDiGraph] = None,
        cache: Optional["CacheManager"] = None,
    ) -> Any:
        value = self.resolve_partial(values, graph, cache)
        if isinstance(value, Symbol):
            raise exc.ResolutionError(
                f"Field {repr(self.field_name)} in resource {self.resource}"
                f" resolves to a symbol ({value}), this is not allowed."
            )
        return value

    def type(self) -> Field:
        return self.field

    def __str__(self) -> str:
        name_or_str = lambda x: getattr(x, "__name__", str(x))
        return (
            f"{type(self).__name__}[{name_or_str(self.field.annotation)}]"
            f"(resource={self.resource}, field_name={self.field_name})"
        )


@CacheManager.handles(Reference)
class ReferenceCache(Cache):
    """
    Cache class for literals
    """

    NOT_SET = object()

    def __init__(self, symbol: Symbol):
        super().__init__(symbol)
        self.compute_graph = self.NOT_SET


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

    @staticmethod
    def _detect_nested_symbols_value(value: Any) -> Any:
        if isinstance(value, Symbol):
            return value
        if isinstance(value, (list, tuple, set)):
            symbols = sum(1 for arg in value if isinstance(arg, Symbol))
            if symbols > 0:
                # We can use any type of field here--arguments to Func objects are not type-checked.
                # We just want the argument to be a Symbol
                return Func[str](lambda *args: type(value)(args))(*value)
        if isinstance(value, dict):
            symbols = sum(1 for arg in value.values() if isinstance(arg, Symbol))
            if symbols > 0:
                # Same as above, type does not matter here
                return Func[str](lambda **kwargs: kwargs)(**value)
        return value

    def _detect_nested_symbols(self):
        self.args = tuple(map(self._detect_nested_symbols_value, self.args))
        keys, values = zip(*self.kwargs.items()) if len(self.kwargs) > 0 else ((), ())
        self.kwargs = dict(zip(keys, map(self._detect_nested_symbols_value, values)))

    def _resolve_args(
        self, values: "ResourceGraph", graph: nx.MultiDiGraph, cache: CacheManager
    ) -> Any:
        if id(self) not in graph:
            # We don't need to check for circular refs here--this happens because
            # new Func instance get created during the evaluation of the graph. The new
            # instance is just a partially resolved version of the old one, though, so
            # there are no net new references to check
            graph = self.compute_graph(values, graph, cache)

        if len(graph[id(self)]) == 0:
            return (), {}

        args = {}
        kwargs = {}
        for obj_id, edges in graph[id(self)].items():
            obj_node = graph.nodes[obj_id]
            obj = obj_node["obj"]

            value = obj.resolve_partial(values, graph, cache)
            for edge in edges.values():
                if "arg" in edge:
                    args[edge["arg"]] = value
                elif "kwarg" in edge:
                    kwargs[edge["kwarg"]] = value

        return args, kwargs

    def __str__(self) -> str:
        type_name = type(self).__name__
        args_string = ""
        if len(self.args) > 0:
            args_string = ", ".join(map(str, self.args))

        kwargs_string = ""
        if len(self.kwargs) > 0:
            kwargs_string = ", ".join("=".join(map(str, tup)) for tup in self.kwargs.items())

        arguments = ", ".join(filter(None, [args_string, kwargs_string]))

        name_or_str = lambda x: getattr(x, "__name__", str(x))
        return (
            f"{type_name}[{name_or_str(self._return_type.annotation)}]"
            f"({name_or_str(self.func)})({arguments})"
        )

    ### PUBLIC METHODS

    def contains_symbols(self) -> bool:
        """
        Indicate whether the arguments to this Func contain any symbolsl (including nested)
        """
        for arg in self.args:
            if isinstance(arg, Symbol):
                return True
        for val in self.kwargs.values():
            if isinstance(val, Symbol):
                return True
        return False

    def refs(self) -> Tuple[Tuple[QueryRef, str], ...]:
        out = []

        for ref in itertools.chain.from_iterable(
            map(type(self).refs_for_value, itertools.chain(self.args, self.kwargs.values()),)
        ):
            if ref not in out:
                out.append(ref)

        return tuple(out)

    def compute_graph(
        self,
        values: "ResourceGraph",
        instance: Optional[nx.MultiDiGraph] = None,
        cache: Optional["CacheManager"] = None,
    ) -> nx.MultiDiGraph:

        cache = CacheManager() if cache is None else cache
        my_cache = cache.get(self)
        if my_cache.compute_graph is not my_cache.NOT_SET:
            return my_cache.compute_graph

        graph = nx.MultiDiGraph() if instance is None else instance

        if id(self) not in graph:
            graph.add_node(id(self), obj=self)

        for idx, arg in enumerate(self.args):
            if not isinstance(arg, Symbol):
                continue
            graph = arg.compute_graph(values, graph, cache)
            if id(arg) not in graph[id(self)] or not any(
                edge.get("arg") == idx for edge in graph[id(self)][id(arg)].values()
            ):
                graph.add_edge(id(self), id(arg), arg=idx)

        for key, val in self.kwargs.items():
            if not isinstance(val, Symbol):
                continue
            graph = val.compute_graph(values, graph, cache)
            if id(val) not in graph[id(self)] or not any(
                edge.get("kwarg") == key for edge in graph[id(self)][id(val)].values()
            ):
                graph.add_edge(id(self), id(val), kwarg=key)

        my_cache.compute_graph = graph
        return graph

    def resolve_partial(
        self,
        values: "ResourceGraph",
        graph: Optional[nx.MultiDiGraph] = None,
        cache: Optional[CacheManager] = None,
    ) -> Any:
        cache = CacheManager() if cache is None else cache
        my_cache = cache.get(self)
        if graph is None:
            graph = self.compute_graph(values, cache=cache)
            detect_circular_references(graph)

        if not self.contains_symbols():
            return self.func(*self.args, **self.kwargs)

        if len(my_cache.args) + len(my_cache.kwargs) > 0:
            args, kwargs = my_cache.args, my_cache.kwargs
        else:
            new_args, new_kwargs = self._resolve_args(values, graph, cache)
            args = tuple(new_args.get(idx, arg) for idx, arg in enumerate(self.args))
            kwargs = {k: new_kwargs.get(k, v) for k, v in self.kwargs.items()}
            my_cache.args, my_cache.kwargs = args, kwargs

        if tuple(args) == tuple(self.args) and kwargs == self.kwargs:
            return self
        return self(*args, **kwargs).resolve_partial(values, graph, cache)

    def resolve(
        self,
        values: "ResourceGraph",
        graph: Optional[nx.MultiDiGraph] = None,
        cache: Optional[CacheManager] = None,
    ) -> Any:
        value = self.resolve_partial(values, graph, cache)
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
        return type(self)(
            partial(self.func, *args, **kwargs), self.annotation, *self.args, **self.kwargs,
        )

    def __call__(self, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> "Func":
        """
		The __call__() method of func just returns a new `Func` object
		with the passed arguments appended to the existing ones
		"""
        return type(self)(self.func, self.annotation, *args, **kwargs)


@CacheManager.handles(Func)
class FuncCache(Cache):
    """
    Simple cache object for the Func symbol
    """

    NOT_SET = object()

    def __init__(self, symbol: Symbol) -> None:
        super().__init__(symbol)
        self.args = ()
        self.kwargs = {}
        self.compute_graph = self.NOT_SET


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

    def compute_graph(
        self,
        values: "ResourceGraph",
        instance: Optional[nx.MultiDiGraph] = None,
        cache: Optional["CacheManager"] = None,
    ) -> nx.MultiDiGraph:
        cache = CacheManager() if cache is None else cache
        my_cache = cache.get(self)
        if my_cache.compute_graph is not my_cache.NOT_SET:
            return my_cache.compute_graph

        graph = nx.MultiDiGraph() if instance is None else instance

        if id(self) not in graph:
            graph.add_node(id(self), obj=self)

        if isinstance(self.value, Symbol):
            graph = self.value.compute_graph(values, graph, cache)
            if id(self.value) not in graph[id(self)]:
                graph.add_edge(id(self), id(self.value))

        my_cache.compute_graph = graph
        return graph

    def type(self) -> Field:
        return self._type

    def resolve_partial(
        self,
        values: "ResourceGraph",
        graph: Optional[nx.MultiDiGraph] = None,
        cache: Optional[CacheManager] = None,
    ) -> Any:
        cache = CacheManager() if cache is None else cache
        if graph is None:
            graph = self.compute_graph(values, cache=cache)
            detect_circular_references(graph)

        if len(graph[id(self)]) == 0:
            return self.value

        items = list(graph[id(self)].items())
        if len(items) > 1 or len(items[0][1]) > 1:
            raise exc.GraphIntegrityError("Literals should have at most one downstream edge.")

        obj_id = items[0][0]
        obj_node = graph.nodes[obj_id]
        obj = obj_node["obj"]

        return obj.resolve_partial(values, graph, cache)

    def resolve(
        self,
        values: "ResourceGraph",
        graph: Optional[nx.MultiDiGraph] = None,
        cache: Optional[CacheManager] = None,
    ) -> Any:
        value = self.resolve_partial(values, graph, cache)
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

    NOT_SET = object()

    def __init__(self, symbol: Symbol):
        super().__init__(symbol)
        self.compute_graph = self.NOT_SET
