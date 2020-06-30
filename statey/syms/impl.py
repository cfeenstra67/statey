import abc
import dataclasses as dc
from functools import wraps
from itertools import zip_longest
from typing import Iterable, Any, Optional, Sequence, Dict

import networkx as nx

from statey import exc
from statey.syms import base, func, utils, types
from statey.syms.object_ import Object


class ObjectImplementation(base.AttributeAccess):
    """
    Some implementation for an object
    """
    @abc.abstractmethod
    def depends_on(self, obj: Object, session: "Session") -> Iterable[Object]:
        """
        Retrieve any objects that this one depends on
        """
        raise NotImplementedError

    @abc.abstractmethod
    def apply(self, obj: Object, dag: nx.DiGraph, session: "Session") -> Any:
        """
        Given the dag containing results of any upstreams as well
        as the current session, fully resolve the value of this object
        """
        raise NotImplementedError

    def apply_alone(self, obj: Object) -> Any:
        """
        Object implementations may not depend on other objects and thus can
        be trivially resolved this way. Default behavior is to return NotImplemented
        """
        return NotImplemented

    @abc.abstractmethod
    def map(self, obj: Object, function: func.Function) -> Object:
        """
        Apply the given function and return a new object.
        """
        raise NotImplementedError

    def call(self, obj: Object, args: Sequence[Any], kwargs: Dict[str, Any]) -> Object:
        """
        Some object implementations can apply arguments
        """
        raise NotImplementedError

    @property
    def id(self) -> Any:
        """
        Get some unique identifier for this object implementation
        """
        return id(self)

    def object_repr(self, obj: "Object") -> str:
        """
        Render a string representation of an object with this implementation
        """
        return f'{type(obj).__name__}[{obj._type}]({repr(self)})'

    def type(self) -> types.Type:
        """
        Some object implementations are bound to a specific type, so this method
        allows the ability to infer the type from the implementatino. Returning
        NotImplemented incidates the type is not necessarily known
        """
        raise NotImplementedError

    def registry(self) -> "Registry":
        """
        Some object implementations are bound to a specific registry, so this method
        allows the ability to infer the registry from the implementation. Default
        behavior is returning NotImplemented
        """
        raise NotImplementedError


class FunctionalAttributeAccessMixin:
    """
    Get attributes by simplying applying semantic get_attr function within
    """
    def get_attr(self, obj: Object, attr: str) -> Any:
        from statey.syms import api

        semantics = obj._registry.get_semantics(obj._type)
        attr_semantics = semantics.attr_semantics(attr)

        if attr_semantics is None:
            raise KeyError(attr)

        getter_func = lambda x: semantics.get_attr(x, attr)
        func_type = utils.single_arg_function_type(semantics.type, attr_semantics.type, "x")
        function_obj = api.function(getter_func, func_type)
        return self.map(obj, function_obj)


class FunctionalMappingMixin:
    """
    Defines default map() behavior that works for almost all cases
    """
    def map(self, obj: Object, function: func.Function) -> Object:
        new_impl = FunctionCall(function, (obj,))
        return Object(new_impl, function.type.return_type, obj._registry)


class FunctionalBehaviorMixin(FunctionalMappingMixin, FunctionalAttributeAccessMixin):
    """
    Simple way to mix in all default functional behavior for object impls
    """


@dc.dataclass(frozen=True)
class Reference(FunctionalMappingMixin, ObjectImplementation):
    """
    References some key in a session, possibly with some relative path
    """
    path: str
    ns: "Namespace" = dc.field(repr=False, hash=False, compare=False)

    def get_attr(self, obj: Object, attr: str) -> Any:
        semantics = obj._registry.get_semantics(obj._type)
        path = self.ns.path_parser.join([self.path, attr])
        if semantics is None:
            raise exc.SymbolKeyError(path, ns)

        new_ref = Reference(path, self.ns)
        return Object(new_ref)

    def depends_on(self, obj: Object, session: "Session") -> Iterable[Object]:
        semantics = obj._registry.get_semantics(obj._type)
        data = session.get_encoded_data(self.path)
        expanded = semantics.expand(data)

        syms = []

        def collect_objects(x):
            if isinstance(x, Object):
                syms.append(x)

        semantics.map(collect_objects, expanded)
        return syms

    def apply(self, obj: Object, dag: nx.DiGraph, session: "Session") -> Any:
        return session.get_encoded_data(self.path)

    @property
    def id(self) -> Any:
        return f'{type(self).__name__}:{self.path}'

    def type(self) -> types.Type:
        return self.ns.resolve(self.path)

    def registry(self) -> "Registry":
        return self.ns.registry

    def object_repr(self, obj: "Object") -> str:
        return f'{type(self).__name__}[{obj._type}]({self.path})'


class StandaloneObjectImplementation(ObjectImplementation):
    """
    Object implementation for impementations that never have dependencies
    """
    def depends_on(self, obj: Object, session: "Session") -> Iterable[Object]:
        return ()

    def apply(self, obj: Object, dag: nx.DiGraph, session: "Session") -> Any:
        return self.apply_alone(obj)

    @abc.abstractmethod
    def apply_alone(self, obj: Object) -> Any:
        """
        This must be implemented in StandaloneObjectImplementation subclasses
        """
        raise NotImplementedError


@dc.dataclass(frozen=True)
class Data(FunctionalMappingMixin, StandaloneObjectImplementation):
    """
    Object implementation for concrete data
    """
    value: Any
    value_type: Optional[types.Type] = None

    def depends_on(self, obj: Object, session: "Session") -> Iterable[Object]:
        semantics = obj._registry.get_semantics(obj._type)
        data = self.value
        expanded = semantics.expand(data)

        syms = []

        def collect_objects(x):
            if isinstance(x, Object):
                syms.append(x)

        semantics.map(collect_objects, expanded)
        return syms

    def apply_alone(self, obj: Object) -> Any:
        encoder = obj._registry.get_encoder(obj._type)
        return encoder.encode(self.value)

    def type(self) -> types.Type:
        if self.value_type is None:
            raise NotImplementedError
        return self.value_type

    def object_repr(self, obj: "Object") -> str:
        return f'{type(self).__name__}[{obj._type}]({repr(self.value)})'

    def get_attr(self, obj: Object, attr: str) -> Any:
        semantics = obj._registry.get_semantics(obj._type)
        if self.value is None:
            new_data = None
        else:
            new_data = semantics.get_attr(self.value, attr)
        attr_semantics = semantics.attr_semantics(attr)
        return Object(Data(new_data), attr_semantics.type, obj._registry)


@dc.dataclass(frozen=True)
class FunctionCall(FunctionalBehaviorMixin, ObjectImplementation):
    """
    Object implementation for a function and associated arguments
    """
    func: func.Function
    args: dc.InitVar[Sequence[Object]] = ()
    kwargs: dc.InitVar[Optional[Dict[str, Object]]] = None
    arguments: Dict[str, Object] = dc.field(init=False, default=None)

    def __post_init__(self, args: Sequence[Object], kwargs: Optional[Dict[str, Object]]) -> None:
        if kwargs is None:
            kwargs = {}
        self.__dict__['arguments'] = utils.bind_function_args(self.func.type, args, kwargs)

    def depends_on(self, obj: Object, session: "Session") -> Iterable[Object]:
        yield from self.arguments.values()

    def apply(self, obj: Object, dag: nx.DiGraph, session: "Session") -> Any:
        unknowns = []

        kwargs = {}
        for key, sym in self.arguments.items():
            arg = dag.nodes[sym._impl.id]["result"]
            arg_encoder = session.ns.registry.get_encoder(sym._type)
            arg = arg_encoder.decode(arg)
            if isinstance(arg, Object):
                unknowns.append(arg)
            kwargs[key] = arg

        if unknowns:
            raise exc.UnknownError(unknowns)

        result = self.func.apply(kwargs)
        encoder = session.ns.registry.get_encoder(self.func.type.return_type)
        return encoder.encode(result)

    def type(self) -> types.Type:
        return self.func.type.return_type

    def object_repr(self, obj: "Object") -> str:
        kwarg_reprs = ', '.join('='.join([key, repr(val)]) for key, val in self.arguments.items())
        return f'{type(self.func).__name__}Call[{obj._type}]({self.func.name}({kwarg_reprs}))'


@dc.dataclass(frozen=True)
class FutureResult(utils.Cloneable):
    """
    Helper so that we can clone futures while still retaining their behavior.
    """

    result: Any = dc.field(default=utils.MISSING)

    def get(self) -> Any:
        """
        Get the result of the future, raising exc.FutureResultNotSet
        if it hasn't been set yet
        """
        if self.result is utils.MISSING:
            raise exc.FutureResultNotSet(self)
        return self.result

    def set(self, result: Any) -> None:
        """
        Set the result, raising exc.FutureResultAlreadySet if it has
        already been set
        """
        if self.result is not utils.MISSING:
            raise exc.FutureResultAlreadySet(self)
        self.__dict__["result"] = result


@dc.dataclass(frozen=True)
class Future(FunctionalBehaviorMixin, ObjectImplementation):
    """
    A future is a value that may or may not yet be set
    """
    refs: Sequence[Object] = ()
    result: FutureResult = dc.field(default_factory=FutureResult)
    return_type: Optional[types.Type] = None

    def get_result(self) -> Any:
        """
        Get the result of the future, raising exc.FutureResultNotSet
        if it hasn't been set yet
        """
        return self.result.get()

    def set_result(self, result: Any) -> None:
        """
        Set the result, raising exc.FutureResultAlreadySet if it has
        already been set
        """
        self.result.set(result)

    def depends_on(self, obj: Object, session: "Session") -> Iterable[Object]:
        return self.refs

    def apply(self, obj: Object, dag: nx.DiGraph, session: "Session") -> Any:
        try:
            res = self.apply_alone(obj)
        except NotImplementedError as err:
            raise exc.UnknownError from err
        return res

    def apply_alone(self, obj: Object) -> Any:
        try:
            return self.get_result()
        except exc.FutureResultNotSet as err:
            raise NotImplementedError from err

    def type(self) -> types.Type:
        if self.return_type is None:
            raise NotImplementedError
        return self.return_type

    def object_repr(self, obj: "Object") -> str:
        return f'{type(self).__name__}[{obj._type}]({self.result.result})'


@dc.dataclass(frozen=True)
class Unknown(ObjectImplementation):
    """
    Some value that is not known
    """
    obj: Object
    refs: Sequence[Object] = ()

    def __post_init__(self) -> None:
        while isinstance(self.obj._impl, Unknown):
            self.__dict__['obj'] = self.obj._impl.obj

    def depends_on(self, obj: Object, session: "Session") -> Iterable[Object]:
        return self.refs

    def apply(self, obj: Object, dag: nx.DiGraph, session: "Session") -> Any:
        raise exc.UnknownError(self.refs)

    def get_attr(self, obj: Object, attr: str) -> Any:

        def handle(result):

            if isinstance(result, Object):
                new_impl = Unknown(result, self.refs)
                return Object(new_impl, result._type, result._registry)

            if callable(result):

                @wraps(result)
                def wrapper(*args, **kwargs):
                    return handle(result(*args, **kwargs))

                return wrapper
        
            raise TypeError(f'Unhandled attribute result type {result}! Failing')

        return handle(self.obj[attr])

    def map(self, obj: Object, function: func.Function) -> Object:
        mapped_object = self.src._inst.map(function)
        new_impl = Unknown(mapped_object, self.refs)
        return Object(new_impl, mapped_object._type, mapped_object._registry)

    def type(self) -> types.Type:
        return self.obj._type

    def registry(self) -> "Registry":
        return self.obj._registry

    def object_repr(self, obj: "Object") -> str:
        return f'{type(self).__name__}[{self.obj._type}]({self.obj})'

@dc.dataclass(frozen=True)
class StructField:
    """
    Single field in a StructSymbol
    """
    name: str
    value: Object


@dc.dataclass(frozen=True)
class Struct(FunctionalMappingMixin, ObjectImplementation):
    """
    Combines multiple objects into a struct
    """
    fields: Sequence[StructField]

    def get_attr(self, obj: Object, attr: str) -> Any:
        field_map = {field.name: field for field in self.fields}
        return field_map[attr].value

    def depends_on(self, obj: Object, session: "Session") -> Iterable[Object]:
        for field in self.fields:
            yield from field.value._impl.depends_on(obj[field.name], session)

    def apply(self, obj: Object, dag: nx.DiGraph, session: "Session") -> Any:
        out = {}
        for field in self.fields:
            out[field.name] = field.value._impl.apply(obj[field.name], dag, session)
        return out

    def type(self) -> types.Type:
        fields = []
        for field in self.fields:
            typ = field.value._type
            fields.append(types.Field(field.name, typ))
        return types.StructType(fields, False)

    def object_repr(self, obj: "Object") -> str:
        field_reprs = ', '.join('='.join([field.name, repr(field.value)]) for field in self.fields)
        return f'{type(self).__name__}[{obj._type}]({field_reprs})'


@dc.dataclass(frozen=True)
class ExpectedValue(ObjectImplementation):
    """
    Defines some parent object and an expectation of what the value will be after resolved
    """
    obj: Object
    expected: Any

    def get_attr(self, obj: Object, attr: str) -> Any:
        semantics = obj._registry.get_semantics(obj._type)
        obj_attr = self.obj[attr]
        expected_attr = semantics.get_attr(self.expected, attr)
        new_impl = ExpectedValue(obj_attr, expected_attr)
        return Object(new_impl)

    def get_attr(self, obj: Object, attr: str) -> Any:

        def handle(result):

            if isinstance(result, Object):
                semantics = self.obj._registry.get_semantics(self.obj._type)
                expected_attr = semantics.get_attr(self.expected, attr)
                new_impl = ExpectedValue(result, expected_attr)
                return Object(new_impl, result._type, result._registry)

            if callable(result):

                @wraps(result)
                def wrapper(*args, **kwargs):
                    return handle(result(*args, **kwargs))

                return wrapper
        
            raise TypeError(f'Unhandled attribute result type {result}! Failing')

        return handle(self.obj[attr])

    def depends_on(self, obj: Object, session: "Session") -> Iterable[Object]:
        yield from self.obj._impl.depends_on(self.obj, session)

    def apply(self, obj: Object, dag: nx.DiGraph, session: "Session") -> Any:
        try:
            return self.obj._impl.apply(self.obj, dag, session)
        except exc.UnknownError as err:
            refs = list(self.depends_on(obj, session))
            raise exc.UnknownError(refs, expected=self.expected) from err

    def map(self, obj: Object, function: func.Function) -> Object:
        mapped_obj = self.obj._impl.map(self.obj, function)
        mapped_expected = function.apply(self.expected)
        new_impl = ExpectedValue(mapped_obj, mapped_expected)
        return Object(new_impl, function.type.return_type, obj._registry)

    def type(self) -> types.Type:
        return self.obj._type

    def registry(self) -> "Registry":
        return self.obj._registry

    def object_repr(self, obj: "Object") -> str:
        return f'{type(self).__name__}[{obj._type}]({self.expected}, from={self.obj})'
