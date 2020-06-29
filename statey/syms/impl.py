import dataclasses as dc
from itertools import zip_longest
from typing import Iterable, Any, Optional

import networkx as nx

from statey import exc
from statey.syms import base, func, utils, types
from statey.syms.object_ import Object
from statey.syms.semantics import Semantics


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

    @property
    def id(self) -> Any:
        """
        Get some unique identifier for this object implementation
        """
        return id(self)

    def type(self) -> types.Type:
        """
        Some object implementations are bound to a specific type, so this method
        allows the ability to infer the type from the implementatino. Returning
        NotImplemented incidates the type is not necessarily known
        """
        return NotImplemented

    def registry(self) -> "Registry":
        """
        Some object implementations are bound to a specific registry, so this method
        allows the ability to infer the registry from the implementation. Default
        behavior is returning NotImplemented
        """
        return NotImplemented


class FunctionalAttributeAccessMixin:
    """
    Get attributes by simplying applying semantic get_attr function within
    """
    def get_attr(self, obj: Object, attr: str) -> Any:
        from statey.syms import api

        semantics = obj.__registry.get_semantics(obj.__type)
        attr_semantics = semantics.attr_semantics(attr)
        getter_func = lambda x: semantics.get_attr(x, attr)
        func_type = utils.single_arg_function_type(semantics.type, attr_semantics.type, "x")
        function_obj = api.function(getter_func, func_type)
        return self.map(obj, function_obj)


class FunctionalMappingMixin:
    """
    Defines default map() behavior that works for almost all cases
    """
    def map(self, obj: Object, function: func.Function) -> Object:
        new_impl = FunctionCall(function, (obj,), {})
        return Object(new_impl, function.type.return_type, obj.__registry)


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
        semantics = obj.__registry.get_semantics(obj.__type)
        attr_semantics = semantics.attr_semantics(attr)
        path = self.ns.path_parser.join([self.path, attr])
        if semantics is None:
            raise exc.SymbolKeyError(path, ns)

        new_ref = Reference(path, self.ns)
        return Object(new_ref, semantics=attr_semantics)

    def depends_on(self, obj: Object, session: "Session") -> Iterable[Object]:
        semantics = obj.__registry.get_semantics(obj.__type)
        data = session.get_encoded_data(self.path)
        expanded = semantics.expand(data)

        syms = []

        def collect_objects(x):
            if isinstance(x, Object):
                syms.append(x)

        semantics.map(collect_objects, expanded)
        return syms

    def apply(self, semantics: Semantics, dag: nx.DiGraph, session: "Session") -> Any:
        return session.get_encoded_data(self.path)

    @property
    def id(self) -> Any:
        return f'{type(self).__name__}:{self.path}'

    def type(self) -> types.Type:
        return self.ns.resolve(self.path)

    def registry(self) -> "Registry":
        return self.ns.registry


class StandaloneObjectImplementation(ObjectImplementation):
    """
    Object implementation for impementations that never have dependencies
    """
    def depends_on(self, semantics: Semantics, session: "Session") -> Iterable[Object]:
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
class Value(FunctionalBehaviorMixin, StandaloneObjectImplementation):
    """
    Object implementation for concrete data
    """
    value: Any
    value_type: Optional[types.Type] = None

    def depends_on(self, obj: Object, session: "Session") -> Iterable[Object]:
        semantics = obj.__registry.get_semantics(obj.__type)
        data = self.value
        expanded = semantics.expand(data)

        syms = []

        def collect_objects(x):
            if isinstance(x, Object):
                syms.append(x)

        semantics.map(collect_objects, expanded)
        return syms

    def apply_alone(self, obj: Object) -> Any:
        return self.value

    def type(self) -> types.Type:
        if self.value_type is None:
            return super().type()
        return self.value_type


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
        arg_fields = self.func.type.args
        arg_names = [field.name for field in arg_fields]

        arg_dict = dict(zip(arg_names, args))
        arg_dict.update(kwargs or {})

        self.__dict__['arguments'] = arg_dict

    def depends_on(self, obj: Object, session: "Session") -> Iterable[Object]:
        yield from self.arguments.values()

    def apply(self, obj: Object, dag: nx.DiGraph, session: "Session") -> Any:
        unknowns = []

        kwargs = {}
        for key, sym in self.arguments.items():
            arg = dag.nodes[sym.symbol_id]["result"]
            if isinstance(arg, Unknown):
                unknowns.append(arg)
            kwargs[key] = arg

        if unknowns:
            raise exc.UnknownError(unknowns)

        return self.func.apply(kwargs)

    def type(self) -> types.Type:
        return self.func.return_type


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
        res = self.apply_alone(obj)
        if res is NotImplemented:
            raise exc.UnknownError
        return res

    def apply_alone(self, obj: Object) -> Any:
        try:
            return self.get_result()
        except exc.FutureResultNotSet as err:
            return NotImplemented

    def type(self) -> types.Type:
        if self.return_type is None:
            return super().type()
        return self.return_type


@dc.dataclass(frozen=True)
class Unknown(ObjectImplementation, utils.Cloneable):
    """
    Some value that is not known
    """
    obj: Object
    refs: Sequence[Object] = ()

    def depends_on(self, obj: Object, session: "Session") -> Iterable[Object]:
        return self.refs

    def apply(self, obj: Object, dag: nx.DiGraph, session: "Session") -> Any:
        raise exc.UnknownError(self.refs)

    def get_attr(self, obj: Object, attr: str) -> Any:
        object_attr = self.obj[attr]
        new_impl = self.clone(obj=object_attr)
        return Object(new_impl, object_attr.__type, object_attr.__registry)

    def map(self, obj: Object, function: func.Function) -> Object:
        mapped_object = self.obj.__impl.map(self.obj, function)
        new_impl = self.clone(obj=mapped_object)
        return Object(new_impl, mapped_object.__type, mapped_object.__registry)

    def type(self) -> types.Type:
        return self.obj.type()

    def registry(self) -> "Registry":
        return self.obj.__registry


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
            yield from field.value.__impl.depends_on(obj[field.name], session)

    def apply(self, obj: Object, dag: nx.DiGraph, session: "Session") -> Any:
        out = {}
        for field in self.fields:
            out[field.name] = field.value.__impl.apply(obj[field.name], dag, session)
        return out

    def type(self) -> types.Type:
        fields = []
        for field in self.fields:
            typ = field.value.type()
            if typ is NotImplemented:
                return NotImplemented
            fields.append(types.Field(field.name typ))
        return types.StructType(fields, False)


@dc.dataclass(frozen=True)
class ExpectedValue(ObjectImplementation):
    """
    Defines some parent object and an expectation of what the value will be after resolved
    """
    obj: Object
    expected: Any

    def get_attr(self, obj: Object, attr: str) -> Any:
        semantics = obj.__registry.get_semantics(obj.__type)
        obj_attr = self.obj[attr]
        expected_attr = semantics.get_attr(self.expected, attr)
        return ExpectedValue(obj_attr, expected_attr)

    def depends_on(self, obj: Object, session: "Session") -> Iterable[Object]:
        yield from self.obj.__impl.depends_on(self.obj, session)

    def apply(self, obj: Object, dag: nx.DiGraph, session: "Session") -> Any:
        try:
            return self.obj.__impl.apply(self.obj, dag, session)
        except exc.UnknownError as err:
            refs = list(self.depends_on(obj, session))
            raise exc.UnknownError(refs, expected=self.expected) from err

    def map(self, obj: Object, function: func.Function) -> Object:
        mapped_obj = self.obj.__impl.map(self.obj, function)
        mapped_expected = function.apply(self.expected)
        new_impl = ExpectedValue(mapped_obj, mapped_expected)
        return Object(new_impl, function.type.return_type, obj.__registry)

    def type(self) -> types.Type:
        return self.obj.type()

    def registry(self) -> "Registry":
        return self.obj.__registry
