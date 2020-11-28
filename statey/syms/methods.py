import abc
import dataclasses as dc
import operator
from functools import wraps
from typing import Any, Dict, Sequence, Callable, Type as PyType, Optional

import statey as st
from statey.syms import func, impl, types, base, utils, api, stack
from statey.syms.object_ import Object


class Method(abc.ABC):
    """
    A method is essentially a factory for creating "computed" attributes
    """

    @abc.abstractmethod
    def bind(self, obj: Object) -> Any:
        """
        Given an input object, return some other object representing this attribute
        """
        raise NotImplementedError

    @abc.abstractmethod
    def return_type(self, obj: Object) -> Any:
        """
        Get the return type of this method for the given object
        """
        raise NotImplementedError


@dc.dataclass(frozen=True)
class InstanceMethod(Method):
    """
    Instance method implementation
    """

    func: func.Function

    def bind(self, obj: Object) -> Any:
        return utils.bind_function_call(self.func, (obj,), registry=obj._registry)

    def return_type(self, obj: Object) -> Any:
        return self.func.type.return_type


@dc.dataclass(frozen=True)
class CallFunctionMethod(Method):
    """
    Some special logic required for applying functions
    """

    func_type: types.FunctionType

    def bind(self, obj: Object) -> Any:
        """
        Unlike all other methods, the __apply__ method for functions
        """

        def call(*args, **kwargs):
            def apply_method(func: self.func_type) -> obj._type.return_type:
                return utils.wrap_function_call(func, args, kwargs)

            res = obj._inst.map(
                utils.native_function(apply_method, registry=obj._registry)
            )
            return Object(res, frame=stack.frame_snapshot(1))

        return call

    def return_type(self, obj: Object) -> Any:
        return self.func_type.return_type

    @classmethod
    @st.hookimpl
    def get_methods(cls, type: types.Type, registry: "Registry") -> "ObjectMethods":
        if not isinstance(type, types.FunctionType):
            return None
        return ConstantObjectMethods({"__call__": cls(type)})


class ObjectMethods(base.AttributeAccess):
    """
    Accessor interface for object methods
    """

    @abc.abstractmethod
    def get_method(self, name: str) -> Method:
        """
        Get a method by name
        """
        raise NotImplementedError

    def get_attr(self, obj: Object, attr: str) -> Object:
        try:
            method = self.get_method(attr)
        except st.exc.NoSuchMethodError as err:
            raise st.exc.SymbolAttributeError(obj, attr) from err
        return method.bind(obj)


class SimpleObjectMethods(ObjectMethods):
    """
    Simple fetch a dictionary of methods via a method, and use that for the get_method function.
    """

    @abc.abstractmethod
    def methods(self) -> Dict[str, Method]:
        """
        Get all of the methods available for this instance
        """
        raise NotImplementedError

    def get_method(self, name: str) -> Method:
        try:
            return self.methods()[name]
        except KeyError as err:
            raise st.exc.NoSuchMethodError(name) from err


@dc.dataclass(frozen=True)
class ConstantObjectMethods(SimpleObjectMethods):
    """
    ObjectMethods implementation from a dict of methods by name
    """

    method_map: Dict[str, Method]

    def methods(self) -> Dict[str, Method]:
        return self.method_map


NoMethods = ConstantObjectMethods({})


@dc.dataclass(frozen=True)
class CompositeObjectMethods(ObjectMethods):
    """
    Class to combine multiple ObjectMethods instances into one
    """

    object_methods: Sequence[ObjectMethods]

    def get_method(self, name: str) -> Method:
        for methods in self.object_methods:
            try:
                return methods.get_method(name)
            except st.exc.NoSuchMethodError:
                pass
        raise st.exc.NoSuchMethodError(name)


def method_from_function(
    function_obj: func.Function,
    return_type: types.Type = utils.MISSING,
    registry: "Registry" = utils.MISSING,
) -> Method:
    """
    Construct a method given a function. A method a function that is accessed as an
    attribute of an object
    """
    if registry is utils.MISSING:
        registry = st.registry

    instance_arg, *func_args = function_obj.type.args

    if return_type is utils.MISSING:
        return_type = function_obj.type.return_type

    method_type = types.NativeFunctionType(func_args, return_type)

    def bind(self: instance_arg.type) -> method_type:
        def bound(*args, **kwargs):
            arguments = utils.bind_function_args(
                function_obj.type, (self,) + args, kwargs
            )
            return function_obj.apply(arguments)

        return utils.native_function(bound, type=method_type, registry=registry)

    method_function_obj = utils.native_function(bind, registry=registry)
    return InstanceMethod(method_function_obj)


def method(
    func: Callable[[Any], Any] = utils.MISSING,
    return_type: types.Type = utils.MISSING,
    registry: "Registry" = utils.MISSING,
) -> Any:
    """
    method_from_function made into a convenient decorator.
    """

    def dec(_func):
        func_obj = utils.native_function(_func, registry=registry)
        method = method_from_function(
            func_obj, return_type=return_type, registry=registry
        )
        _func._statey_method = method
        return _func

    return dec if func is utils.MISSING else dec(func)


class DeclarativeMethodsMeta(type(ObjectMethods)):
    """
    Collect method objects declared
    """

    def __new__(
        cls, name: str, bases: Sequence[PyType], attrs: Dict[str, Any]
    ) -> PyType:
        super_cls = super().__new__(cls, name, bases, attrs)
        methods = super_cls.__methods__ if hasattr(super_cls, "__methods__") else {}
        methods = methods.copy()

        new_methods = {
            key: val._statey_method
            for key, val in attrs.items()
            if isinstance(getattr(val, "_statey_method", None), Method)
        }
        new_methods.update(
            {key: val for key, val in attrs.items() if isinstance(val, Method)}
        )
        methods.update(new_methods)

        super_cls.__methods__ = methods

        return super_cls


class DeclarativeMethods(ObjectMethods, metaclass=DeclarativeMethodsMeta):
    """
    Simple way to write methods for particular types by simply declaring them
    as instance methods on a DeclarativeMethods subclass
    """

    def get_method(self, name: str) -> Method:
        try:
            return self.__methods__[name]
        except KeyError as err:
            raise st.exc.NoSuchMethodError(name) from err


@dc.dataclass(frozen=True)
class MethodsForType:
    """
    Plugin to map methods to some particular types.Type subclass
    """

    type_cls: PyType[types.Type]
    methods: ObjectMethods

    @st.hookimpl
    def get_methods(self, type: types.Type, registry: "Registry") -> ObjectMethods:
        if not isinstance(type, self.type_cls):
            return None
        return self.methods


@dc.dataclass(frozen=True)
class BinaryMagicMethod(Method):
    """
    Magic methods need to return regular python functions instead of statey objects
    """

    name: str
    instance_type: types.Type
    operation: Callable[[Any, Any], Any]
    return_type_opt: Optional[types.Type] = None

    def bind(self, obj: Object) -> Any:

        return_type = self.return_type(obj)

        def apply_method(other: Any) -> return_type:
            def operate(
                inst: self.instance_type, other: self.instance_type
            ) -> return_type:
                return self.operation(inst, other)

            operate.__name__ = self.name
            out = utils.native_function(operate)(obj, other)
            return Object(out, frame=stack.frame_snapshot(1))

        apply_method.__name__ = self.name
        return apply_method

    def return_type(self, obj: Object) -> Any:
        return self.return_type_opt or self.instance_type


def get_magic_methods():
    return {
        "__eq__": operator.eq,
        "__ne__": operator.ne,
        "__add__": operator.add,
        "__sub__": operator.sub,
        "__mul__": operator.mul,
        "__truediv__": operator.truediv,
        "__floordiv__": operator.floordiv,
        "__pow__": operator.pow,
        "__round__": round,
    }


def get_magic_method_types():
    return {
        "__eq__": types.BooleanType(False),
        "__ne__": types.BooleanType(False),
        "__round__": types.IntegerType(True),
    }


@dc.dataclass(frozen=True)
class BinaryMagicMethods(ObjectMethods):
    """
    Methods that are defined for all statey objects. These act a bit differently than regular
    methods since they need to be called by python internally
    """

    type: types.Type
    method_map: Dict[str, Callable[[Any, Any], Any]] = dc.field(
        default_factory=get_magic_methods
    )
    method_map_type_overrides: Dict[str, types.Type] = dc.field(
        default_factory=get_magic_method_types
    )

    def get_method(self, name: str) -> Method:
        try:
            method = self.method_map[name]
        except KeyError as err:
            raise st.exc.NoSuchMethodError(name) from err
        return BinaryMagicMethod(
            name=name,
            operation=method,
            instance_type=self.type,
            return_type_opt=self.method_map_type_overrides.get(name),
        )

    @classmethod
    @st.hookimpl
    def get_methods(cls, type: types.Type, registry: "Registry") -> ObjectMethods:
        if type.nullable:
            return None
        return cls(type)


@dc.dataclass(frozen=True)
class ExpectedValueMethod(Method):
    """
    Method to easily create expected values
    """

    def bind(self, obj: Object) -> Any:
        def expect(value: Any) -> Object:
            expected_obj = Object(value, obj._type, obj._registry)
            return Object(
                impl.ExpectedValue(obj, expected_obj), frame=stack.frame_snapshot(1)
            )

        return expect

    def return_type(self, obj: Object) -> Any:
        return utils.single_arg_function_type(obj._type, obj._type)


@dc.dataclass(frozen=True)
class BaseObjectMethods(SimpleObjectMethods):
    """
    Defines methods available for all objects
    """

    type: types.Type

    def methods(self) -> Dict[str, Method]:
        return {"__rshift__": ExpectedValueMethod()}

    @classmethod
    @st.hookimpl
    def get_methods(cls, type: types.Type, registry: "Registry") -> ObjectMethods:
        if type.nullable:
            return None
        return cls(type)


@dc.dataclass(frozen=True)
class StringMethods(DeclarativeMethods):
    """
    Methods available for string objects
    """

    @method
    def split(self: str, delim: str) -> Sequence[str]:
        return self.split(delim)

    @method
    def strip(self: str) -> str:
        return self.strip()

    @st.hookimpl
    def get_methods(self, type: types.Type, registry: "Registry") -> ObjectMethods:
        if not isinstance(type, types.StringType) or type.nullable:
            return None
        return self


@dc.dataclass(frozen=True)
class OptionMethod(Method):
    """
    Turn every method call into something like a scala option.map()
    """

    type: types.Type
    base_method: Method

    def return_type(self, obj: Object) -> Any:
        return self.base_method.return_type(obj).with_nullable(True)

    def bind(self, obj: Object) -> Any:

        handler = self.base_method.bind(obj)
        return_type = self.return_type(obj)

        if isinstance(return_type, types.FunctionType):

            nullable_return = return_type.return_type.with_nullable(True)

            def out_handler(*args, **kwargs):
                @utils.native_function
                def option_handler(data: self.type) -> nullable_return:
                    if data is None:
                        return None
                    return handler(*args, **kwargs)

                return option_handler(obj)

            return out_handler

        @utils.native_function
        def option_handler(data: self.type) -> return_type:
            if data is None:
                return None
            return handler

        return option_handler(obj)


@dc.dataclass(frozen=True)
class OptionMethods(ObjectMethods):
    """
    Turn every method call into something like a scala option.map()
    """

    type: types.Type
    methods: ObjectMethods

    def get_method(self, name: str) -> Method:
        base_method = self.methods.get_method(name)
        return OptionMethod(self.type, base_method)

    @classmethod
    @st.hookimpl
    def get_methods(cls, type: types.Type, registry: "Registry") -> ObjectMethods:
        if not type.nullable:
            return None
        non_nullable_methods = registry.get_methods(type.with_nullable(False))
        return cls(type, non_nullable_methods)


OBJECT_METHODS_PLUGINS = [
    CallFunctionMethod,
    BinaryMagicMethods,
    BaseObjectMethods,
    StringMethods(),
    OptionMethods,
]


def register(registry: Optional["Registry"] = None) -> None:
    """
    Replace default object methods classes
    """
    if registry is None:
        registry = st.registry

    for cls in OBJECT_METHODS_PLUGINS:
        registry.register(cls)
