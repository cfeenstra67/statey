import abc
import dataclasses as dc
from typing import Dict, Any, Callable, Optional

import statey as st
from statey.syms import types, utils, func
from statey.syms.object_ import Object


class Caster(abc.ABC):
    """
    A caster defines behavior to cast a value from one type to another
    """
    type: types.Type

    @abc.abstractmethod
    def cast(self, obj: Object) -> Object:
        """
        Cast the given object to this caster's type
        """
        raise NotImplementedError


class ValueCaster(Caster):
    """
    Cast from one type to another using a native python function
    """
    type: types.Type

    @abc.abstractmethod
    def cast_value(self, value: Any) -> Any:
        """
        Cast the given non-object value to `type`
        """
        raise NotImplementedError

    def cast(self, obj: Object) -> Object:
        func_type = utils.single_arg_function_type(obj._type, self.type)
        function = func.NativeFunction(
            type=func_type,
            func=self.cast_value,
            name=f"cast[{obj._type} => {self.type}]"
        )
        return obj._inst.map(function)


@dc.dataclass(frozen=True)
class FunctionalCaster(ValueCaster):
    """
    ValueCaster using a function as the cast_value() method
    """
    type: types.Type
    cast_value: Callable[[Any], Any] = dc.field()


@dc.dataclass(frozen=True)
class ForceCaster(Caster):
    """
    Caster that simply replaces the type of a given object. This will cause the object
    to be encoded as that type
    """
    type: types.Type

    def cast(self, obj: Object) -> Object:
        return Object(obj._impl, self.type, obj._registry)


@dc.dataclass(frozen=True)
class PredicateCastingPlugin:
    """
    Provide a (from_type, to_type) => bool predicate and a caster factory
    """
    predicate: Callable[[types.Type, types.Type], bool]
    factory: Callable[[types.Type], Caster]

    @st.hookimpl
    def get_caster(self, from_type: types.Type, to_type: types.Type, registry: "Registry") -> Caster:
        if not self.predicate(from_type, to_type):
            return None
        return self.factory(to_type)


PLUGINS = [
    # Cast non-nullable to nullable, can force this
    PredicateCastingPlugin(
        predicate=lambda from_type, to_type: from_type == to_type.to_nullable(False),
        factory=ForceCaster
    )
]


def register(registry: Optional["Registry"] = None) -> None:
    """
    Register casters in the default registry
    """
    if registry is None:
        registry = st.registry

    for plugin in PLUGINS:
        registry.register(plugin)
