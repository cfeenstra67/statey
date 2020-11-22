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
    def cast_value(self, value: Any) -> Any:
        """
        Cast a concrete value between types rather than an object
        """
        raise NotImplementedError

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

    def cast(self, obj: Object) -> Object:
        func_type = utils.single_arg_function_type(obj._type, self.type)
        function = func.NativeFunction(
            type=func_type,
            func=self.cast_value,
            name=f"cast[{obj._type} => {self.type}]",
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

    def cast_value(self, value: Any) -> Any:
        return value

    def cast(self, obj: Object) -> Object:
        return Object(obj._impl, self.type, obj._registry)


@dc.dataclass(frozen=True)
class ArrayElementCaster(ValueCaster):
    """
    Caster for array types
    """

    type: types.ArrayType
    element_caster: Caster

    def cast_value(self, value: Any) -> Any:
        if value is None:
            return None
        return [self.element_caster.cast_value(item) for item in value]


@dc.dataclass(frozen=True)
class MapElementCaster(ValueCaster):
    """
    Caster for map types
    """

    type: types.MapType
    key_caster: Caster
    value_caster: Caster

    def cast_value(self, value: Any) -> Any:
        if value is None:
            return None
        return {
            self.key_caster.cast_value(key): self.value_caster.cast_value(val)
            for key, val in value.items()
        }


@dc.dataclass(frozen=True)
class StructCaster(Caster):
    """
    Caster for struct types
    """

    type: types.StructType
    field_casters: Dict[str, Caster]

    def cast_value(self, value: Any) -> Any:
        if value is None:
            return None
        return {
            field: caster.cast_value(value[field]) for field, caster in value.items()
        }

    def cast(self, obj: Object) -> Object:
        out = {}
        for field in self.type.fields:
            caster = self.field_casters[field.name]
            out[field.name] = caster.cast(obj[field.name])
        return Object(out, self.type, obj._registry)


@dc.dataclass(frozen=True)
class PredicateCastingPlugin:
    """
    Provide a (from_type, to_type) => bool predicate and a caster factory
    """

    factory: Callable[[types.Type, types.Type, "Registry"], Optional[Caster]]

    @st.hookimpl
    def get_caster(
        self, from_type: types.Type, to_type: types.Type, registry: "Registry"
    ) -> Caster:
        return self.factory(from_type, to_type, registry)


def quasi_equal_cast(
    from_type: types.Type, to_type: types.Type, registry: "Registry"
) -> Optional[Caster]:
    """
    Predicate returning True when the types are the same other than metadata
    """
    if from_type == to_type.with_meta(
        from_type.meta
    ) or from_type == to_type.with_nullable(False).with_meta(from_type.meta):
        return ForceCaster(to_type)
    return None


def array_cast(
    from_type: types.Type, to_type: types.Type, registry: "Registry"
) -> Optional[Caster]:
    """
    Simple identity caster for arrays
    """
    if not isinstance(from_type, types.ArrayType) or not isinstance(
        to_type, types.ArrayType
    ):
        return None

    element_caster = None
    if from_type.element_type != to_type.element_type:
        try:
            element_caster = registry.get_caster(
                from_type.element_type, to_type.element_type
            )
        except st.exc.NoCasterFound:
            return None

    with_from_element = to_type.with_element_type(from_type.element_type)
    if from_type == with_from_element.with_meta(
        from_type.meta
    ) or from_type == with_from_element.with_nullable(False).with_meta(from_type.meta):
        if element_caster is None or isinstance(element_caster, ForceCaster):
            return ForceCaster(to_type)

        return ArrayElementCaster(to_type, element_caster)

    return None


def struct_cast(
    from_type: types.Type, to_type: types.Type, registry: "Registry"
) -> Optional[Caster]:
    """
    Simple identity caster for structs
    """
    if not isinstance(from_type, types.StructType) or not isinstance(
        to_type, types.StructType
    ):
        return None
    if {field.name for field in from_type.fields} != {
        field.name for field in to_type.fields
    }:
        return None

    field_casters = None
    if {field.name: field for field in from_type.fields} != {
        field.name: field for field in to_type.fields
    }:
        field_casters = {}
        for field in to_type.fields:
            try:
                field_caster = registry.get_caster(
                    from_type[field.name].type, field.type
                )
            except st.exc.NoCasterFound:
                return None
            field_casters[field.name] = field_caster

    with_from_fields = to_type.with_fields(from_type.fields)
    if from_type == with_from_fields.with_meta(
        from_type.meta
    ) or from_type == with_from_fields.with_nullable(False).with_meta(from_type.meta):
        if field_casters is None or all(
            isinstance(caster, ForceCaster) for caster in field_casters.values()
        ):
            return ForceCaster(to_type)

        return StructCaster(to_type, field_casters)

    return None


def map_cast(
    from_type: types.Type, to_type: types.Type, registry: "Registry"
) -> Optional[Caster]:
    """
    Simple identity caster for maps
    """
    if not isinstance(from_type, types.MapType) or not isinstance(
        to_type, types.MapType
    ):
        return None

    key_caster = None
    if from_type.key_type != to_type.key_type:
        try:
            key_caster = registry.get_caster(from_type.key_type, to_type.value_type)
        except st.exc.NoCasterFound:
            return None

    value_caster = None
    if from_type.value_type != to_type.value_type:
        try:
            value_caster = registry.get_caster(from_type.value_type, to_type.value_type)
        except st.exc.NoCasterFound:
            return None

    with_same_subtypes = to_type.with_key_type(from_type.key_type).with_value_type(
        from_type.value_type
    )

    if from_type == with_same_subtypes.with_meta(
        from_type.meta
    ) or from_type == with_same_subtypes.with_nullable(False).with_meta(from_type.meta):
        if (key_caster is None and value_caster is None) or (
            isinstance(key_caster, ForceCaster)
            and isinstance(value_caster, ForceCaster)
        ):
            return ForceCaster(to_type)

        if key_caster is None:
            key_caster = ForceCaster(to_type.key_type)

        if value_caster is None:
            value_caster = ForceCaster(to_type.value_type)

        return MapElementCaster(to_type, key_caster, value_caster)

    return None


def number_cast(
    from_type: types.Type, to_type: types.Type, registry: "Registry"
) -> Optional[Caster]:
    """
    Caster allowing different numerical types to be cast to one another.
    """
    if isinstance(from_type, types.NumberType) and isinstance(
        to_type, types.NumberType
    ):
        return ForceCaster(to_type)
    return None


PLUGINS = [
    PredicateCastingPlugin(number_cast),
    PredicateCastingPlugin(array_cast),
    PredicateCastingPlugin(struct_cast),
    PredicateCastingPlugin(map_cast),
    PredicateCastingPlugin(quasi_equal_cast),
]


def register(registry: Optional["Registry"] = None) -> None:
    """
    Register casters in the default registry
    """
    if registry is None:
        registry = st.registry

    for plugin in PLUGINS:
        registry.register(plugin)
