import abc
import base64

import dataclasses as dc
from typing import Type as PyType, Any, Dict, Optional

import marshmallow as ma
import pickle
import pluggy

import statey as st
from statey.syms import types, utils, Object


class Encoder(abc.ABC):
    """
	An encoder encodes data of some with possibly native types info some format
	"""

    type: types.Type

    @abc.abstractmethod
    def encode(self, type: types.Type, value: Any) -> Any:
        """
		Given a type and some _non-validated_ value, convert it to a serializable value
		"""
        raise NotImplementedError

    @abc.abstractmethod
    def decode(self, type: types.Type, value: Any) -> Any:
        """
		Given a freshly deserialized dictionary, potentially apply some post-processing or wrap
		it in a native type
		"""
        raise NotImplementedError


class EncoderHooks:
    """
	Hooks to wrap encoder functionality
	"""

    @st.hookspec(firstresult=True)
    def encode(self, value: Any) -> Any:
        """
		Optionally apply some logic to encode the given value. return None if the given value is not handled.
		"""

    @st.hookspec(firstresult=True)
    def decode(self, value: Any) -> Any:
        """
		Opposite of the encode() hook
		"""


def create_encoder_plugin_manager():
    """
	Factory function to create the default plugin manager for encoders
	"""
    pm = st.create_plugin_manager()
    pm.add_hookspecs(EncoderHooks)
    return pm


class HookHandlingEncoder(Encoder):
    """
    Handles hooks properly in encode() and decode()
    """

    def encode(self, value: Any) -> Any:
        result = self.pm.hook.encode(value=value)
        return value if result is None else result

    def decode(self, value: Any) -> Any:
        result = self.pm.hook.decode(value=value)
        return value if result is None else result


@dc.dataclass(frozen=True)
class DefaultEncoder(HookHandlingEncoder, utils.Cloneable):
    """
	The default encoder just handles hooks properly, doesn't do any actual encoding
	"""

    type: types.Type
    pm: pluggy.PluginManager = dc.field(
        init=False,
        default_factory=create_encoder_plugin_manager,
        compare=False,
        repr=False,
    )

    @classmethod
    @st.hookimpl
    def get_encoder(
        cls, type: types.Type, registry: "Registry", serializable: bool
    ) -> Encoder:
        """
		The basic encoder behavior just calls hooks, but we should pass through plugins too.
		"""
        if serializable:
            return None
        inst = cls(type)
        for plugin in type.meta.get("plugins", []):
            inst.pm.register(plugin)
        return inst


@dc.dataclass(frozen=True)
class MarshmallowEncoder(HookHandlingEncoder):
    """
	Encodeable helper to get all functionality from a field factory
	"""

    type: types.Type
    registry: "Registry"
    pm: pluggy.PluginManager = dc.field(
        init=False,
        compare=False,
        repr=False,
        default_factory=create_encoder_plugin_manager,
    )

    @abc.abstractmethod
    def base_marshmallow_field(self, encoding: bool) -> ma.fields.Field:
        """
		Return the marshmallow field for this type
		"""
        raise NotImplementedError

    def marshmallow_field(self, encoding: bool) -> ma.fields.Field:
        kws = self._marshmallow_field_kws(self.type.nullable, self.type.meta)
        base = self.base_marshmallow_field(encoding)
        return utils.PossiblySymbolicField(base, self.type, self.registry, **kws)

    def encode(self, data: Any) -> Any:
        # Allow pre-encoding hooks
        data = super().encode(data)
        field = self.marshmallow_field(True)
        with utils.reraise_ma_validation_error():
            # This does the validation
            data = field.deserialize(data)
        # This allows us to leverage marshmallow to do things like encoding
        # dates as strings w/ symmetrical encoding/decoding logic
        return field.serialize("tmp", {"tmp": data})

    def decode(self, data: Any) -> Any:
        with utils.reraise_ma_validation_error():
            value = self.marshmallow_field(False).deserialize(data)
            # Allow post-decoding hooks
            return super().decode(value)

    @staticmethod
    def _marshmallow_field_kws(nullable: bool, meta: Dict[str, Any]) -> Dict[str, Any]:
        default = meta.get("default", utils.MISSING)
        validate = meta.get("validator", utils.MISSING)
        if nullable:
            kws = {
                "required": False,
                "default": None if utils.is_missing(default) else default,
                "missing": None if utils.is_missing(default) else default,
                "allow_none": True,
            }
        elif utils.is_missing(default):
            kws = {"required": True}
        else:
            kws = {"missing": default, "default": default}

        if not utils.is_missing(validate):
            kws["validate"] = validate

        return kws


class MarshmallowValueEncoder(MarshmallowEncoder):
    """
	Simple marshmallow encoder for value types
	"""

    base_field: ma.fields.Field
    type_cls: PyType[types.Type]
    serializable: bool

    def base_marshmallow_field(self, encoding: bool) -> ma.fields.Field:
        return self.base_field

    @classmethod
    @st.hookimpl
    def get_encoder(
        cls, type: types.Type, registry: "Registry", serializable: bool
    ) -> Encoder:
        if serializable and not cls.serializable:
            return None
        if isinstance(type, cls.type_cls):
            instance = cls(type, registry)
            for plugin in type.meta.get("plugins", []):
                instance.pm.register(plugin)
            return instance
        return None


@dc.dataclass(frozen=True)
class IntegerEncoder(MarshmallowValueEncoder):
    type_cls = types.IntegerType
    base_field = ma.fields.Int()
    serializable = True


@dc.dataclass(frozen=True)
class FloatEncoder(MarshmallowValueEncoder):
    type_cls = types.FloatType
    base_field = ma.fields.Float()
    serializable = True


@dc.dataclass(frozen=True, repr=False)
class BooleanEncoder(MarshmallowValueEncoder):
    type_cls = types.BooleanType
    base_field = ma.fields.Bool()
    serializable = True


@dc.dataclass(frozen=True, repr=False)
class StringEncoder(MarshmallowValueEncoder):
    type_cls = types.StringType
    base_field = ma.fields.Str()
    serializable = True


@dc.dataclass(frozen=True, repr=False)
class ArrayEncoder(MarshmallowEncoder):
    """
	An array with some element type
	"""

    element_encoder: Encoder

    def base_marshmallow_field(self, encoding: bool) -> ma.fields.Field:
        kws = self._marshmallow_field_kws(
            self.element_encoder.type.nullable, self.element_encoder.type.meta
        )
        if encoding:
            kws["serialize"] = lambda x: x
            kws["deserialize"] = self.element_encoder.encode
        else:
            kws["serialize"] = lambda x: x
            kws["deserialize"] = self.element_encoder.decode

        element_field = utils.SingleValueFunction(**kws)
        return ma.fields.List(element_field)

    @classmethod
    @st.hookimpl
    def get_encoder(
        cls, type: types.Type, registry: "Registry", serializable: bool
    ) -> Encoder:
        if not isinstance(type, types.ArrayType):
            return None
        element_encoder = registry.get_encoder(type.element_type, serializable)
        instance = cls(type, registry, element_encoder)
        for plugin in type.meta.get("plugins", []):
            instance.pm.register(plugin)
        return instance


@dc.dataclass(frozen=True, repr=False)
class StructEncoder(MarshmallowEncoder):

    field_encoders: Dict[str, Encoder]

    def base_marshmallow_field(self, encoding: bool) -> ma.fields.Field:
        return ma.fields.Nested(self.marshmallow_schema(encoding))

    def marshmallow_schema(self, encoding: bool) -> ma.Schema:
        fields = {}
        for name, encoder in self.field_encoders.items():
            kws = self._marshmallow_field_kws(encoder.type.nullable, encoder.type.meta)
            if encoding:
                kws["serialize"] = lambda x: x
                kws["deserialize"] = encoder.encode
            else:
                kws["serialize"] = lambda x: x
                kws["deserialize"] = encoder.decode
            fields[name] = utils.SingleValueFunction(**kws)
        return type("StructSchema", (ma.Schema,), fields)()

    @classmethod
    @st.hookimpl
    def get_encoder(
        cls, type: types.Type, registry: "Registry", serializable: bool
    ) -> Encoder:
        if not isinstance(type, types.StructType):
            return None
        encoders = {}
        for field in type.fields:
            encoders[field.name] = registry.get_encoder(field.type, serializable)
        instance = cls(type, registry, encoders)
        for plugin in type.meta.get("plugins", []):
            instance.pm.register(plugin)
        return instance


@dc.dataclass(frozen=True)
class NativeFunctionEncoder(StructEncoder):
    """
    Encoder for native python functions
    """

    module: Any = pickle

    def encode(self, value: Any) -> Any:
        if isinstance(value, Object) or value is None:
            return super().encode(value)

        serialized_bytes = self.module.dumps(value.func)
        converted = {
            "serialized": base64.b64encode(serialized_bytes),
            "name": value.name,
        }
        return super().encode(converted)

    def decode(self, value: Any) -> Any:
        from statey.syms import func

        value = super().decode(value)
        if isinstance(value, Object) or value is None:
            return value
        function_ob = self.module.loads(base64.b64decode(value["serialized"]))
        return func.NativeFunction(self.type, function_ob, value["name"])

    @classmethod
    @st.hookimpl
    def get_encoder(
        cls, type: types.Type, registry: "Registry", serializable: bool
    ) -> Encoder:
        if not isinstance(type, types.NativeFunctionType):
            return None
        as_struct = types.StructType(type.fields, type.nullable, type.meta)
        struct_encoder = registry.get_encoder(as_struct, serializable)
        return cls(type, registry, struct_encoder.field_encoders)


@dc.dataclass(frozen=True, repr=False)
class MapEncoder(MarshmallowEncoder):
    """
    An array with some element type
    """

    key_encoder: Encoder
    value_encoder: Encoder

    def base_marshmallow_field(self, encoding: bool) -> ma.fields.Field:
        key_kws = self._marshmallow_field_kws(
            self.key_encoder.type.nullable, self.key_encoder.type.meta
        )
        if encoding:
            key_kws["serialize"] = lambda x: x
            key_kws["deserialize"] = self.key_encoder.encode
        else:
            key_kws["serialize"] = lambda x: x
            key_kws["deserialize"] = self.key_encoder.decode

        key_field = utils.SingleValueFunction(**key_kws)

        value_kws = self._marshmallow_field_kws(
            self.value_encoder.type.nullable, self.value_encoder.type.meta
        )
        if encoding:
            value_kws["serialize"] = lambda x: x
            value_kws["deserialize"] = self.value_encoder.encode
        else:
            value_kws["serialize"] = lambda x: x
            value_kws["deserialize"] = self.value_encoder.decode

        value_field = utils.SingleValueFunction(**value_kws)
        return ma.fields.Dict(keys=key_field, values=value_field)

    @classmethod
    @st.hookimpl
    def get_encoder(
        cls, type: types.Type, registry: "Registry", serializable: bool
    ) -> Encoder:
        if not isinstance(type, types.MapType):
            return None
        key_encoder = registry.get_encoder(type.key_type, serializable)
        value_encoder = registry.get_encoder(type.value_type, serializable)
        instance = cls(type, registry, key_encoder, value_encoder)
        for plugin in type.meta.get("plugins", []):
            instance.pm.register(plugin)
        return instance


ENCODER_CLASSES = [
    DefaultEncoder,
    IntegerEncoder,
    FloatEncoder,
    BooleanEncoder,
    StringEncoder,
    ArrayEncoder,
    StructEncoder,
    NativeFunctionEncoder,
    MapEncoder,
]


# We'll prefer a better pickling module if we have one.
try:
    import dill
except ImportError:
    import warnings

    warnings.warn("Dill is not installed.", RuntimeWarning)
else:

    @dc.dataclass(frozen=True)
    class DillFunctionEncoder(NativeFunctionEncoder):
        """
        dill-based python function encoder
        """

        module: Any = dill

    ENCODER_CLASSES.append(DillFunctionEncoder)


try:
    import cloudpickle
except ImportError:
    import warnings

    warnings.warn("Cloudpickle is not installed.", RuntimeWarning)
else:

    @dc.dataclass(frozen=True)
    class CloudPickleFunctionEncoder(NativeFunctionEncoder):
        """
        cloudpickle-based python function encoder
        """

        module: Any = cloudpickle

    ENCODER_CLASSES.append(CloudPickleFunctionEncoder)


def register(registry: Optional["Registry"] = None) -> None:
    """
	Replace default encoder with encoders defined here
	"""
    if registry is None:
        registry = st.registry

    for cls in ENCODER_CLASSES:
        registry.register(cls)
