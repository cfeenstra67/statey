import abc
import dataclasses as dc
from typing import Any, Mapping, Optional, Dict

import marshmallow as ma

import statey as st
from statey.syms import (
    impl,
    impl as impl_mod,
    Object,
    session,
    type_serializers as ts,
    utils,
    encoders,
    semantics,
    types,
)


class ObjectImplementationSerializer(abc.ABC):
    """
    Serializes an ObjectImplementation
    """

    @abc.abstractmethod
    def serialize(self, implementation: impl.ObjectImplementation) -> Any:
        """
        Serialize the given object implementation into a JSON-serializable
        format
        """
        raise NotImplementedError

    @abc.abstractmethod
    def deserialize(
        self, data: Any, session: session.Session, objects: Mapping[Any, Object]
    ) -> impl.ObjectImplementation:
        """
        Given an object decoded from JSON, construct an ObjectImplementation
        instance
        """
        raise NotImplementedError


class ReferenceSchema(ma.Schema):
    """
    Schema for serializing Reference data
    """

    name = ma.fields.Str(required=True, default="reference")
    path = ma.fields.Str(required=True)


class ReferenceSerializer(ObjectImplementationSerializer):
    """
    Serializes a Reference
    """

    def serialize(self, implementation: impl.ObjectImplementation) -> Any:
        out = {"path": implementation.path}
        return ReferenceSchema().dump(out)

    def deserialize(
        self, data: Any, session: session.Session, objects: Mapping[Any, Object]
    ) -> impl.ObjectImplementation:
        deserialized = ReferenceSchema().load(data)
        return impl.Reference(deserialized["path"], session.ns)

    @classmethod
    @st.hookimpl
    def get_impl_serializer(
        cls, impl: impl.ObjectImplementation, type: types.Type, registry: "Registry"
    ) -> ObjectImplementationSerializer:
        if not isinstance(impl, impl_mod.Reference):
            return None
        return cls()

    @classmethod
    @st.hookimpl
    def get_impl_serializer_from_data(
        cls, data: Any, registry: "Registry"
    ) -> ObjectImplementationSerializer:
        if not isinstance(data, dict) or data.get("name") != "reference":
            return None
        return cls()


class FutureSchema(ma.Schema):
    """
    Schema for serializing Future data
    """

    name = ma.fields.Str(required=True, default="future")
    refs = ma.fields.List(ma.fields.Field(), required=True, default=list)
    return_type = ma.fields.Dict(missing=None, default=None)
    result = ma.fields.Field(missing=utils.MISSING)


@dc.dataclass(frozen=True)
class FutureSerializer(ObjectImplementationSerializer):
    """
    Serializes a Future
    """

    return_type_serializer: Optional[ts.TypeSerializer] = None

    def serialize(self, implementation: impl.ObjectImplementation) -> Any:
        out = {"refs": [obj._impl.id for obj in implementation.refs]}
        if (
            self.return_type_serializer is not None
            and implementation.return_type is not None
        ):
            out["return_type"] = self.return_type_serializer.serialize(
                implementation.return_type
            )

        if not utils.is_missing(implementation.result.result):
            out["result"] = implementation.result.result

        return FutureSchema().dump(out)

    def deserialize(
        self, data: Any, session: session.Session, objects: Mapping[Any, Object]
    ) -> impl.ObjectImplementation:
        deserialized = FutureSchema().load(data)
        result_value = data["result"]
        result = impl.FutureResult(result_value)

        object_refs = [objects[ref_id] for ref_id in data["refs"]]

        return_type = None
        if data["return_type"] is not None and self.return_type_serializer is not None:
            return_type = self.return_type_serializer.deserialize(data["return_type"])

        return impl.Future(object_refs, result, return_type)

    @classmethod
    @st.hookimpl
    def get_impl_serializer(
        cls, impl: impl.ObjectImplementation, type: types.Type, registry: "Registry"
    ) -> ObjectImplementationSerializer:
        if not isinstance(impl, impl_mod.Future):
            return None
        return_type_serializer = None
        if impl.return_type is not None:
            return_type_serializer = registry.get_type_serializer(impl.return_type)
        return cls(return_type_serializer)

    @classmethod
    @st.hookimpl
    def get_impl_serializer_from_data(
        cls, data: Any, registry: "Registry"
    ) -> ObjectImplementationSerializer:
        if not isinstance(data, dict) or data.get("name") != "future":
            return None
        return_type_serializer = None
        if data["return_type"] is not None:
            return_type_serializer = registry.get_type_serializer_from_data(
                data["return_type"]
            )
        return cls(return_type_serializer)


class UnknownSchema(ma.Schema):
    """
    Schema for serializing Unknown data
    """

    name = ma.fields.Str(required=True, default="unknown")
    obj = ma.fields.Field()
    refs = ma.fields.List(ma.fields.Field(), required=True, default=list)
    return_type = ma.fields.Dict(missing=None, default=None)


@dc.dataclass(frozen=True)
class UnknownSerializer(ObjectImplementationSerializer):
    """
    Serializes an Unknown value
    """

    return_type_serializer: Optional[ts.TypeSerializer] = None

    def serialize(self, implementation: impl.ObjectImplementation) -> Any:
        out = {"refs": [obj._impl.id for obj in implementation.refs]}
        if implementation.obj is not None:
            out["obj"] = implementation.obj._impl.id

        if (
            implementation.return_type is not None
            and self.return_type_serializer is not None
        ):
            out["return_type"] = self.return_type_serializer.serialize(
                implementation.return_type
            )

        return UnknownSchema().dump(out)

    def deserialize(
        self, data: Any, session: session.Session, objects: Mapping[Any, Object]
    ) -> impl.ObjectImplementation:
        deserialized = UnknownSchema().load(data)
        refs = [objects[ref_id] for ref_id in deserialized["refs"]]

        obj = deserialized["obj"]
        if obj is not None:
            obj = objects[obj]

        return_type = None
        if (
            deserialized["return_type"] is not None
            and self.return_type_serializer is not None
        ):
            return_type = self.return_type_serializer.deserialize(
                deserialized["return_type"]
            )

        return impl.Unknown(obj, refs, return_type)

    @classmethod
    @st.hookimpl
    def get_impl_serializer(
        cls, impl: impl.ObjectImplementation, type: types.Type, registry: "Registry"
    ) -> ObjectImplementationSerializer:
        if not isinstance(impl, impl_mod.Unknown):
            return None
        return_type_serializer = registry.get_type_serializer(type)
        return cls(return_type_serializer)

    @classmethod
    @st.hookimpl
    def get_impl_serializer_from_data(
        cls, data: Any, registry: "Registry"
    ) -> ObjectImplementationSerializer:
        if not isinstance(data, dict) or data.get("name") != "unknown":
            return None
        return_type_serializer = None
        if data["return_type"] is not None:
            return_type_serializer = registry.get_type_serializer_from_data(
                data["return_type"]
            )
        return cls(return_type_serializer)


class StructFieldSchema(ma.Schema):
    """
    Schema for a StructField
    """

    name = ma.fields.Str(required=True)
    value = ma.fields.Field(required=True)


class StructSchema(ma.Schema):
    """
    Schema for a Struct
    """

    name = ma.fields.Str(required=True, default="struct")
    fields = ma.fields.List(
        ma.fields.Nested(StructFieldSchema()), required=True, default=list
    )


@dc.dataclass(frozen=True)
class StructSerializer(ObjectImplementationSerializer):
    """
    Serializes a Struct value
    """

    def serialize(self, implementation: impl.ObjectImplementation) -> Any:
        fields = []
        field_schema = StructFieldSchema()
        for field in implementation.fields:
            fields.append({"name": field.name, "value": field.value._impl.id})

        out = {"fields": fields}
        return StructSchema().dump(out)

    def deserialize(
        self, data: Any, session: session.Session, objects: Mapping[Any, Object]
    ) -> impl.ObjectImplementation:
        deserialized = StructSchema().load(data)
        fields = []
        for field in deserialized["fields"]:
            name = field["name"]
            value = objects[field["value"]]
            fields.append(impl.StructField(name, value))
        return impl.Struct(fields)

    @classmethod
    @st.hookimpl
    def get_impl_serializer(
        cls, impl: impl.ObjectImplementation, type: types.Type, registry: "Registry"
    ) -> ObjectImplementationSerializer:
        if not isinstance(impl, impl_mod.Struct):
            return None
        return cls()

    @classmethod
    @st.hookimpl
    def get_impl_serializer_from_data(
        cls, data: Any, registry: "Registry"
    ) -> ObjectImplementationSerializer:
        if not isinstance(data, dict) or data.get("name") != "struct":
            return None
        return cls()


class ExpectedValueSchema(ma.Schema):
    """
    Schema for an ExpectedValue
    """

    name = ma.fields.Str(required=True, default="expected_value")
    obj = ma.fields.Field(required=True)
    expected = ma.fields.Field(required=True)


@dc.dataclass(frozen=True)
class ExpectedValueSerializer(ObjectImplementationSerializer):
    """
    Serializes an ExpectedValue value
    """

    def serialize(self, implementation: impl.ObjectImplementation) -> Any:
        out = {
            "obj": implementation.obj._impl.id,
            "expected": implementation.expected._impl.id,
        }
        return ExpectedValueSchema().dump(out)

    def deserialize(
        self, data: Any, session: session.Session, objects: Mapping[Any, Object]
    ) -> impl.ObjectImplementation:
        deserialized = ExpectedValueSchema().load(data)
        obj = objects[deserialized["obj"]]
        expected = objects[deserialized["expected"]]
        return impl.ExpectedValue(obj, expected)

    @classmethod
    @st.hookimpl
    def get_impl_serializer(
        cls, impl: impl.ObjectImplementation, type: types.Type, registry: "Registry"
    ) -> ObjectImplementationSerializer:
        if not isinstance(impl, impl_mod.ExpectedValue):
            return None
        return cls()

    @classmethod
    @st.hookimpl
    def get_impl_serializer_from_data(
        cls, data: Any, registry: "Registry"
    ) -> ObjectImplementationSerializer:
        if not isinstance(data, dict) or data.get("name") != "expected_value":
            return None
        return cls()


class DataSerializer(ObjectImplementationSerializer):
    """
    Base class for serializers for Data implementations
    """


class DataSchema(ma.Schema):
    """
    Schema for a data value w/o an algebraic type
    """

    name = ma.fields.Str(required=True, default="data")
    value = ma.fields.Field(required=True)
    type = ma.fields.Dict(required=True)
    value_type = ma.fields.Dict(missing=None, default=None)


@dc.dataclass(frozen=True)
class DataValueSerializer(DataSerializer):
    """
    Serializes a Data value w/ a non-algebraic data type
    """

    value_encoder: encoders.Encoder
    type_serializer: ts.TypeSerializer
    value_type_serializer: Optional[ts.TypeSerializer] = None

    def serialize(self, implementation: impl.ObjectImplementation) -> Any:
        out = {"type": self.type_serializer.serialize(self.value_encoder.type)}
        if isinstance(implementation.value, Object):
            out["value"] = {"object": implementation.value._impl.id}
        else:
            out["value"] = {"data": self.value_encoder.encode(implementation.value)}

        if (
            implementation.value_type is not None
            and self.value_type_serializer is not None
        ):
            out["value_type"] = self.value_type_serializer.serialize(
                implementation.value_type
            )

        return DataSchema().dump(out)

    def deserialize(
        self, data: Any, session: session.Session, objects: Mapping[Any, Object]
    ) -> impl.ObjectImplementation:

        deserialized = DataSchema().load(data)
        val_json = deserialized["value"]
        if "object" in val_json:
            value = objects[val_json["object"]]
        elif "data" in val_json:
            value = self.value_encoder.decode(val_json["data"])
        else:
            raise ValueError(f"Unable to interpret {val_json}!")

        value_type = None
        if (
            deserialized["value_type"] is not None
            and self.value_type_serializer is not None
        ):
            value_type = self.value_type_serializer.deserialize(
                deserialized["value_type"]
            )

        return impl.Data(value, value_type)

    @classmethod
    @st.hookimpl
    def get_impl_serializer(
        cls, impl: impl.ObjectImplementation, type: types.Type, registry: "Registry"
    ) -> ObjectImplementationSerializer:
        if not isinstance(impl, impl_mod.Data):
            return None
        value_encoder = registry.get_encoder(type, serializable=True)
        value_type_serializer = None
        if impl.value_type is not None:
            value_type_serializer = registry.get_type_serializer(impl.value_type)

        type_serializer = registry.get_type_serializer(type)
        return cls(value_encoder, type_serializer, value_type_serializer)

    @classmethod
    @st.hookimpl
    def get_impl_serializer_from_data(
        cls, data: Any, registry: "Registry"
    ) -> ObjectImplementationSerializer:
        if not isinstance(data, dict) or data.get("name") != "data":
            return None
        type_ser = registry.get_type_serializer_from_data(data["type"])
        typ = type_ser.deserialize(data["type"])
        value_encoder = registry.get_encoder(typ)

        value_type_ser = None
        if data["value_type"] is not None:
            value_type_ser = registry.get_type_serializer_from_data(data["value_type"])

        return cls(value_encoder, type_ser, value_type_ser)


@dc.dataclass(frozen=True)
class DataArraySerializer(DataSerializer):
    """
    Data serializer for array types
    """

    encoder: encoders.Encoder
    type_serializer: ts.TypeSerializer
    element_serializer: ObjectImplementationSerializer
    value_type_serializer: Optional[ts.TypeSerializer] = None

    def serialize(self, implementation: impl.ObjectImplementation) -> Any:
        out = {"type": self.type_serializer.serialize(self.encoder.type)}
        if isinstance(implementation.value, Object):
            out["value"] = {"object": implementation.value._impl.id}
        else:
            data = []
            if implementation.value is None:
                data = None
            else:
                encoded_value = self.encoder.encode(implementation.value)
                for item in encoded_value:
                    if isinstance(item, Object):
                        data.append({"object": item._impl.id})
                    else:
                        new_impl = impl.Data(item)
                        serialized = self.element_serializer.serialize(new_impl)[
                            "value"
                        ]
                        data.append(serialized)

            out["value"] = {"data": data}

        if (
            implementation.value_type is not None
            and self.value_type_serializer is not None
        ):
            out["value_type"] = self.value_type_serializer.serialize(
                implementation.value_type
            )

        return DataSchema().dump(out)

    def deserialize(
        self, data: Any, session: session.Session, objects: Mapping[Any, Object]
    ) -> impl.ObjectImplementation:

        deserialized = DataSchema().load(data)
        val_json = deserialized["value"]
        if "object" in val_json:
            value = objects[val_json["object"]]
        elif "data" in val_json:
            value_list = val_json["data"]
            value = []
            if value_list is None:
                value = None
            else:
                for item in value_list:
                    if "object" in item:
                        item_value = objects[item["object"]]
                    elif "data" in item:
                        sub_impl = self.element_serializer.deserialize(
                            {"value": item["data"]}, session, objects
                        )
                        item_value = sub_impl.value
                    else:
                        raise ValueError(f"Unable to interpret {val_json}!")

                    value.append(item_value)
        else:
            raise ValueError(f"Unable to interpret {val_json}!")

        value_type = None
        if (
            deserialized["value_type"] is not None
            and self.value_type_serializer is not None
        ):
            value_type = self.value_type_serializer.deserialize(
                deserialized["value_type"]
            )

        return impl.Data(value, value_type)

    @classmethod
    @st.hookimpl
    def get_impl_serializer(
        cls, impl: impl.ObjectImplementation, type: types.Type, registry: "Registry"
    ) -> ObjectImplementationSerializer:
        if not isinstance(impl, impl_mod.Data):
            return None
        if not isinstance(type, types.ArrayType):
            return None
        encoder = registry.get_encoder(type, serializable=True)
        element_serializer = registry.get_impl_serializer(
            impl=impl_mod.Data(None), type=type.element_type
        )

        value_type_serializer = None
        if impl.value_type is not None:
            value_type_serializer = registry.get_type_serializer(impl.value_type)

        type_serializer = registry.get_type_serializer(type)
        return cls(encoder, type_serializer, element_serializer, value_type_serializer)

    @classmethod
    @st.hookimpl
    def get_impl_serializer_from_data(
        cls, data: Any, registry: "Registry"
    ) -> ObjectImplementationSerializer:
        if not isinstance(data, dict) or data.get("name") != "data":
            return None

        type_ser = registry.get_type_serializer_from_data(data["type"])
        typ = type_ser.deserialize(data["type"])
        if not isinstance(typ, types.ArrayType):
            return None

        encoder = registry.get_encoder(typ)
        element_type_ser = registry.get_type_serializer(typ.element_type)

        element_data = {
            "name": "data",
            "value": None,
            "type": element_type_ser.serialize(typ.element_type),
            "value_type": None,
        }
        element_serializer = registry.get_impl_serializer_from_data(element_data)

        value_type_ser = None
        if data["value_type"] is not None:
            value_type_ser = registry.get_type_serializer_from_data(data["value_type"])

        return cls(encoder, type_ser, element_serializer, value_type_ser)


@dc.dataclass(frozen=True)
class DataMapSerializer(DataSerializer):
    """
    Data serializer for array types
    """

    encoder: encoders.Encoder
    type_serializer: ts.TypeSerializer
    key_serializer: ObjectImplementationSerializer
    value_serializer: ObjectImplementationSerializer
    value_type_serializer: Optional[ts.TypeSerializer] = None

    def serialize(self, implementation: impl.ObjectImplementation) -> Any:
        out = {"type": self.type_serializer.serialize(self.encoder.type)}
        if isinstance(implementation.value, Object):
            out["value"] = {"object": implementation.value._impl.id}
        else:
            data = []
            if implementation.value is None:
                data = None
            else:
                encoded_value = self.encoder.encode(implementation.value)
                for key, val in encoded_value.items():
                    if isinstance(key, Object):
                        key_dict = {"object": key._impl.id}
                    else:
                        new_impl = impl.Data(key)
                        key_dict = self.key_serializer.serialize(new_impl)["value"]

                    if isinstance(value, Object):
                        value_dict = {"object": value._impl.id}
                    else:
                        new_impl = impl.Data(value)
                        value_dict = self.value_serializer.serialize(new_impl)["value"]

                    data.append((key_dict, value_dict))

            out["value"] = {"data": data}

        if (
            implementation.value_type is not None
            and self.value_type_serializer is not None
        ):
            out["value_type"] = self.value_type_serializer.serialize(
                implementation.value_type
            )

        return DataSchema().dump(out)

    def deserialize(
        self, data: Any, session: session.Session, objects: Mapping[Any, Object]
    ) -> impl.ObjectImplementation:

        deserialized = DataSchema().load(data)
        val_json = deserialized["value"]
        if "object" in val_json:
            value = objects[val_json["object"]]
        elif "data" in val_json:
            value_list = val_json["data"]
            value = {}
            if value_list is None:
                value = None
            else:
                for key_dict, value_dict in value_list:
                    if "object" in key_dict:
                        key = objects[key_dict["object"]]
                    elif "data" in key_dict:
                        sub_impl = self.element_serializer.deserialize(
                            {"value": key_dict["data"]}, session, objects
                        )
                        key = sub_impl.value
                    else:
                        raise ValueError(f"Unable to interpret {key_dict}!")

                    if "object" in value_dict:
                        val = objects[value_dict["object"]]
                    elif "data" in value_dict:
                        sub_impl = self.element_serializer.deserialize(
                            {"value": value_dict["data"]}, session, objects
                        )
                        val = sub_impl.value
                    else:
                        raise ValueError(f"Unable to interpret {value_dict}!")

                    value[key] = val

        else:
            raise ValueError(f"Unable to interpret {val_json}!")

        value_type = None
        if (
            deserialized["value_type"] is not None
            and self.value_type_serializer is not None
        ):
            value_type = self.value_type_serializer.deserialize(
                deserialized["value_type"]
            )

        return impl.Data(value, value_type)

    @classmethod
    @st.hookimpl
    def get_impl_serializer(
        cls, impl: impl.ObjectImplementation, type: types.Type, registry: "Registry"
    ) -> ObjectImplementationSerializer:
        if not isinstance(impl, impl_mod.Data):
            return None
        if not isinstance(type, types.ArrayType):
            return None
        encoder = registry.get_encoder(type, serializable=True)
        element_serializer = registry.get_impl_serializer(
            impl=impl_mod.Data(None), type=type.element_type
        )

        value_type_serializer = None
        if impl.value_type is not None:
            value_type_serializer = registry.get_type_serializer(impl.value_type)

        type_serializer = registry.get_type_serializer(type)
        return cls(encoder, type_serializer, element_serializer, value_type_serializer)

    @classmethod
    @st.hookimpl
    def get_impl_serializer_from_data(
        cls, data: Any, registry: "Registry"
    ) -> ObjectImplementationSerializer:
        if not isinstance(data, dict) or data.get("name") != "data":
            return None

        type_ser = registry.get_type_serializer_from_data(data["type"])
        typ = type_ser.deserialize(data["type"])
        if not isinstance(typ, types.ArrayType):
            return None

        encoder = registry.get_encoder(typ)
        element_type_ser = registry.get_type_serializer(typ.element_type)

        element_data = {
            "name": "data",
            "value": None,
            "type": element_type_ser.serialize(typ.element_type),
            "value_type": None,
        }
        element_serializer = registry.get_impl_serializer_from_data(element_data)

        value_type_ser = None
        if data["value_type"] is not None:
            value_type_ser = registry.get_type_serializer_from_data(data["value_type"])

        return cls(encoder, type_ser, element_serializer, value_type_ser)


@dc.dataclass(frozen=True)
class DataStructSerializer(DataSerializer):
    """
    Data serializer for struct types
    """

    encoder: encoders.Encoder
    type_serializer: ts.TypeSerializer
    field_serializers: Dict[str, ObjectImplementationSerializer]
    value_type_serializer: Optional[ts.TypeSerializer] = None

    def serialize(self, implementation: impl.ObjectImplementation) -> Any:
        out = {"type": self.type_serializer.serialize(self.encoder.type)}
        if isinstance(implementation.value, Object):
            out["value"] = {"object": implementation.value._impl.id}
        else:
            data = {}
            if implementation.value is None:
                data = None
            else:
                encoded_value = self.encoder.encode(implementation.value)

                for field, value in encoded_value.items():
                    if isinstance(value, Object):
                        data[field] = {"object": value._impl.id}
                    else:
                        serializer = self.field_serializers[field]
                        new_impl = impl.Data(value)
                        serialized = serializer.serialize(new_impl)["value"]
                        data[field] = serialized

            out["value"] = {"data": data}

        if (
            implementation.value_type is not None
            and self.value_type_serializer is not None
        ):
            out["value_type"] = self.value_type_serializer.serialize(
                implementation.value_type
            )

        return DataSchema().dump(out)

    def deserialize(
        self, data: Any, session: session.Session, objects: Mapping[Any, Object]
    ) -> impl.ObjectImplementation:

        deserialized = DataSchema().load(data)
        val_json = deserialized["value"]
        if "object" in val_json:
            value = objects[val_json["object"]]
        elif "data" in val_json:
            value_dict = val_json["data"]
            value = {}
            if value_dict is None:
                value = None
            else:
                for field, item in value_dict.items():
                    if "object" in item:
                        item_value = objects[item["object"]]
                    elif "data" in item:
                        serializer = self.field_serializers[field]
                        sub_data = {"value": {"data": item["data"]}, "name": "data"}
                        sub_impl = serializer.deserialize(sub_data, session, objects)
                        item_value = sub_impl.value
                    else:
                        raise ValueError(f"Unable to interpret {val_json}!")

                    value[field] = item_value

        else:
            raise ValueError(f"Unable to interpret {val_json}!")

        value_type = None
        if (
            deserialized["value_type"] is not None
            and self.value_type_serializer is not None
        ):
            value_type = self.value_type_serializer.deserialize(
                deserialized["value_type"]
            )

        return impl.Data(value, value_type)

    @classmethod
    @st.hookimpl
    def get_impl_serializer(
        cls, impl: impl.ObjectImplementation, type: types.Type, registry: "Registry"
    ) -> ObjectImplementationSerializer:
        if not isinstance(impl, impl_mod.Data):
            return None
        if not isinstance(type, types.StructType):
            return None
        encoder = registry.get_encoder(type, serializable=True)
        field_serializers = {}
        for field in type.fields:
            field_serializers[field.name] = registry.get_impl_serializer(
                impl=impl_mod.Data(None), type=field.type
            )

        value_type_serializer = None
        if impl.value_type is not None:
            value_type_serializer = registry.get_type_serializer(impl.value_type)

        type_serializer = registry.get_type_serializer(type)
        return cls(encoder, type_serializer, field_serializers, value_type_serializer)

    @classmethod
    @st.hookimpl
    def get_impl_serializer_from_data(
        cls, data: Any, registry: "Registry"
    ) -> ObjectImplementationSerializer:
        if not isinstance(data, dict) or data.get("name") != "data":
            return None

        type_ser = registry.get_type_serializer_from_data(data["type"])
        typ = type_ser.deserialize(data["type"])
        if not isinstance(typ, types.StructType):
            return None

        encoder = registry.get_encoder(typ)

        field_serializers = {}
        for field in typ.fields:
            field_type_ser = registry.get_type_serializer(field.type)
            field_data = {
                "name": "data",
                "value": None,
                "type": field_type_ser.serialize(field.type),
                "value_type": None,
            }
            field_serializers[field.name] = registry.get_impl_serializer_from_data(
                field_data
            )

        value_type_ser = None
        if data["value_type"] is not None:
            value_type_ser = registry.get_type_serializer_from_data(data["value_type"])

        return cls(encoder, type_ser, field_serializers, value_type_ser)


class FunctionCallSchema(ma.Schema):
    """
    Output schema for a function call
    """

    name = ma.fields.Str(required=True, default="function_call")
    func = ma.fields.Dict()
    func_type = ma.fields.Dict()
    arguments = ma.fields.Dict(
        keys=ma.fields.Str(), values=ma.fields.Field(), missing=dict
    )


@dc.dataclass(frozen=True)
class FunctionCallSerializer(ObjectImplementationSerializer):
    """
    Serializer for a function call
    """

    function_encoder: encoders.Encoder
    function_type_serializer: ts.TypeSerializer

    def serialize(self, implementation: impl.ObjectImplementation) -> Any:
        encoded_func = self.function_encoder.encode(implementation.func)
        func_type = self.function_type_serializer.serialize(implementation.func.type)
        arguments = {key: obj._impl.id for key, obj in implementation.arguments.items()}

        out = {"func": encoded_func, "func_type": func_type, "arguments": arguments}
        return FunctionCallSchema().dump(out)

    def deserialize(
        self, data: Any, session: session.Session, objects: Mapping[Any, Object]
    ) -> impl.ObjectImplementation:

        loaded_data = FunctionCallSchema().load(data)
        loaded_func = self.function_encoder.decode(loaded_data["func"])
        loaded_kwargs = {
            key: objects[obj_id] for key, obj_id in loaded_data["arguments"].items()
        }
        return impl.FunctionCall(loaded_func, loaded_kwargs)

    @classmethod
    @st.hookimpl
    def get_impl_serializer(
        cls, impl: impl.ObjectImplementation, type: types.Type, registry: "Registry"
    ) -> ObjectImplementationSerializer:

        if not isinstance(impl, impl_mod.FunctionCall):
            return None
        func_encoder = registry.get_encoder(impl.func.type)
        func_type_ser = registry.get_type_serializer(impl.func.type)
        return cls(func_encoder, func_type_ser)

    @classmethod
    @st.hookimpl
    def get_impl_serializer_from_data(
        cls, data: Any, registry: "Registry"
    ) -> ObjectImplementationSerializer:
        if not isinstance(data, dict) or data.get("name") != "function_call":
            return None

        func_type_ser = registry.get_type_serializer_from_data(data["func_type"])
        func_type = func_type_ser.deserialize(data["func_type"])
        func_encoder = registry.get_encoder(func_type)

        return cls(func_encoder, func_type_ser)


IMPL_SERIALIZER_CLASSES = [
    ReferenceSerializer,
    FutureSerializer,
    ExpectedValueSerializer,
    DataValueSerializer,
    DataArraySerializer,
    DataStructSerializer,
    FunctionCallSerializer,
]


def register(registry: Optional["Registry"] = None) -> None:
    """
    Register casters in the default registry
    """
    if registry is None:
        registry = st.registry

    for plugin in IMPL_SERIALIZER_CLASSES:
        registry.register(plugin)
