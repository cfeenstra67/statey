import pytest
import statey as st


@pytest.mark.parametrize(
    "type, serialized",
    [
        pytest.param(int, {"type": "integer"}, id="int"),
        pytest.param(
            ~st.Integer, {"type": "integer", "nullable": True}, id="int_nullable"
        ),
        pytest.param(float, {"type": "number"}, id="float"),
        pytest.param(
            ~st.Float, {"type": "number", "nullable": True}, id="float_nullable"
        ),
        pytest.param(bool, {"type": "boolean"}, id="bool"),
        pytest.param(
            ~st.Boolean, {"type": "boolean", "nullable": True}, id="bool_nullable"
        ),
        pytest.param(str, {"type": "string"}, id="str"),
        pytest.param(
            ~st.String, {"type": "string", "nullable": True}, id="str_nullable"
        ),
        pytest.param(
            st.Array[str], {"type": "array", "items": {"type": "string"}}, id="array"
        ),
        pytest.param(
            ~st.Array[str],
            {"type": "array", "items": {"type": "string"}, "nullable": True},
            id="array_nullable",
        ),
        pytest.param(
            st.Struct["field_1" : st.Array[int], "field_2" : ~st.Boolean],
            {
                "type": "object",
                "properties": {
                    "field_1": {"type": "array", "items": {"type": "integer"}},
                    "field_2": {"type": "boolean"},
                },
                "required": ["field_1"],
            },
            id="struct",
        ),
        pytest.param(
            ~st.Struct["field_1" : st.Array[int], "field_2" : ~st.Boolean],
            {
                "type": "object",
                "properties": {
                    "field_1": {"type": "array", "items": {"type": "integer"}},
                    "field_2": {"type": "boolean"},
                },
                "required": ["field_1"],
                "nullable": True,
            },
            id="struct_nullable",
        ),
        pytest.param(
            st.Map[str, st.Struct["a":int]],
            {
                "type": "object",
                "additionalProperties": {
                    "type": "object",
                    "properties": {"a": {"type": "integer"}},
                    "required": ["a"],
                },
            },
            id="map",
        ),
        pytest.param(
            st.Map[int, st.Struct["a":int]],
            {
                "type": "object",
                "keys": {"type": "integer"},
                "additionalProperties": {
                    "type": "object",
                    "properties": {"a": {"type": "integer"}},
                    "required": ["a"],
                },
            },
            id="map_non_str_keys",
        ),
        pytest.param(
            ~st.Map[str, st.Struct["a":int]],
            {
                "type": "object",
                "additionalProperties": {
                    "type": "object",
                    "properties": {"a": {"type": "integer"}},
                    "required": ["a"],
                },
                "nullable": True,
            },
            id="map_nullable",
        ),
        pytest.param(st.TypeType(), {"type": "type"}, id="type_non_nullable"),
        pytest.param(
            ~st.TypeType(), {"type": "type", "nullable": True}, id="type_nullable"
        ),
    ],
)
def test_serialize_type(type, serialized, registry):
    type = registry.get_type(type)
    type_serializer = registry.get_type_serializer(type)

    result = type_serializer.serialize(type)
    assert result == serialized

    result_serializer = registry.get_type_serializer_from_data(result)
    result_type = result_serializer.deserialize(result)

    assert result_type == type
