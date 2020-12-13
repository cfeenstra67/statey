from typing import Optional

import pytest
import statey as st
from statey.syms import encoders


@pytest.mark.parametrize(
    "type, check",
    [
        pytest.param(int, lambda x: isinstance(x, encoders.IntegerEncoder), id="int"),
        pytest.param(str, lambda x: isinstance(x, encoders.StringEncoder), id="str"),
        pytest.param(bool, lambda x: isinstance(x, encoders.BooleanEncoder), id="bool"),
        pytest.param(float, lambda x: isinstance(x, encoders.FloatEncoder), id="float"),
        pytest.param(
            st.Struct["a" : st.Array[int], "b" : st.Boolean],
            lambda x: (
                isinstance(x, encoders.StructEncoder)
                and set(x.field_encoders) == {"a", "b"}
                and isinstance(x.field_encoders["a"], encoders.ArrayEncoder)
                and isinstance(x.field_encoders["b"], encoders.BooleanEncoder)
            ),
            id="struct",
        ),
        pytest.param(
            st.Array[str],
            lambda x: isinstance(x, encoders.ArrayEncoder)
            and isinstance(x.element_encoder, encoders.StringEncoder),
            id="array",
        ),
        pytest.param(
            st.Map[str, st.Struct["a" : st.String]],
            lambda x: (
                isinstance(x, encoders.MapEncoder)
                and isinstance(x.key_encoder, encoders.StringEncoder)
                and isinstance(x.value_encoder, encoders.StructEncoder)
            ),
            id="map",
        ),
        pytest.param(
            st.TypeType(), lambda x: isinstance(x, encoders.TypeEncoder), id="type"
        ),
    ],
)
def test_get_encoder(type, check, registry):
    st_type = registry.get_type(type)
    encoder = registry.get_encoder(st_type)
    assert check(encoder)


RAISES = object()


@pytest.mark.parametrize(
    "type, data, result",
    [
        pytest.param(int, 1, 1, id="int"),
        pytest.param(int, "1", 1, id="int_from_str"),
        pytest.param(int, None, RAISES, id="int_null_fail"),
        pytest.param(~st.Integer, None, None, id="int_null"),
        pytest.param(str, "abc", "abc", id="str"),
        pytest.param(str, 1, RAISES, id="str_from_int_fail"),
        pytest.param(bool, True, True, id="bool"),
        pytest.param(bool, 1, True, id="bool_from_1"),
        pytest.param(float, 1.234, 1.234, id="float"),
        pytest.param(float, "1.234", 1.234, id="float_from_str"),
        pytest.param(st.Array[str], ["a", "b", "c"], ["a", "b", "c"], id="array_str"),
        pytest.param(
            st.Struct["a":str, "b":int],
            {"a": "1", "b": "2"},
            {"a": "1", "b": 2},
            id="struct",
        ),
        pytest.param(
            st.Map[str, st.Array[int]],
            {"a": [1, "2", 3], "b": [], "c": ["53"]},
            {"a": [1, 2, 3], "b": [], "c": [53]},
            id="map",
        ),
        pytest.param(
            st.Struct["nullable" : ~st.String, "non_nullable":bool],
            {"non_nullable": False},
            {"nullable": None, "non_nullable": False},
            id="struct_nullable_field",
        ),
        pytest.param(
            st.Struct[
                "nullable" : ~st.Struct[
                    "sub_1":str, "sub_2" : ~st.Integer, "sub_3" : st.Array[str]
                ],
                "non_nullable":bool,
            ],
            {"non_nullable": False},
            {"nullable": None, "non_nullable": False},
            id="struct_nullable_nested",
        ),
        pytest.param(
            st.Struct[
                "nullable" : ~st.Struct[
                    "sub_1":str, "sub_2" : ~st.Integer, "sub_3" : st.Array[str]
                ],
                "non_nullable":bool,
            ],
            {
                "non_nullable": False,
                "nullable": st.Object[
                    ~st.Struct[
                        "sub_1":str, "sub_2" : ~st.Integer, "sub_3" : st.Array[str]
                    ]
                ](None),
            },
            {"nullable": None, "non_nullable": False},
            id="struct_nullable_nested_obj",
        ),
        pytest.param(st.TypeType(), st.Integer, {"type": "integer"}, id="type_int"),
        pytest.param(
            st.TypeType(),
            st.Struct["a":int, "b":bool],
            {
                "type": "object",
                "properties": {"a": {"type": "integer"}, "b": {"type": "boolean"}},
                "required": ["a", "b"],
            },
            id="type_struct",
        ),
    ],
)
def test_encode(type, data, result, session, registry):
    type = registry.get_type(type)
    encoder = registry.get_encoder(type)
    if result is RAISES:
        with pytest.raises(st.exc.InputValidationError):
            encoder.encode(data)
    else:
        obj = st.Object[type](data)
        resolved = session.resolve(obj, decode=False)
        assert result == resolved
