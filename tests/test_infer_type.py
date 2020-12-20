from datetime import date, datetime

import pytest
import statey as st


@pytest.mark.parametrize(
    "input, result",
    [
        pytest.param(1, st.Integer, id="int"),
        pytest.param("blah", st.String, id="str"),
        pytest.param(False, st.Boolean, id="bool"),
        pytest.param(1.23, st.Float, id="float"),
        pytest.param([1, 2, 3], st.Array[int], id="array_int"),
        pytest.param([1, "2", False], st.Array[st.Any], id="array_any"),
        pytest.param(["abc", "def", "ghi"], st.Array[str], id="array_str"),
        pytest.param(
            {"a": {"b": "c"}}, st.Struct["a" : st.Struct["b":str]], id="struct_dict"
        ),
        pytest.param(date(2020, 1, 1), st.Date, id="date"),
        pytest.param(datetime(2020, 11, 1), st.DateTime, id="datetime"),
    ],
)
def test_infer_type(input, result, registry):
    assert registry.infer_type(input) == result
