from datetime import date, datetime
from typing import Sequence, Optional, Any, Dict

import pytest
import statey as st


@pytest.mark.parametrize(
    "input, result",
    [
        pytest.param(int, st.Integer, id="int"),
        pytest.param(st.Integer, st.Integer, id="int_identity"),
        pytest.param(str, st.String, id="str"),
        pytest.param(st.String, st.String, id="str_identity"),
        pytest.param(bool, st.Boolean, id="bool"),
        pytest.param(st.Boolean, st.Boolean, id="bool_identity"),
        pytest.param(float, st.Float, id="float"),
        pytest.param(st.Float, st.Float, id="float_identity"),
        pytest.param(Optional[int], ~st.Integer, id="optional_int"),
        pytest.param(Sequence[str], st.Array[str], id="sequence_str"),
        pytest.param(
            Optional[Sequence[bool]], ~st.Array[bool], id="optional_sequence_bool"
        ),
        pytest.param(Any, st.Any, id="any"),
        pytest.param(
            Dict[str, Sequence[int]], st.Map[str, st.Array[int]], id="dict_to_map"
        ),
        pytest.param(date, st.Date, id="date"),
        pytest.param(datetime, st.DateTime, id="datetime"),
    ],
)
def test_get_type(input, result, registry):
    assert registry.get_type(input) == result
