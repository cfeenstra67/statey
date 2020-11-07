import pytest
import statey as st


@pytest.mark.parametrize('obj, typ, output', [
    pytest.param(
        st.Object(1), st.Integer, 1,
        id='literal_int_1'
    ),
    pytest.param(
        st.Object(True), st.Boolean, True,
        id='literal_bool_1'
    ),
    pytest.param(
        st.Object[str]('92'), st.String, '92',
        id='literal_str_1'
    ),
    pytest.param(
        st.Object[int]('123'), st.Integer, 123,
        id='literal_int_2'
    ),
    pytest.param(
        st.Object({'a': 1, 'b': {'c': 'd'}}),
        st.Struct["a": int, "b": st.Struct["c": str]],
        {'a': 1, 'b': {'c': 'd'}},
        id='literal_struct_1'
    ),
    pytest.param(
        st.Object([1, 2, 3]),
        st.Array[int],
        [1, 2, 3],
        id='literal_array_1'
    ),
    pytest.param(
        st.Object(['1', '2', '3']),
        st.Array[str],
        ['1', '2', '3'],
        id='literal_array_2'
    ),
    pytest.param(
        st.Object(['1', 2, True]),
        st.Array[st.Any],
        ['1', 2, True],
        id='literal_array_3'
    ),
    pytest.param(
        st.Object[st.Map[str, int]]({'a': '2', 'b': '3'}),
        st.Map[str, int],
        {'a': 2, 'b': 3},
        id='literal_map_1'
    ),
    pytest.param(
        st.Object(1) + st.Object(21342), st.Integer, 21343,
        id='sum_1'
    ),
    pytest.param(
        st.Object(1) + 21342, st.Integer, 21343,
        id='sum_2'
    ),
    pytest.param(
        st.Object(123) * 2, st.Integer, 246,
        id='multiplication_1'
    ),
    pytest.param(
        st.Object(123.) / 2, st.Float, 61.5,
        id='truediv_1'
    ),
    pytest.param(
        st.Object(123.) / 2., st.Float, 61.5,
        id='truediv_2'
    ),
    pytest.param(
        st.Object(123) / 2, st.Integer, 61,
        id='truediv_3'
    ),
    pytest.param(
        st.Object(123) / 2., st.Integer, 61,
        id='truediv_4'
    ),
    pytest.param(
        st.Object(123) // 2, st.Integer, 61,
        id='floordiv_1'
    ),
    pytest.param(
        st.Object(123924) - 12, st.Integer, 123912,
        id='sub_1'
    ),
    pytest.param(
        st.Object(3) == 3, st.Boolean, True,
        id='eq_1'
    ),
    pytest.param(
        st.Object(234) != 234, st.Boolean, False,
        id='ne_1'
    ),
    pytest.param(
        st.Object({'a': {'b': 'c'}}).a.b, st.String, 'c',
        id='struct_attr_1'
    ),
    pytest.param(
        st.Object({'a': {'b': 'c'}}).a, st.Struct["b": st.String], {'b': 'c'},
        id='struct_attr_2'
    ),
    pytest.param(
        st.Object(range(4)), st.Array[int], [0, 1, 2, 3],
        id='range_1'
    ),
    pytest.param(
        st.Object(range(2, 120))[40], st.Integer, 42,
        id='array_index_1'
    ),
    pytest.param(
        st.Object(range(12, 36))[10:-10], st.Array[int], [22, 23, 24, 25],
        id='array_slice_1'
    ),
    pytest.param(
        st.Object([{'a': 1, 'b': 2}]).a, st.Array[int], [1],
        id='array_struct_attr_1'
    )
])
def test_resolve_object(session, obj, typ, output):
    assert obj._type == typ
    assert session.resolve(obj) == output
