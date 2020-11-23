import pytest
import statey as st
from statey.syms import casters


RAISES = object()


@pytest.mark.parametrize('from_type, to_type, check', [
    pytest.param(int, int, lambda x: isinstance(x, casters.ForceCaster), id='int'),
    pytest.param(~st.Integer, int, RAISES, id='int_from_nullable'),
    pytest.param(int, ~st.Integer, lambda x: isinstance(x, casters.ForceCaster), id='int_to_nullable'),
    pytest.param(~st.Integer, ~st.Integer, lambda x: isinstance(x, casters.ForceCaster), id='int_nullable_to_nullable'),
    pytest.param(str, str, lambda x: isinstance(x, casters.ForceCaster), id='str'),
    pytest.param(~st.String, str, RAISES, id='str_from_nullable'),
    pytest.param(str, ~st.String, lambda x: isinstance(x, casters.ForceCaster), id='str_to_nullable'),
    pytest.param(~st.String, ~st.String, lambda x: isinstance(x, casters.ForceCaster), id='str_nullable_to_nullable'),
    pytest.param(bool, bool, lambda x: isinstance(x, casters.ForceCaster), id='bool'),
    pytest.param(~st.Boolean, bool, RAISES, id='bool_from_nullable'),
    pytest.param(bool, ~st.Boolean, lambda x: isinstance(x, casters.ForceCaster), id='bool_to_nullable'),
    pytest.param(~st.Boolean, ~st.Boolean, lambda x: isinstance(x, casters.ForceCaster), id='bool_nullable_to_nullable'),
    pytest.param(float, float, lambda x: isinstance(x, casters.ForceCaster), id='float'),
    pytest.param(~st.Float, float, RAISES, id='float_from_nullable'),
    pytest.param(float, ~st.Float, lambda x: isinstance(x, casters.ForceCaster), id='float_to_nullable'),
    pytest.param(~st.Float, ~st.Float, lambda x: isinstance(x, casters.ForceCaster), id='float_nullable_to_nullable'),
    pytest.param(st.Array[int], st.Array[int], lambda x: isinstance(x, casters.ForceCaster), id='array'),
    pytest.param(st.Array[int], ~st.Array[int], lambda x: isinstance(x, casters.ForceCaster), id='array_to_nullable'),
    pytest.param(st.Array[int], st.Array[~st.Integer], lambda x: isinstance(x, casters.ForceCaster), id='array_null_element'),
    pytest.param(st.Struct["a": int], st.Struct["a": int], lambda x: isinstance(x, casters.ForceCaster), id='struct'),
    pytest.param(st.Struct["a": int], ~st.Struct["a": int], lambda x: isinstance(x, casters.ForceCaster), id='struct_to_nullable'),
    pytest.param(
        st.Struct["a": int, "b": bool],
        st.Struct["a": ~st.Integer, "b": bool],
        lambda x: isinstance(x, casters.ForceCaster),
        id='struct_nullable_field'
    ),
    pytest.param(st.Map[int, int], st.Map[int, int], lambda x: isinstance(x, casters.ForceCaster), id='map'),
    pytest.param(st.Map[int, int], ~st.Map[int, int], lambda x: isinstance(x, casters.ForceCaster), id='map_to_nullable'),
    pytest.param(st.Map[int, int], st.Map[~st.Integer, ~st.Integer], lambda x: isinstance(x, casters.ForceCaster), id='map_nullable_values'),
    pytest.param(float, int, lambda x: isinstance(x, casters.ForceCaster), id='float_to_int'),
    pytest.param(int, ~st.Float, lambda x: isinstance(x, casters.ForceCaster), id='int_to_nullable_float'),
])
def test_get_caster(from_type, to_type, check, registry):
    from_type = registry.get_type(from_type)
    to_type = registry.get_type(to_type)

    if check is RAISES:
        with pytest.raises(st.exc.NoCasterFound):
            registry.get_caster(from_type, to_type)
    else:
        caster = registry.get_caster(from_type, to_type)
        assert check(caster)
