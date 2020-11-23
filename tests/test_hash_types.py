import copy

import pytest
import statey as st


@pytest.mark.parametrize('type, success', [
    pytest.param(int, True, id='int'),
    pytest.param(st.Integer(default=0), True, id='int_with_meta'),
    pytest.param(st.Integer(default={}), True, id='int_with_non_hashable_meta'),
    pytest.param(str, True, id='str'),
    pytest.param(bool, True, id='bool'),
    pytest.param(float, True, id='float'),
    pytest.param(st.Array[str], True, id='array'),
    pytest.param(st.Struct["a": int], True, id='struct'),
    pytest.param(st.Map[str, st.Array[int]], True, id='map')
])
def test_hash_type(type, success, registry):
    type = registry.get_type(type)
    obj = {}
    if success:
        obj[type] = 1
        type_copy = copy.deepcopy(type)
        assert obj[type_copy] == 1
    else:
        with pytest.raises(TypeError):
            obj[type] = 1
