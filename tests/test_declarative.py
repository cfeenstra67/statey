import pytest
import statey as st


@st.declarative
def declarative_1(session):
    a = 1


@st.declarative
def declarative_2(session):
    a = 1
    b = a + 1


@st.declarative
def declarative_struct_1(session):
    a = {'a': 1, 'b': 2}
    b = a.a + a.b
    c = {'d': a, 'e': b}
    d = c.d.a + c.d.b + c.e


@pytest.mark.parametrize('func, result_key, result', [
    pytest.param(
        declarative_1, 'a', 1,
        id='declarative_1'
    ),
    pytest.param(
        declarative_2, 'b', 2,
        id='declarative_2'
    ),
    pytest.param(
        declarative_struct_1, 'd', 6,
        id='declarative_struct_1'
    )
])
def test_declarative(session, func, result_key, result):
    func(session)
    assert session.resolve(session.ns.ref(result_key)) == result
