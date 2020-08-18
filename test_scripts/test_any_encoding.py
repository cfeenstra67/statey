import statey as st
from typing import Any


session = st.create_session()

ref = session["a"] << 'abc'

@st.function
def test_func(a: Any) -> str:
    return str(a)

ref2 = session["b"] << test_func(object())

print("FUNC", test_func)
print("CALL", test_func(1))
print("REF", ref)

print("RESOLVED", session.resolve(ref))
print("RESOLVED2", session.resolve(ref2))
