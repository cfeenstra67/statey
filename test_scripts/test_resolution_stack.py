import statey as st


session = st.create_session()


@st.function
def func1() -> int:
    raise Exception

@st.function
def func2(a: int) -> bool:
    return bool(a)

@st.function
def func3(b: bool, c: bool) -> str:
    return str(b) + str(c)


a = func2(func1())
b = func2(a)

obj = session['a'] << func3(a, b)

print("RESOLVED", session.resolve(obj))
