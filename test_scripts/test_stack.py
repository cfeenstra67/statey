import statey as st

session = st.create_session()

@st.function
def raise_error(x: int) -> int:
	return x / 0

obj = st.Object(1)

# obj.a
print("RESOLVED2", session.resolve(st.Object(st.Unknown(return_type=obj._type))))
print("RESOLVED", session.resolve(raise_error(obj)))
