import statey as st


session = st.create_session()


@st.function
def get_1() -> int:
    return 'blah'

# a = session['0'] << 0
# b = session['1'] << get_1()

# for i in range(2, 101):
#     c = session[str(i)] << a + b
#     a, b = b, c


# print("HERE", session.resolve(b))


new_type = st.Struct[
    "a": bool,
    "b": str,
    "c": st.Struct["blah": int],
    "d": st.Struct["fuck": st.Array[int]]
]


a = session["a": new_type] << {
    "a": st.Object("blah"),
    "b": 1,
    "c": {"blah": st.Object("abc")},
    "d": {"fuck": ["abc", 2, 3]}
}

b = session["b"] << a.c.blah

# a.c.blah2

print("HERE", session.resolve(b))
