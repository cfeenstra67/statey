import operator

import statey as st

# st.hooks.register_default_plugins()

obj = st.Object(1)

print("OBJ", obj)

s = st.struct[
	"a": 1,
	"B": 2,
	"c": False
]

s2 = st.struct[
	"blah1": s.a + 2,
	"blah2": s.c
]

s3 = st.Object({
	"blah1": s.a + 1,
	"blah2": s.c
}, s2._type)

print("STRUCT", s)

session = st.create_session()

print("RESOLVED", session.resolve(s))

func = st.F[int](operator.add)

func_obj = func(obj, 4)

print("FUNC", func)
print("CALL", func_obj)

print("RESOLVED2", session.resolve(func_obj))

print("RESOLVED3", session.resolve(s.c))

# print("METHOD", s.test)

# print("METHOD2", s.test(1))

# print("METHOD2 RESOLVED", session.resolve(s.test(1)))

# print("METHOD2 RESOLVED2", session.resolve(s.test(0)))

print("S2", s2)

print("HERE WE GO", session.resolve(s2))

print("HERE WE GO", session.resolve(s3))



obj1 = st.Object(123)

print("ADD", obj1 + 1)
