import statey as st

session = st.create_session()

typ = st.Struct[
    "a": st.Integer,
    "b": st.Struct[
        "c": st.Struct[
            "d": st.Boolean,
            "blah": st.String
        ],
        "mr_magoo": st.Array[st.String]
    ]
]


obj = st.Object[typ]({
    'a': 1,
    'b': {
        'c': {
            'd': False,
            'blah': 'Fuck'
        },
        'mr_magoo': ['blah']
    }
})


print("TYP", typ)
print("OBJ", obj)
print("RESOLVED", session.resolve(obj))

print("INTERPOLATED1", st.struct_interpolate(typ, {'b.c.d': st.Integer}))

obj2 = st.struct_interpolate(obj, {'b.c.blah': 'Hello?', 'a': 2})
print("INTERPOLATED2", obj2)
print("INTERPOLATED2_RESOLVED", session.resolve(obj2))

obj3 = st.struct_interpolate(obj, {'b.c.blah': [1]}, keep_type=False)
print("INTERPOLATED3", obj3)
print("INTERPOLATED3_RESOLVED", session.resolve(obj3))


GraphType = st.Struct[
    "nodes": st.Struct,
    "edges": st.Array[st.Struct[
        "left": st.String,
        "right": st.String,
        "data": st.Map[st.String, st.String]
    ]]
]






