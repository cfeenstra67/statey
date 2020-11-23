import statey as st


obj = st.Object[~st.String](None)

obj2 = st.Object[~st.String]('abc,de,fg')

sess = st.create_session()

print("OBJ", obj)

call = obj.split(',')

print("???", obj.split)

print("CALL", call)

call2 = obj2.split(',')

print("RESOLVED", sess.resolve(call))

print("CALL2", call2)

print("RESOLVED", sess.resolve(call2))
