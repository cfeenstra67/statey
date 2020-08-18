import json

import statey as st


session = st.create_session()
ser = st.registry.get_session_serializer(session)

ref1 = session['a'] << st.Object(1)

ref2 = session['b'] << ref1 + 2

serialized = ser.serialize(session)
print("SERIALIZED", json.dumps(serialized, indent=2))

deser = ser.deserialize(serialized)
print("DESERIALIZED", deser)

deser.set_data('a', st.Object(3))
print("HERE", deser.resolve(ref2))
print("HERE2", session.resolve(ref2))
