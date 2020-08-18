import json

import statey as st

def dump(x):
    return json.dumps(x, indent=2, sort_keys=True)

session = st.create_session()

obj = st.Object({'a': 1, 'b': 2})

b_attr = obj.b

obj2 = st.Object({'a': b_attr, 'c': 'abc'})

ser = st.registry.get_object_serializer(obj)

print("DESERIALIZED", obj)

serialized = ser.serialize(obj)
print("SERIALIZED", dump(serialized))

print("AGAIN", ser.deserialize(serialized, session, {}))

print("DESERIALIZED2", obj2)

ser2 = st.registry.get_object_serializer(obj2)
serialized2 = ser2.serialize(obj2)

print("SERIALIZED2", dump(serialized2))

print("AGAIN2", ser2.deserialize(serialized2, session, {b_attr._impl.id: b_attr}))
