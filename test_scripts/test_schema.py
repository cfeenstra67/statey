import statey as st

def validator(x):
    if not x:
        raise st.exc.InputValidationError('bad value')

typ = st.S.Struct[
    "A": st.registry.get_type(str, meta={'default': 'abc'}),
    "B": st.registry.get_type(bool, meta={'validator': validator, 'default': False})
].t

encoder = st.registry.get_encoder(typ)

inp = {'A': 'blah', 'B': True}

print("ENCODED1", encoder.encode({}))
print("ENCODED2", encoder.encode(inp))
try:
    print("ENCODED3", encoder.encode({'A': 'fuck', 'B': False}))
except st.exc.InputValidationError:
    print("Raised for bad value")


# encoder = st.registry.get_encoder(schema.output_type)

# inp = {'A': 'blah', "B": True}

# print("ENCODED1", encoder.encode(inp))

# encoder.field_encoders['A'].meta['default'] = 'a'
# encoder.field_encoders['B'].meta['default'] = False

# print("ENCODED2", encoder.encode({}))
# print("ENCODED3", encoder.encode(inp))

# def validator(abc):
#     if not abc:
#         raise st.exc.InputValidationError("Bad value")

# encoder.field_encoders['B'].meta['validator'] = validator

# print("ENCODED4", encoder.encode({}))
# print("ENCODED5", encoder.encode(inp))
# try:
#     print("ENCODED6", encoder.encode(dict(inp, B=st.Object(False))))
# except st.exc.InputValidationError:
#     print("Raised w/ bad value")
