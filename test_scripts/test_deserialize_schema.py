import statey as st

import json

import jsonschema

import copy


RESOLVER_STORE = {
    'pulumi.json': {
        'Any': {'type': 'any'},
        'Asset': {'type': 'string'}
    }
}


def fix_broken_refs(doc):
    """
    In Pulumi schemas some keys contain slashes; this messes w/ jsonschema.RefResolver
    """
    keys = ["types", "resources", "functions"]

    out = {}

    for key in keys:

        doc_types = doc[key]

        for key, val in list(doc_types.items()):
            comps = key.split('/')
            current = out
            for comp in comps[:-1]:
                current = current.setdefault(comp, {})

            current[comps[-1]] = val

    return out



def resolve_refs(schema, doc):
    """

    """
    if '$ref' in schema:
        resolver = jsonschema.RefResolver(
            '', doc, RESOLVER_STORE
        )
        try:
            resolved = resolver.resolve(schema['$ref'])[1]
        except jsonschema.exceptions.RefResolutionError:
            second = {'type': 'string'}
            ct2 = 0
        else:
            second, ct2 = resolve_refs(resolved, doc)
        return second, ct2 + 1

    resolved = 0
    schema_type = schema['type']
    if schema_type == 'object':
        schema = schema.copy()

        if 'properties' in schema:
            props = {}

            for key, val in schema.get('properties', {}).items():
                out, ct = resolve_refs(val, doc)
                props[key] = out
                resolved += ct
            schema = dict(schema, properties=props)

        if 'additionalProperties' in schema:
            schema['additionalProperties'], ct = resolve_refs(schema['additionalProperties'], doc)
            resolved += ct

        if resolved > 0:
            second, ct2 = resolve_refs(schema, doc)
            return second, resolved + ct2
        return schema, resolved

    if schema_type == 'array':
        out, ct = resolve_refs(schema['items'], doc)
        resolved += ct
        out_schema = dict(schema, items=out)
        if resolved > 0:
            return resolve_refs(out_schema, doc)
        return out_schema, resolved

    return schema, resolved


with open('schema_fmt.json') as f:
    data = json.load(f)


resources = data["resources"].copy()

data = fix_broken_refs(data)


out = {}

for key, schema in resources.items():
    input_type = {
        'type': 'object',
        'properties': schema.get('inputProperties', {}),
        'required': schema.get('requiredInputs', [])
    }
    input_type, _ = resolve_refs(input_type, data)
    input_ser = st.registry.get_type_serializer_from_data(input_type)
    input_type_obj = input_ser.deserialize(input_type)

    output_type = {
        'type': 'object',
        'properties': schema.get('properties', {}),
        'required': schema.get('required', [])
    }
    output_type, _ = resolve_refs(output_type, data)
    output_ser = st.registry.get_type_serializer_from_data(output_type)
    output_type_obj = output_ser.deserialize(output_type)

    out[key] = (input_ser, output_ser)

print("HERE")
