import json

import jsonschema


with open('schema_fmt.json') as f:
    doc = json.load(f)


def resolve_refs(schema, doc):
    resolver = jsonschema.RefResolver('#', doc)
    if '$ref' in schema:
        return resolver.resolve(schema['$ref'])[1]

    schema_type = schema['type']
    if schema_type == 'object':
        props = {}
        for key, val in schema['properties'].items():
            props[key] = resolve_refs(val, doc)
        schema = dict(schema, properties=props)
        if 'additionalProperties' in schema:
            schema['additionalProperties'] = resolve_refs(schema['additionalProperties'], doc)
        return schema

    if schema_type == 'array':
        return dict(schema, items=resolve_refs(schema['items'], doc))

    return schema


def fix_broken_refs(doc):

    keys = ["types", "resources", "functions"]

    for key in keys:

        doc_types = doc[key]

        for key, val in list(doc_types.items()):
            del doc_types[key]
            comps = key.split('/')
            current = doc_types
            for comp in comps[:-1]:
                current = current.setdefault(comp, {})

            current[comps[-1]] = val
