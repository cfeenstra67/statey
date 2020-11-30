import dataclasses as dc
import os
import sys
from importlib.machinery import ModuleSpec

import jsonschema
import statey as st
import pylumi
from typing import Sequence, Dict, Any, Optional

from statey.ext.pulumi import __file__ as root_file
from statey.ext.pulumi.constants import RESOLVER_STORE, PULUMI_NS


ROOT_DIR = os.path.abspath(os.path.dirname(root_file))


@dc.dataclass(frozen=True)
class PulumiResourceSchema:
    """
    Describes
    """

    description: str
    input_type: st.Type
    output_type: st.Type


@dc.dataclass(frozen=True)
class PulumiProviderSchema:
    """
    Object representing a pulumi provider schema response

    (not all information consumed)
    """

    name: str
    version: str
    description: str
    resources: Dict[str, PulumiResourceSchema]


class PulumiProviderSchemaParser:
    """
    Parses a pulumi provider get_schema() response into a PulumiProviderSchema object
    """

    def _fix_broken_refs(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """
        In Pulumi schemas some keys contain slashes; this messes w/ jsonschema.RefResolver
        """
        keys = ["types", "resources", "functions"]

        out = {}

        for key in keys:

            doc_types = doc[key]
            this_out = out.setdefault(key, {})

            for key, val in list(doc_types.items()):
                comps = key.split("/")
                current = this_out
                for comp in comps[:-1]:
                    current = current.setdefault(comp, {})

                current[comps[-1]] = val

        return out

    def _resolve_refs(
        self, schema: Dict[str, Any], doc: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Resolve any references in the given pulumi provider schema
        """
        if "$ref" in schema:
            resolver = jsonschema.RefResolver("", doc, RESOLVER_STORE)
            try:
                resolved = resolver.resolve(schema["$ref"])[1]
            except jsonschema.exceptions.RefResolutionError:
                second = {"type": "string"}
                ct2 = 0
            else:
                second, ct2 = self._resolve_refs(resolved, doc)
            return second, ct2 + 1

        resolved = 0
        schema_type = schema.get("type")
        if schema_type == "object":
            schema = schema.copy()

            if "properties" in schema:
                props = {}

                for key, val in schema.get("properties", {}).items():
                    out, ct = self._resolve_refs(val, doc)
                    props[key] = out
                    resolved += ct
                schema = dict(schema, properties=props)

            if "additionalProperties" in schema:
                schema["additionalProperties"], ct = self._resolve_refs(
                    schema["additionalProperties"], doc
                )
                resolved += ct

            if resolved > 0:
                second, ct2 = self._resolve_refs(schema, doc)
                return second, resolved + ct2
            return schema, resolved

        if schema_type == "array":
            out, ct = self._resolve_refs(schema["items"], doc)
            resolved += ct
            out_schema = dict(schema, items=out)
            if resolved > 0:
                return self._resolve_refs(out_schema, doc)
            return out_schema, resolved

        if schema_type is not None:
            return schema, resolved

        one_of = schema.get("oneOf")
        if one_of is None:
            return schema, resolved

        outs = []
        for item in one_of:
            out, ct = self._resolve_refs(item, doc)
            if out not in outs:
                outs.append(out)
                resolved += ct

        out_schema = schema.copy()
        del out_schema["oneOf"]
        out_schema.update(outs[-1])
        return out_schema, resolved

    def parse(
        self, data: Dict[str, Any], registry: Optional[st.Registry] = None
    ) -> PulumiProviderSchema:
        """
        Parse a dictionary response into a PulumiProviderSchema
        """
        if registry is None:
            registry = st.registry

        resources = data["resources"].copy()

        fixed_data = self._fix_broken_refs(data)

        out = {}
        for key, schema in resources.items():
            input_type = {
                "type": "object",
                "properties": schema.get("inputProperties", {}),
                "required": schema.get("requiredInputs", []),
            }
            input_type, _ = self._resolve_refs(input_type, fixed_data)
            input_ser = registry.get_type_serializer_from_data(input_type)
            input_type_obj = input_ser.deserialize(input_type)

            output_type = {
                "type": "object",
                "properties": schema.get("properties", {}),
                "required": schema.get("required", []),
            }
            output_type, _ = self._resolve_refs(output_type, fixed_data)
            output_ser = registry.get_type_serializer_from_data(output_type)
            output_type_obj = output_ser.deserialize(output_type)

            out[key] = PulumiResourceSchema(
                description=schema.get("description"),
                input_type=input_type_obj,
                output_type=output_type_obj,
            )

        return PulumiProviderSchema(
            name=data.get("name"),
            version=data.get("version"),
            description=data.get("description"),
            resources=out,
        )


def object_to_pulumi_json(obj: st.Object, session: st.Session) -> str:
    """
    Encode an object as JSON to transmit to pulumi
    """

    def convert_object(obj):
        typ = obj._type
        unknown_value = pylumi.UnknownValue.NULL_
        if isinstance(typ, st.StringType):
            unknown_value = pylumi.UnknownValue.STRING
        elif isinstance(typ, st.NumberType):
            unknown_value = pylumi.UnknownValue.NUMBER
        elif isinstance(typ, st.BooleanType):
            unknown_value = pylumi.UnknownValue.BOOL
        elif isinstance(typ, st.ArrayType):
            unknown_value = pylumi.UnknownValue.ARRAY
        elif isinstance(typ, (st.StructType, st.MapType)):
            unknown_value = pylumi.UnknownValue.OBJECT

        return unknown_value

    semantics = obj._registry.get_semantics(obj._type)
    data = session.resolve(obj, allow_unknowns=True)
    expanded = semantics.expand(data)
    with_encoded_unknowns = semantics.map_objects(convert_object, expanded)

    return with_encoded_unknowns


def parse_provider_schema_response(
    data: Dict[str, Any], registry: Optional[st.Registry] = None
) -> PulumiProviderSchema:
    """
    Parse a JSON schema response into a PulumiProviderSchema object
    """
    return PulumiProviderSchemaParser().parse(data, registry)


class ProviderFinder:
    """
    Module meta finder to allow loading providers via the import system
    """

    prefix = "statey.ext.pulumi.providers"

    def find_spec(self, fullname, path, target=None) -> ModuleSpec:
        from statey.ext.pulumi import provider_api

        if not path:
            return None
        if os.path.abspath(path[0]) != ROOT_DIR:
            return None
        if not fullname.startswith(self.prefix):
            return None
        name = fullname[len(self.prefix) + 1 :]
        if not name:
            return ModuleSpec(fullname, ProvidersLoader(), is_package=False)

        head, *path = name.split(".", 1)
        if path:
            return None

        loader = ProviderLoader(name)
        return ModuleSpec(fullname, loader, is_package=False)


class ProviderLoader:
    """
    Module loader to load a provider
    """

    def __init__(self, name: str) -> None:
        self.name = name

    def load_module(self, spec):
        from statey.ext.pulumi import provider_api

        api = sys.modules[spec] = provider_api.provider_api(self.name)
        api.__file__ = provider_api.__file__
        api.__name__ = spec
        api.__loader__ = self
        api.__package__ = spec.rsplit(".", 1)[0]
        return api


class ProvidersLoader:
    """
    Module loader to load the `providers` module
    """

    def load_module(self, spec):
        from statey.ext.pulumi import provider_api

        api = sys.modules[spec] = provider_api.ProvidersAPI()
        api.__file__ = provider_api.__file__
        api.__name__ = spec
        api.__loader__ = self
        api.__package__ = spec.rsplit(".", 1)[0]
        return api


FINDER = ProviderFinder()


def register_meta_finder() -> None:
    """
    Register a ProviderFinder in the import system
    """
    if FINDER not in sys.meta_path:
        sys.meta_path.append(FINDER)


def unregister_meta_finder() -> None:
    """
    Unregster the ProviderFinder from the meta path, if it is registered.
    """
    if FINDER in sys.meta_path:
        sys.meta_path.remove(FINDER)


class AwsEnvironmentHandler:
    """
    Plugins to allow for easy environment-based setup of the pulumi AWS provider
    """

    @staticmethod
    @st.hookimpl(hookwrapper=True)
    def get_provider_config(name, params, registry):
        if name == PULUMI_NS + "/aws":
            default_region = os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION"))
            if default_region is not None:
                params.setdefault("region", default_region)

        outcome = yield


PLUGINS = [AwsEnvironmentHandler]


def register(registry: Optional[st.Registry] = None) -> None:
    """
    Register all plugins for this module
    """
    if registry is None:
        registry = st.registry

    register_meta_finder()

    for plugin in PLUGINS:
        registry.register(plugin)
