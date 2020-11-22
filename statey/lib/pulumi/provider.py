import dataclasses as dc
from typing import Sequence, Dict, Any, Optional

import jsonschema
import pylumi

import statey as st
from statey.lib.pulumi.exc import PulumiValidationError


RESOLVER_STORE = {"pulumi.json": {"Any": {"type": "any"}, "Asset": {"type": "string"}}}


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

            for key, val in list(doc_types.items()):
                comps = key.split("/")
                current = out
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
        schema_type = schema["type"]
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

        return schema, resolved

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
        unknown_value = ""
        if isinstance(typ, st.NumberType):
            unknown_value = pylumi.UNKNOWN_NUMBER_VALUE
        elif isinstance(typ, st.BooleanType):
            unknown_value = pylumi.UNKNOWN_BOOL_VALUE
        elif isinstance(typ, st.ArrayType):
            unknown_value = pylumi.UNKNOWN_ARRAY_VALUE
        elif isinstance(typ, (st.StructType, st.MapType)):
            unknown_value = pylumi.UNKNOWN_OBJECT_VALUE

        return {pylumi.UNKNOWN_KEY: unknown_value}

    semantics = obj._registry.get_semantics(obj._type)
    data = session.resolve(obj, allow_unknowns=True)
    expanded = semantics.expand(data)
    with_encoded_unknowns = semantics.map_objects(convert_object, expanded)

    return with_encoded_unknowns


class PulumiResourceMachine(st.SingleStateMachine):
    """

    """

    def make_output(
        self, data: Dict[str, Any], id: str, typ: st.Type
    ) -> Dict[str, Any]:
        """

        """
        data = dict(data, PulumiID=id)
        # Trim any extra fields
        data = {
            field.name: data[field.name] for field in typ.fields if field.name in data
        }
        encoder = st.registry.get_encoder(typ)
        return encoder.encode(data)

    def make_expected_output(
        self, data: Dict[str, Any], config: st.StateConfig, typ: st.Type
    ) -> st.Object:
        """

        """
        stripped = {key: val for key, val in data.items() if val is not None}

        drop_fields = []
        for field in typ.fields:
            if field.name not in stripped:
                drop_fields.append(field.name)

        sub_type = st.struct_drop(typ, *drop_fields)
        data_obj = st.Object(stripped, sub_type)

        unknown = st.Unknown[typ]
        config_type = config.obj._type
        config_fields = {field.name: field for field in config_type.fields}
        registry = config.obj._registry

        def get_value(key):
            this_field = typ[key]
            if key in config_fields:
                config_field = config_fields[key]

                try:
                    caster = registry.get_caster(config_field.type, this_field.type)
                except st.exc.NoCasterFound:
                    pass
                else:
                    return caster.cast(config.obj[key])

                non_nullable_config = config_field.type.with_nullable(False)
                try:
                    caster = registry.get_caster(non_nullable_config, this_field.type)
                except st.exc.NoCasterFound:
                    pass
                else:
                    return caster.cast(st.ifnull(config.obj[key], unknown[key]))

            return unknown[key]

        return st.fill(data_obj, typ, get_value)

    def clean_check_resp(self, resp: Dict[str, Any]) -> Dict[str, Any]:
        """
        Clean the check() response
        """
        resp = resp.copy()
        for key in ['__defaults']:
            resp.pop(key, None)
        return resp

    def get_action(
        self,
        current: st.StateSnapshot,
        config: st.StateConfig,
        session: st.TaskSession,
    ) -> st.ModificationAction:

        check_resp, errs = self.provider.pulumi_provider.check(
            pylumi.URN(self.resource_name),
            current.data,
            object_to_pulumi_json(config.obj, session),
        )
        check_resp = self.clean_check_resp(check_resp)
        if errs:
            raise PulumiValidationError(errs)

        current_data = current.data.copy()
        pulumi_id = current_data.pop("PulumiID")
        diff_resp = self.provider.pulumi_provider.diff(
            pylumi.URN(self.resource_name, pulumi_id),
            pulumi_id,
            current_data,
            check_resp,
        )

        if diff_resp["DeleteBeforeReplace"]:
            return st.ModificationAction.DELETE_AND_RECREATE

        if diff_resp["Changes"] > 1:
            return st.ModificationAction.MODIFY

        return st.ModificationAction.NONE

    async def get_expected(
        self, current: st.StateSnapshot, config: st.StateConfig, session: st.TaskSession
    ) -> Any:

        current_is_up = current.state.name == "UP"
        config_is_up = config.state.name == "UP"

        if not config_is_up:
            return {}

        check_resp, errs = self.provider.pulumi_provider.check(
            pylumi.URN(self.resource_name),
            current.data,
            object_to_pulumi_json(config.obj, session),
        )
        check_resp = self.clean_check_resp(check_resp)
        if errs:
            raise PulumiValidationError(errs)

        if not current_is_up:
            resp = self.provider.pulumi_provider.create(
                pylumi.URN(self.resource_name),
                check_resp,
                self.provider.operation_timeout,
                preview=True,
            )
            return self.make_expected_output(
                resp["Properties"], config, self.UP.output_type
            )

        current_data = current.data.copy()
        pulumi_id = current_data.pop("PulumiID")
        diff_resp = self.provider.pulumi_provider.diff(
            pylumi.URN(self.resource_name, pulumi_id),
            pulumi_id,
            current_data,
            check_resp,
        )

        output = current.data.copy()
        for key in diff_resp["ChangedKeys"]:
            output.pop(key, None)

        return self.make_expected_output(output, config, self.UP.output_type)

    async def refresh_state(self, data: Any) -> Optional[Any]:
        data = data.copy()
        pulumi_id = data.pop("PulumiID")
        resp = self.provider.pulumi_provider.read(
            pylumi.URN(self.resource_name, pulumi_id),
            pulumi_id,
            inputs=data,
            state=data,
        )
        if not resp["Outputs"]:
            return None
        return self.make_output(resp["Outputs"], pulumi_id, self.UP.output_type)

    async def create(
        self, session: st.TaskSession, config: st.StateConfig
    ) -> st.Object:
        current = st.StateSnapshot({}, self.null_state.state)
        expected = await self.get_expected(current, config, session)
        return session["create"] << (
            st.task.new(self.create_task)(config.obj) >> expected
        )

    @property
    def create_task(self) -> Any:

        input_type = self.UP.input_type
        output_type = self.UP.output_type

        async def create_task(config: input_type) -> output_type:
            check_resp, errs = self.provider.pulumi_provider.check(
                pylumi.URN(self.resource_name), {}, config
            )
            check_resp = self.clean_check_resp(check_resp)
            if errs:
                raise PulumiValidationError(errs)
            resp = self.provider.pulumi_provider.create(
                pylumi.URN(self.resource_name),
                check_resp,
                self.provider.operation_timeout,
            )
            return self.make_output(resp["Properties"], resp["ID"], output_type)

        return create_task

    async def modify(
        self,
        session: st.TaskSession,
        current: st.StateSnapshot,
        config: st.StateConfig,
    ) -> st.Object:
        expected = await self.get_expected(current, config, session)
        return session["modify"] << (
            st.task.new(self.modify_task)(current.obj, config.obj) >> expected
        )

    @property
    def modify_task(self) -> Any:

        input_type = self.UP.input_type
        output_type = self.UP.output_type

        async def modify_task(current: output_type, config: input_type) -> output_type:
            current = current.copy()
            pulumi_id = current.pop("PulumiID")

            check_resp, errs = self.provider.pulumi_provider.check(
                pylumi.URN(self.resource_name, pulumi_id), current, config
            )
            check_resp = self.clean_check_resp(check_resp)
            if errs:
                raise PulumiValidationError(errs)

            resp = self.provider.pulumi_provider.update(
                pylumi.URN(self.resource_name, pulumi_id),
                id=pulumi_id,
                olds=current,
                news=check_resp,
                timeout=self.provider.operation_timeout,
            )
            return self.make_output(resp["Properties"], resp["ID"], output_type)

        return modify_task

    async def delete(
        self, session: st.TaskSession, current: st.StateSnapshot
    ) -> st.Object:
        ref = session["delete"] << st.task.new(self.delete_task)(current.obj)
        return st.join(st.Object({}, st.EmptyType, session.ns.registry), ref)

    @property
    def delete_task(self) -> Any:

        null_type = self.DOWN.output_type
        output_type = self.UP.output_type

        async def delete_task(current: output_type) -> null_type:
            current = current.copy()
            pulumi_id = current.pop("PulumiID")
            resp = self.provider.pulumi_provider.delete(
                pylumi.URN(self.resource_name, pulumi_id),
                pulumi_id,
                current,
                timeout=self.provider.operation_timeout,
            )
            return {}

        return delete_task


class PulumiProvider(st.Provider):
    """
    Represents a pulumi provider
    """

    def __init__(
        self,
        id: st.ProviderId,
        schema: PulumiProviderSchema,
        operation_timeout: int = 3600,
    ) -> None:

        self.id = id
        self.context = None
        self.pulumi_provider = None
        self.schema = schema
        self.operation_timeout = operation_timeout

        self._resource_cache = {}

    async def setup(self) -> None:
        ctx = self.context = pylumi.Context()
        ctx.setup()
        provider = self.pulumi_provider = ctx.provider(self.schema.name, self.id.meta)
        provider.configure()

    async def teardown(self) -> None:
        self.context.teardown()
        self.context = self.pulumi_provider = None

    def get_resource(self, name: str) -> st.Resource:
        if name not in self.schema.resources:
            raise st.exc.NoResourceFound(name)

        resource_schema = self.schema.resources[name]
        # Knowing a resource's ID is necessary for pulumi operation
        output_type = st.struct_add(resource_schema.output_type, ("PulumiID", str))
        return self.construct_resource(
            name=name, input_type=resource_schema.input_type, output_type=output_type
        )

    def get_task(self, name: str) -> st.Task:
        raise NotImplementedError

    def construct_resource(
        self, name: str, input_type: st.Type, output_type: st.Type
    ) -> st.Resource:
        """
        Construct a new MachineResource for the given resource name and input/output types
        """
        if name in self._resource_cache:
            return self._resource_cache[name]

        machine_cls = type(
            PulumiResourceMachine.__name__,
            (PulumiResourceMachine,),
            {"UP": st.State("UP", input_type, output_type)},
        )

        resource = st.MachineResource(name, machine_cls, self)
        self._resource_cache[name] = resource

        return resource

    @st.hookimpl
    def get_provider(
        self, name: str, params: Dict[str, Any], registry: st.Registry
    ) -> st.Provider:
        if st.ProviderId(name, params) != self.id:
            return None
        return self


def load_pulumi_provider(
    name: str,
    schema_version: int = 0,
    config: Optional[Dict[str, Any]] = None,
    provider_name: Optional[str] = None,
    operation_timeout: Optional[int] = None,
    register: bool = True,
) -> PulumiProvider:
    """
    Load and register a pulumi provider by name.
    """
    if config is None:
        config = {}

    if provider_name is None:
        provider_name = "pulumi/" + name

    with pylumi.Context() as ctx, ctx.provider(name, config) as provider:
        schema = provider.get_schema(schema_version)

    parsed = PulumiProviderSchemaParser().parse(schema)

    kwargs = {}
    if operation_timeout is not None:
        kwargs["operation_timeout"] = operation_timeout

    provider_obj = PulumiProvider(
        st.ProviderId(provider_name, config), parsed, **kwargs
    )
    if register:
        st.registry.register(provider_obj)

    return provider_obj


class PulumiHooks:
    """
    Default hook implementations for pulumi providers.
    """

    @staticmethod
    @st.hookimpl
    def get_provider(
        name: str, params: Dict[str, Any], registry: st.Registry
    ) -> st.Provider:
        if not name.startswith("pulumi/"):
            return None
        _, provider_name = name.split("/", 1)
        return load_pulumi_provider(provider_name, config=params)


DEFAULT_PLUGINS = [PulumiHooks]


def register(registry: Optional[st.Registry] = None) -> None:
    """
    Register plugins for this module
    """
    if registry is None:
        registry = st.registry

    for plugin in DEFAULT_PLUGINS:
        registry.register(plugin)
