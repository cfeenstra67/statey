import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Sequence, Dict, Any, Optional

import pylumi

import statey as st
from statey.lib.pulumi.constants import PULUMI_ID, PULUMI_NS
from statey.lib.pulumi.helpers import (
    parse_provider_schema_response,
    PulumiProviderSchema,
)
from statey.lib.pulumi.resource import PulumiResourceMachine


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
        self.schema = schema
        self.operation_timeout = operation_timeout

        self.context = None
        self.pulumi_provider = None
        self.thread_pool = None

        self._resource_cache = {}

    async def setup(self) -> None:
        loop = asyncio.get_running_loop()

        pool = self.thread_pool = ThreadPoolExecutor()
        pool.__enter__()

        ctx = self.context = pylumi.Context()
        provider = self.pulumi_provider = ctx.provider(self.schema.name, self.id.meta)
        # Doesn't work to do this async for some reason
        ctx.setup()
        provider.configure()

    async def teardown(self) -> None:
        loop = asyncio.get_running_loop()

        # Doesn't work w/ async for some reason
        self.context.teardown()

        self.thread_pool.__exit__(None, None, None)
        self.context = self.pulumi_provider = self.thread_pool = None

    def list_resources(self) -> Sequence[str]:
        return sorted(self.schema.resources)

    def get_resource(self, name: str) -> st.Resource:
        if name not in self.schema.resources:
            raise st.exc.NoResourceFound(name)

        resource_schema = self.schema.resources[name]
        # Knowing a resource's ID is necessary for pulumi operation
        output_type = st.struct_add(resource_schema.output_type, (PULUMI_ID, str))
        return self.construct_resource(
            name=name, input_type=resource_schema.input_type, output_type=output_type
        )

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

        resource = machine_cls(name, self)
        self._resource_cache[name] = resource

        return resource

    @st.hookimpl
    def get_provider(
        self, name: str, params: Dict[str, Any], registry: st.Registry
    ) -> st.Provider:
        if st.ProviderId(name, params) != self.id:
            return None
        return self

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.id})"


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

    parsed = parse_provider_schema_response(schema)

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
        if not name.startswith(PULUMI_NS + "/"):
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
