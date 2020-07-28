NS = "statey"

from statey.hooks import (
    hookimpl,
    hookspec,
    create_plugin_manager,
    register_default_plugins,
)

from statey.registry import Registry, create_registry, RegistryCachingWrapper

registry = create_registry()

from statey import task, syms, exc

from statey.fsm import (
    Machine,
    transition,
    MachineResource,
    SingleStateMachine,
    SimpleMachine,
)

from statey.syms.api import (
    F,
    join,
    struct,
    function,
    map,
    declarative,
    replace,
    fill,
    fill_unknowns,
    filter_struct,
)

from statey.syms.diff import Diff, DiffConfig, Differ

from statey.syms.func import Function

from statey.syms.impl import Unknown

from statey.syms.object_ import Object

from statey.syms.schemas import builder as S

from statey.syms.session import Session

from statey.syms.py_session import create_session

from statey.syms.types import (
    Type,
    StructType,
    StringType,
    IntegerType,
    FloatType,
    ArrayType,
    BooleanType,
    Field,
    EmptyType,
)

from statey.resource import (
    BoundState,
    ResourceSession,
    ResourceState,
    State,
    NullState,
    create_resource_session,
    StateSnapshot,
    StateConfig,
    ResourceGraph,
    Resource,
)

from statey.task import TaskSession, create_task_session


def set_registry(new_registry: Registry) -> None:
    """
    Set st.registry
    """
    global registry
    registry = new_registry


register_default_plugins(registry)
