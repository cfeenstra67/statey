import warnings

NS = "statey"

__version__ = "0.0.6"

from statey.hooks import (
    hookimpl,
    hookspec,
    create_plugin_manager,
    register_default_plugins,
)

from statey.registry import Registry, create_registry

registry = create_registry()

from statey import (
    exc,
    helpers,
    plan,
    task,
    syms,
)

from statey.fsm import (
    Machine,
    transition,
    SingleStateMachine,
    SimpleMachine,
    ModificationAction,
)

from statey.plan import Plan, PlanAction, Migrator, DefaultMigrator

from statey.plugin_installer import PluginInstaller

from statey.provider import Provider, ProviderId

from statey.syms.diff import Diff, DiffConfig, Differ

# `fmt` is an optional dependency, this import will fail if it isn't installed
try:
    from statey.syms.fmt import f
except RuntimeError:
    warnings.warn(
        "`fmt` is not installed, the st.f() function will not be usable.",
        RuntimeWarning,
    )

from statey.syms.func import Function

from statey.syms.impl import Reference, Unknown

from statey.syms.object_ import Object

from statey.syms.session import Session

from statey.syms.path import PathParser

from statey.syms.py_session import create_session

from statey.syms.types import (
    Type,
    StructType,
    StringType,
    IntegerType,
    FloatType,
    ArrayType,
    MapType,
    BooleanType,
    NumberType,
    Field,
    EmptyType,
    Integer,
    String,
    Float,
    Struct,
    Array,
    Boolean,
    Any,
    Map,
)

from statey.syms.api import (
    F,
    join,
    struct,
    function,
    map,
    declarative,
    struct_replace,
    fill,
    fill_unknowns,
    filter_struct,
    ifnull,
    struct_drop,
    struct_add,
    struct_interpolate,
    str,
    int,
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

from statey.task import Task, TaskSession, TaskStatus, create_task_session


def set_registry(new_registry: Registry) -> None:
    """
    Set st.registry
    """
    global registry
    registry = new_registry


register_default_plugins(registry)
