import abc
import dataclasses as dc
import enum
import types as pytypes
from collections import Counter
from functools import wraps
from typing import Sequence, Callable, Type as PyType, Dict, Any, Optional

import networkx as nx

import statey as st
from statey import resource, task, exc
from statey.syms import utils, types, Object, diff


class Transition(abc.ABC):
    """
    A transition defines the procedure from migration a machine
    from one state to another (they may also be the same state)
    """

    from_name: str
    to_name: str
    name: str

    @abc.abstractmethod
    async def plan(
        self,
        current: resource.BoundState,
        config: resource.BoundState,
        session: task.TaskSession,
    ) -> Object:
        """
        Same as Resource.plan(), except for planning
        a specific transition.
        """
        raise NotImplementedError


@dc.dataclass(frozen=True)
class FunctionTransition(Transition):
    """
    Transition class that simply wraps a function
    """

    from_name: str
    to_name: str
    name: str
    func: Callable[[Any], Any]

    async def plan(
        self,
        current: resource.StateSnapshot,
        config: resource.StateConfig,
        session: task.TaskSession,
    ) -> Object:
        return await self.func(current=current, config=config, session=session)


def transition(from_name: str, to_name: str, name: str = utils.MISSING) -> Any:
    """
    Generate a decorate to wrap a function as a transition
    """

    def dec(func):
        nonlocal name
        if name is utils.MISSING:
            name = getattr(func, "__name__", "<unknown>")

        @wraps(func)
        def get_transition(*args, **kwargs):
            new_func = lambda *args2, **kwargs2: func(
                *args, *args2, **kwargs, **kwargs2
            )
            return FunctionTransition(from_name, to_name, name, new_func)

        get_transition.transition_factory = True

        return get_transition

    return dec


class MachineMeta(type(resource.States)):
    """
    Special behavior for state machines
    """

    @classmethod
    def _validate_states(
        cls, old_states: Sequence[resource.State], new_states: Sequence[resource.State]
    ) -> Sequence[resource.State]:

        new_names = Counter(state.name for state in new_states)
        if new_names and max(new_names.values()) > 1:
            multi = {k: v for k, v in new_names.items() if v > 1}
            raise ValueError(f"Duplicate states found: {multi}")

        old_states = [state for state in old_states if state.name not in new_names]
        return old_states + list(new_states)

    def __new__(
        cls, name: str, bases: Sequence[PyType], attrs: Dict[str, Any]
    ) -> PyType:
        super_cls = super().__new__(cls, name, bases, attrs)
        states = super_cls.__states__ if hasattr(super_cls, "__states__") else ()
        new_states = [val for val in attrs.values() if isinstance(val, resource.State)]
        states = cls._validate_states(states, new_states)
        super_cls.__states__ = tuple(states)

        transitions = (
            super_cls.__transitions__
            if hasattr(super_cls, "__transitions__")
            else set()
        )
        new_transitions = {
            name
            for name, val in attrs.items()
            if hasattr(val, "transition_factory") and val.transition_factory
        }
        super_cls.__transitions__ = transitions | new_transitions

        return super_cls


class Machine(resource.States, metaclass=MachineMeta):
    """
    Class with a metaclass to automatically collect states and transitions into class variables.
    """

    def __init__(self, resource_name: str) -> None:
        self.resource_name = resource_name
        # This is temporary, should clean this up
        for state in self.__states__:
            self.set_resource_state(resource.ResourceState(state, resource_name))

    def set_resource_state(self, state: resource.ResourceState) -> None:
        setattr(self, state.state.name, state)

    @property
    def null_state(self) -> resource.ResourceState:
        state = next((s for s in self.__states__ if s.null))
        return resource.ResourceState(state, self.resource_name)

    async def plan(
        self,
        current: resource.StateSnapshot,
        config: resource.StateConfig,
        session: task.TaskSession,
    ) -> Object:

        from_name = current.state.name
        to_name = config.state.name

        transitions = (getattr(self, tran)() for tran in self.__transitions__)
        transition = next(
            (
                tran
                for tran in transitions
                if tran.from_name == from_name
                if tran.to_name == to_name
            ),
            None,
        )
        if transition is None:
            raise exc.PlanError(
                f"Unable to find transition from {from_name} to {to_name}."
            )

        return await transition.plan(current, config, session)

    def __call__(self, *args, **kwargs) -> resource.ResourceState:
        states = [state for state in self.__states__ if state != self.null_state.state]
        if len(states) > 1:
            raise TypeError(f'"{self.resource_name}" has more than one non-null state.')
        if len(states) < 1:
            raise TypeError(
                f'"{self.resource_name}" does not have any non-null states.'
            )
        return resource.ResourceState(states[0], self.resource_name)(*args, **kwargs)

    @abc.abstractmethod
    async def refresh(self, current: resource.BoundState) -> resource.BoundState:
        """
        Same as Resource.refresh()
        """
        raise NotImplementedError

    async def finalize(self, current: resource.BoundState) -> resource.BoundState:
        return current


class ModificationAction(enum.Enum):
    """
    Actions to control simple machine behavior
    """

    NONE = "none"
    MODIFY = "modify"
    DELETE_AND_RECREATE = "delete_and_recreate"


class SingleStateMachine(Machine):
    """
    A simple machine is an FSM which can only have two states: UP and DOWN.

    Note that a SimpleMachine's UP state should have all of the same fields available
    in its output type as its input type.
    """

    UP: resource.State
    DOWN: resource.NullState = resource.NullState("DOWN")

    @abc.abstractmethod
    async def create(
        self, session: task.TaskSession, config: resource.StateConfig
    ) -> "Object":
        """
        Create this resource with the given configuration
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def delete(
        self, session: task.TaskSession, current: resource.StateSnapshot
    ) -> "Object":
        """
        Delete the resource with the given data
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def modify(
        self,
        session: task.TaskSession,
        current: resource.StateSnapshot,
        config: resource.StateConfig,
    ) -> "Object":
        """
        Modify the resource from `data` to the given config. Default implementation
        is always to delete and recreate the resource.

        NOTE: if subclasses do not modify the get_action() implementation they can
        override this with a stub method, as it will never be called. It is defined
        as an abstract to avoid the case where it is omitted accidentally and
        NotImplementedError is raised during the task execution
        """
        raise NotImplementedError

    # Overridding this as an "optional" abstract method
    modify = NotImplemented

    @abc.abstractmethod
    async def refresh_state(self, data: Any) -> Optional[Any]:
        """
        Get a refreshed version of `data` (which is in the state UP). Return None
        to indicate the resource no longer exists.
        """
        raise NotImplementedError

    async def refresh_config(self, config: "Object") -> "Object":
        """
        Transform a configuration before planning
        """
        return config

    def get_action(self, diff: diff.Diff) -> ModificationAction:
        """
        With the given diff, determine which action must be taken to get to the configured
        state. This is only called when both the current and configured state are UP.

        Overriding this method is optional, by default it will always delete and recreate
        the resource.
        """
        if not diff:
            return ModificationAction.NONE
        return ModificationAction.DELETE_AND_RECREATE

    def get_diff(
        self,
        current: resource.StateSnapshot,
        config: resource.StateConfig,
        session: task.TaskSession,
    ) -> diff.Diff:
        """
        Produce a diff given the current, config and session data
        """
        differ = session.ns.registry.get_differ(config.state.input_type)
        current_as_config = st.filter_struct(current.obj, config.type)
        return differ.diff(current_as_config, config.obj, session)

    async def refresh(self, current: resource.StateSnapshot) -> resource.StateSnapshot:
        if current.state.name == self.null_state.name:
            return current
        info = await self.refresh_state(current.data)
        if info is None:
            return resource.StateSnapshot({}, self.null_state)
        return resource.StateSnapshot(info, current.state)

    @transition("UP", "UP")
    async def modify_resource(
        self,
        current: resource.StateSnapshot,
        config: resource.StateConfig,
        session: task.TaskSession,
    ) -> Object:

        config = config.clone(obj=await self.refresh_config(config.obj))

        diff = self.get_diff(current, config, session)

        action = self.get_action(diff)

        if action == ModificationAction.NONE:
            return current.obj

        if action == ModificationAction.MODIFY:
            if self.modify is NotImplemented:
                raise NotImplementedError(
                    f"`modify` has not been defined in {type(self).__name__}."
                )
            return await self.modify(session, current, config)

        if action == ModificationAction.DELETE_AND_RECREATE:
            raise exc.NullRequired
            # ref = await self.delete(session, current)
            # snapshotted = session["post_deletion_snapshot"] << self.DOWN.snapshot(ref)
            # joined_config = config.clone(obj=st.join(config.obj, snapshotted))
            # return await self.create(session, joined_config)

        raise exc.InvalidModificationAction(action)

    @transition("DOWN", "UP")
    async def create_resource(
        self,
        current: resource.StateSnapshot,
        config: resource.StateConfig,
        session: task.TaskSession,
    ) -> Object:

        config = config.clone(obj=await self.refresh_config(config.obj))
        return await self.create(session, config)

    @transition("UP", "DOWN")
    async def delete_resource(
        self,
        current: resource.StateSnapshot,
        config: resource.StateConfig,
        session: task.TaskSession,
    ) -> Object:

        return await self.delete(session, current)


class SimpleMachine(SingleStateMachine):
    """
    A simple machine has only a single state and each transition only consists
    of a single task
    """

    async def get_expected(self, config: resource.StateConfig) -> Any:
        """
        Get the expected output for the given configuration. Default implementation
        is just passing through config fields and setting the rest as unknown
        """
        return st.fill_unknowns(config.obj, config.state.output_type)

    # Not defined as abstract methods because subclasses may want to just override
    # the top-level methods instead
    async def create_task(self, config: Any) -> Any:
        """
        Defines a single task called "create" that will create this resource
        """
        raise NotImplementedError

    async def delete_task(self, current: Any) -> Any:
        """
        Defines a single task called "delete" that will delete this resource
        """
        raise NotImplementedError

    async def modify_task(self, current: Any, config: Any) -> Any:
        """
        Defines a single task called "modify" that will modify this resource
        """
        raise NotImplementedError

    def _get_optional_method(self, name: str) -> Callable[[Any], Any]:
        if getattr(type(self), name) is getattr(SimpleMachine, name):
            raise NotImplementedError(f"{name} has not been defined in this class.")
        return getattr(self, name)

    async def create(
        self, session: task.TaskSession, config: resource.StateConfig
    ) -> "Object":
        expected = await self.get_expected(config)
        create_task = self._get_optional_method("create_task")
        return session["create"] << (task.new(create_task)(config.obj) >> expected)

    async def delete(
        self, session: task.TaskSession, current: resource.StateSnapshot
    ) -> "Object":
        delete_task = self._get_optional_method("delete_task")
        ref = session["delete"] << task.new(delete_task)(current.obj)
        return st.join(st.Object({}, st.EmptyType, session.ns.registry), ref)

    async def modify(
        self,
        session: task.TaskSession,
        current: resource.StateSnapshot,
        config: resource.StateConfig,
    ) -> "Object":
        expected = await self.get_expected(config)
        modify_task = self._get_optional_method("modify_task")
        return session["modify"] << (
            task.new(modify_task)(current.obj, config.obj) >> expected
        )


class MachineResource(resource.Resource):
    """
    Simple wrapper resource, for state machines all logic is really in the States
    implementation

    Example:
    rs = MachineResource(MyMachine('new_resource'))
    """

    # This will be set in the constructor
    States = None

    def __init__(self, name: str, machine_cls: PyType[Machine]) -> None:
        self.States = self.machine_cls = machine_cls
        self.name = name
        super().__init__()

    async def plan(
        self,
        current: resource.StateSnapshot,
        config: resource.StateConfig,
        session: task.TaskSession,
    ) -> Object:
        return await self.s.plan(current, config, session)

    async def refresh(self, current: resource.StateSnapshot) -> resource.StateSnapshot:
        return await self.s.refresh(current)

    async def finalize(self, current: resource.StateSnapshot) -> resource.StateSnapshot:
        return await self.s.finalize(current)
