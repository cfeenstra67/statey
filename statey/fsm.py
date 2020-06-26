import abc
import dataclasses as dc
import types as pytypes
from collections import Counter
from functools import wraps
from typing import Sequence, Callable, Type as PyType, Dict, Any

import networkx as nx

from statey import resource, task, exc
from statey.syms import schemas, utils, symbols, types


@dc.dataclass(frozen=True)
class MachineState(resource.AbstractState):
    name: str
    schema: schemas.Schema
    null: bool = False

    @property
    def type(self) -> types.Type:
        return self.schema.output_type


@dc.dataclass(frozen=True)
class NullMachineState(MachineState):
    schema: types.Type = dc.field(init=False, default=schemas.StructSchema(()))
    null: bool = dc.field(repr=False, init=False, default=True)


@dc.dataclass(frozen=True)
class MachineResourceState(resource.ResourceState):
    state: MachineState
    resource_name: str

    def __call__(self, *args, **kwargs) -> resource.BoundState:
        bound = super().__call__(*args, **kwargs)
        return bound.clone(data=self.state.schema(bound.data))


# class TransitionSpec(abc.ABC):
#     """
#     Transition specification
#     """
#     from_name: str
#     to_name: str
#     name: str

#     @abc.abstractmethod
#     def bind(self, machine: 'Machine') -> 'Transition':
#         """
#         Factory method to bind a transition spec to a specific machine
#         instance
#         """
#         raise NotImplementedError


# @dc.dataclass(frozen=True)
# class FunctionTransitionSpec(TransitionSpec):
#     """
#     Transition spec for a simple function
#     """
#     from_name: str
#     to_name: str
#     name: str
#     func: Callable[[Any], Any]

#     def bind(self, machine: 'Machine') -> 'Transition':
#         return FunctionTransition(
#             from_name=self.from_name,
#             to_name=self.to_name,
#             machine=machine,
#             func=self.func
#         )


class Transition(abc.ABC):
    """
    A transition defines the procedure from migration a machine
    from one state to another (they may also be the same state)
    """
    from_name: str
    to_name: str
    name: str

    @abc.abstractmethod
    def plan(
        self,
        current: resource.BoundState,
        config: resource.BoundState,
        session: task.TaskSession,
        input: symbols.Symbol
    ) -> symbols.Symbol:
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

    def plan(
        self,
        current: resource.BoundState,
        config: resource.BoundState,
        session: task.TaskSession,
        input: symbols.Symbol,
    ) -> symbols.Symbol:
        return self.func(
            current=current,
            config=config,
            session=session,
            input=input
        )


def transition(from_name: str, to_name: str, name: str = utils.MISSING) -> Any:
    """
    Generate a decorate to wrap a function as a transition
    """
    def dec(func):
        nonlocal name
        if name is utils.MISSING:
            name = getattr(func, '__name__', '<unknown>')

        @wraps(func)
        def get_transition(*args, **kwargs):
            new_func = lambda *args2, **kwargs2: func(*args, *args2, **kwargs, **kwargs2)
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
        cls, old_states: Sequence[MachineState], new_states: Sequence[MachineState]
    ) -> Sequence[MachineState]:

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
        new_states = [val for val in attrs.values() if isinstance(val, MachineState)]
        states = cls._validate_states(states, new_states)
        super_cls.__states__ = tuple(states)

        transitions = super_cls.__transitions__ if hasattr(super_cls, "__transitions__") else set()
        new_transitions = {
            name for name, val in attrs.items()
            if hasattr(val, 'transition_factory') and val.transition_factory
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
            self.set_resource_state(MachineResourceState(state, resource_name))

    def set_resource_state(self, state: MachineResourceState) -> None:
        setattr(self, state.state.name, state)

    @property
    def null_state(self) -> resource.ResourceState:
        state = next((s for s in self.__states__ if s.null))
        return MachineResourceState(state, self.resource_name)

    def get_schema(self, state: str) -> schemas.Schema:
        state_map = {state.name for state in self.__states__}
        return state_map[state].schema

    def plan(
        self,
        current: resource.BoundState,
        config: resource.BoundState,
        session: task.TaskSession,
        input: symbols.Symbol,
    ) -> symbols.Symbol:

        from_name = current.resource_state.state.name
        to_name = config.resource_state.state.name

        transitions = (
            getattr(self, tran)() for tran in self.__transitions__
        )
        transition = next((
            tran for tran in transitions
            if tran.from_name == from_name
            if tran.to_name == to_name
        ), None)
        if transition is None:
            raise exc.PlanError(f'Unable to find transition from {from_name} to {to_name}.')

        return transition.plan(current, config, session, input)

    def __call__(self, *args, **kwargs) -> resource.ResourceState:
        states = [state for state in self.__states__ if state != self.null_state.state]
        if len(states) > 1:
            raise TypeError(f'"{self.resource_name}" has more than one non-null state.')
        return MachineResourceState(states[0], self.resource_name)(*args, **kwargs)

    @abc.abstractmethod
    async def refresh(self, current: resource.BoundState) -> resource.BoundState:
        """
        Same as Resource.refresh()
        """
        raise NotImplementedError

    async def finalize(self, current: resource.BoundState) -> resource.BoundState:
        return current


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

    def plan(
        self,
        current: resource.BoundState,
        config: resource.BoundState,
        session: task.TaskSession,
        input: symbols.Symbol,
    ) -> symbols.Symbol:
        return self.s.plan(current, config, session, input)

    async def refresh(self, current: resource.BoundState) -> resource.BoundState:
        return await self.s.refresh(current)

    async def finalize(self, current: resource.BoundState) -> resource.BoundState:
        return await self.s.finalize(current)
