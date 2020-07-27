import abc
import dataclasses as dc
import enum
import inspect
import sys
import textwrap as tw
import traceback
from datetime import datetime
from functools import wraps, partial
from typing import (
    Tuple,
    Any,
    Optional,
    Callable,
    Sequence,
    Type as PyType,
    Dict,
    Coroutine,
)

import networkx as nx

import statey as st
from statey.syms import session, types, utils, impl, Object


class TaskStatus(enum.Enum):
    """
    Indicates what stage of execution a given task is in.
	"""

    NOT_STARTED = 0
    PENDING = 1
    SKIPPED = 2
    FAILED = 3
    SUCCESS = 4


@dc.dataclass(frozen=True)
class ErrorInfo:
    """
	Container for the for all error information
	"""

    exc_type: Optional[PyType[Exception]] = None
    exc_value: Optional[Exception] = None
    exc_tb: Optional["traceback"] = None

    @classmethod
    def exc_info(cls) -> "ErrorInfo":
        """
		Mimics the sys.exc_info() method
		"""
        return cls(*sys.exc_info())

    def format_exception(self) -> str:
        """
		Return a formatted version of this exception.
		"""
        return "\n".join(
            traceback.format_exception(self.exc_type, self.exc_value, self.exc_tb)
        )


@dc.dataclass(frozen=True)
class TaskInfo:
    """
	Contains information about the state of a task
	"""

    status: TaskStatus
    timestamp: datetime = dc.field(default_factory=datetime.utcnow)
    error: Optional[ErrorInfo] = None
    skipped_by: Optional[str] = None


class Task(abc.ABC):
    """
	Base class for tasks. A task is a unit of computation
	"""

    description: Optional[str]

    @abc.abstractmethod
    def always_eager(self) -> bool:
        """
		Indicate that this task should always be executed eagerly, meaning even if
		we are interrupted in the middle of execution if all of its dependencies get
		executed, we will still execute this task as long as we exit cleanly
		"""
        raise NotImplementedError

    @abc.abstractmethod
    async def run(self) -> None:
        """
		This should be overridden to implement actual task logic.
		"""
        raise NotImplementedError


@dc.dataclass(frozen=True)
class FunctionTaskFactory:
    """
	This is essentially a factory that allows us to implement tasks for a session
	"""

    func: Callable[[Any], Any]
    is_always_eager: bool = False
    description: Optional[str] = None

    def __call__(self, *args, **kwargs) -> "SessionTaskSpec":
        """
		Create a SessionTaskSpec from this function and the given arguments
		"""
        return SessionTaskSpec(
            func=self.func,
            args=args,
            kwargs=kwargs,
            is_always_eager=self.is_always_eager,
            description=self.description,
        )


def task_wrapper(
    func: Callable[[Any], Any] = utils.MISSING,
    description: Optional[str] = None,
    always_eager: bool = False,
    maybe_instance_method: bool = True,
) -> Callable[[Any], Any]:
    """
	Decorator to wrap `func` as a task factory. `func` should be an asynchronous function.
	"""

    def process(_func):
        desc = description
        if desc is None:
            doc = getattr(_func, "__doc__", None)
            desc = tw.dedent(doc).strip() if doc else desc

        sig = inspect.signature(_func)
        param_names = list(sig.parameters)

        if param_names[:1] == ["self"] and maybe_instance_method:

            @wraps(_func)
            def wrapper(self, *args, **kwargs):
                return FunctionTaskFactory(
                    func=partial(func, self),
                    is_always_eager=always_eager,
                    description=desc,
                )(*args, **kwargs)

        else:

            @wraps(_func)
            def wrapper(*args, **kwargs):
                return FunctionTaskFactory(
                    func=_func, is_always_eager=always_eager, description=desc
                )(*args, **kwargs)

        return wrapper

    if func is utils.MISSING:
        return process
    return process(func)


# Alias for the task() function to allow syntax like st.task.new()
new = task_wrapper


@dc.dataclass(frozen=True)
class SessionTaskSpec(utils.Cloneable):
    """
	A task bound with input data
	"""

    func: Callable[[Any], Any]
    args: Sequence[Any]
    kwargs: Dict[str, Any]
    description: Optional[str] = None
    is_always_eager: bool = False
    expected: Any = utils.MISSING

    def _expect(self, value: Any) -> "SessionTaskSpec":
        """
		Set the expectation of the output future for tasks created from this spec.
		"""
        return self.clone(expected=value)

    def __rshift__(self, value: Any) -> "SessionTaskSpec":
        return self._expect(value)

    def bind(self, session: session.Session) -> "SessionTask":
        """
		Bind this spec to the given session, returning a SessionTask that can be run independently.
		"""
        call_obj = utils.wrap_function_call(
            self.func, self.args, self.kwargs, registry=session.ns.registry
        )
        new_future = impl.Future(tuple(call_obj._impl.arguments.values()))

        return SessionTask(
            session=session,
            func=self.func,
            func_type=call_obj._impl.func.type,
            arguments=call_obj._impl.arguments,
            output_future=new_future,
            output_type=call_obj._type,
            description=self.description,
            is_always_eager=self.is_always_eager,
        )


@dc.dataclass(frozen=True)
class SessionTask(Task):
    """
	A task that can be used symbolically within a session
	"""

    session: session.Session
    func: Callable[[Any], Any]
    func_type: types.FunctionType
    arguments: Dict[str, Object]
    output_future: impl.Future
    output_type: types.Type
    description: Optional[str] = None
    is_always_eager: bool = False
    description: Optional[str] = None

    def always_eager(self) -> bool:
        return self.is_always_eager

    async def run(self) -> None:
        args = []
        for arg in self.func_type.args:
            ref = self.arguments[arg.name]
            args.append(self.session.resolve(ref))

        result = self.func(*args)

        def handle_result(output, idx=0):
            output_symbol = Object(output, self.output_type, self.session.ns.registry)
            resolved_output = self.session.resolve(output_symbol, decode=False)
            self.output_future.set_result(resolved_output, overwrite=idx > 0)

        # Handle all types of functions:
        # - async generators
        # - async functions
        # - regular generators
        # - regular functions
        if inspect.isasyncgen(result):
            idx = 0
            async for output in result:
                handle_result(output, idx)
                idx += 1
        elif inspect.iscoroutine(result):
            handle_result(await result)
        elif inspect.isgenerator(result):
            for idx, output in enumerate(result):
                handle_result(output, idx)
        else:
            handle_result(result)


@dc.dataclass(frozen=True)
class SessionSwitch(Task):
    """
	A session switch resolves a key in one session, and sets that in another session
	"""

    input_session: session.Session
    input_symbol: Object
    output_session: session.Session
    output_key: str
    allow_unknowns: bool = True
    description: Optional[str] = None

    def always_eager(self) -> bool:
        return False

    async def run(self) -> None:
        resolved_input = self.input_session.resolve(
            self.input_symbol, allow_unknowns=self.allow_unknowns, decode=False
        )
        self.output_session.set_data(self.output_key, resolved_input)


@dc.dataclass(frozen=True)
class ResourceGraphOperation(Task):
    """
	Defines some operation to perform against a resource graph
	"""

    key: str
    resource_graph: "ResourceGraph"

    def always_eager(self) -> bool:
        return True


async def async_identity(x):
    """
    Simple async identity function
    """
    return x


@dc.dataclass(frozen=True)
class GraphSetKey(ResourceGraphOperation):
    """
	Set some key in a resource graph
	"""

    input_session: session.Session
    input_symbol: Object
    dependencies: Sequence[str] = ()
    remove_dependencies: bool = True
    state: Optional["ResourceState"] = None
    finalize: Callable[[Any], Coroutine] = async_identity
    description: Optional[str] = None

    async def run(self) -> None:
        from statey.resource import StateSnapshot

        data = self.input_session.resolve(self.input_symbol, decode=False)
        state = StateSnapshot(data, self.state)
        final_state = await self.finalize(state)

        self.resource_graph.set(
            key=self.key,
            value=final_state.data,
            type=self.input_symbol._type,
            remove_dependencies=self.remove_dependencies,
            state=final_state.state,
        )
        if self.dependencies:
            self.resource_graph.add_dependencies(self.key, self.dependencies)


@dc.dataclass(frozen=True)
class GraphDeleteKey(ResourceGraphOperation):
    """
	Delete some key in a resource graph.
	"""

    description: Optional[str] = None

    async def run(self) -> None:
        self.resource_graph.delete(self.key)


class TaskSession(session.Session):
    """
	Session subclass that wraps a regular session but handles resources in a special manner.
	"""

    def __init__(self, session: session.Session) -> None:
        super().__init__(session.ns)
        self.session = session
        self.checkpoints = {}
        self.tasks = {}
        self.pm.register(self)

    def before_set_checkpoint(
        self, key: str, value: "StateSnapshot"
    ) -> Tuple[Any, types.Type]:
        self.checkpoints[key] = value
        return value.data, value.state.output_type

    def before_set_bind_task(
        self, key: str, value: SessionTaskSpec
    ) -> Tuple[Any, types.Type]:
        self.tasks[key] = bound = value.bind(self)
        out_sym = Object(bound.output_future, bound.output_type, self.ns.registry)
        if value.expected is not utils.MISSING:
            out_sym >>= value.expected
        return out_sym, out_sym._type

    @st.hookimpl
    def before_set(self, key: str, value: Any) -> Tuple[Any, types.Type]:
        if isinstance(value, SessionTaskSpec):
            return self.before_set_bind_task(key, value)
        if isinstance(value, st.StateSnapshot):
            return self.before_set_checkpoint(key, value)
        return None

    def resolve(
        self, symbol: Object, allow_unknowns: bool = False, decode: bool = True
    ) -> Any:
        return self.session.resolve(symbol, allow_unknowns, decode)

    def set_data(self, key: str, data: Any) -> None:
        return self.session.set_data(key, data)

    def delete_data(self, key: str) -> None:
        return self.session.delete_data(key)

    def dependency_graph(self) -> nx.MultiDiGraph:
        return self.session.dependency_graph()

    def task_graph(
        self, resource_graph: "ResourceGraph", checkpoint_key: str
    ) -> nx.DiGraph:
        """
        Creates a graph with only tasks as the nodes. If executed in order
        each task's input will be fully resolved and the entire graph of tasks
        has a deterministic output
        """
        task_subgraph = self.dependency_graph()
        keep = list(self.tasks) + list(self.checkpoints)
        utils.subgraph_retaining_dependencies(task_subgraph, keep)
        for node in task_subgraph.nodes:
            if node in self.tasks:
                task_subgraph.nodes[node]["task"] = self.tasks[node]
            else:
                # Construct checkpoint task
                state = self.checkpoints[node]
                resource = self.ns.registry.get_resource(state.state.resource)
                task = GraphSetKey(
                    input_session=self,
                    input_symbol=state.obj,
                    remove_dependencies=False,
                    state=state.state,
                    key=checkpoint_key,
                    resource_graph=resource_graph,
                    finalize=resource.finalize,
                )
                task_subgraph.nodes[node]["task"] = task

        return task_subgraph

    def clone(self) -> "TaskSession":
        cloned_session = self.session.clone()
        new_inst = type(self)(cloned_session)
        new_inst.tasks = self.tasks.copy()
        new_inst.checkpoints = self.checkpoints.copy()
        new_inst.pm = self.pm
        return new_inst


def create_task_session(session: Optional[session.Session] = None) -> TaskSession:
    """
    Create a task session with default configuration
    """
    if session is None:
        session = st.create_session()
    return TaskSession(session)
