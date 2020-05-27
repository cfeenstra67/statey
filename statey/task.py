import abc
import dataclasses as dc
import enum
from datetime import datetime
from typing import Tuple, Any, Optional, Callable, Sequence

import networkx as nx
import pluggy

import statey as st
from statey.syms import session, types, symbols, utils


class TaskStatus(enum.Enum):
	"""

	"""
	NOT_STARTED = 0
	PENDING = 1
	SKIPPED = 2
	FAILED = 3
	SUCCESS = 4


@dc.dataclass(frozen=True)
class TaskInfo:
	"""
	Contains information about the state of a task
	"""
	status: TaskStatus
	timestamp: datetime = dc.field(default_factory=datetime.utcnow)
	error: Optional[Exception] = None
	skipped_by: Optional[str] = None


class Task(abc.ABC):
	"""
	Base class for tasks. A task is a unit of computation
	"""
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
class SessionTaskSpec(abc.ABC):
	"""
	This is essentially a factory that allows us to implement tasks for a session
	"""
	input_type: types.Type
	output_type: types.Type

	def always_eager(self) -> bool:
		return False

	@property
	@abc.abstractmethod
	def expected(self) -> Any:
		"""
		Return the expected value for this task, returning utils.MISSING if none is known
		"""
		raise NotImplementedError

	@abc.abstractmethod
	async def run(self, data: Any) -> Any:
		"""
		Perform a task on the given input data
		"""
		raise NotImplementedError

	def __call__(self, data: Any) -> 'BoundTask':
		return BoundTaskSpec(self, data)


@dc.dataclass(frozen=True)
class FunctionTaskSpec(SessionTaskSpec):
	"""
	This is essentially a factory that allows us to implement tasks for a session
	"""
	func: Callable[[Any], Any]
	expected: Any = utils.MISSING

	async def run(self, data: Any) -> Any:
		"""
		Perform a task on the given input data
		"""
		return await self.func(data)

	def __call__(self, data: Any) -> 'BoundTask':
		return BoundTaskSpec(self, data)


@dc.dataclass(frozen=True)
class BoundTaskSpec:
	"""
	A task bound with input data
	"""
	spec: SessionTaskSpec
	data: Any

	def bind(self, session: session.Session) -> 'SessionTask':
		new_future = symbols.Future(
			self.spec.output_type,
			session.ns.registry,
			expected=self.spec.expected
		)
		return SessionTask(
			session=session,
			spec=self.spec,
			data=self.data,
			output_future=new_future
		)


@dc.dataclass(frozen=True)
class SessionTask(Task):
	"""
	A task that can be used symbolically within a session
	"""
	session: session.Session
	spec: SessionTaskSpec
	data: Any
	output_future: symbols.Future

	def always_eager(self) -> bool:
		return False

	async def run(self) -> None:
		resolved_input = self.session.resolve(self.data, decode=False)
		output = await self.spec.run(resolved_input)
		output_symbol = symbols.Literal(
			value=output,
			type=self.output_future.type,
			registry=self.session.ns.registry
		)
		resolved_output = self.session.resolve(output_symbol, decode=False)
		self.output_future.set_result(resolved_output)


@dc.dataclass(frozen=True)
class SessionSwitch(Task):
	"""
	A session switch resolves a key in one session, and sets that in another session
	"""
	input_session: session.Session
	input_symbol: symbols.Symbol
	output_session: session.Session
	output_key: str
	allow_unknowns: bool = True

	def always_eager(self) -> bool:
		return False

	async def run(self) -> None:
		resolved_input = self.input_session.resolve(self.input_symbol, allow_unknowns=self.allow_unknowns, decode=False)
		self.output_session.set_data(self.output_key, resolved_input)


@dc.dataclass(frozen=True)
class ResourceGraphOperation(Task):
	"""
	Defines some operation to perform against a resource graph
	"""
	key: str
	resource_graph: 'ResourceGraph'

	def always_eager(self) -> bool:
		return True


@dc.dataclass(frozen=True)
class GraphSetKey(ResourceGraphOperation):
	"""
	Set some key in a resource graph
	"""
	input_session: session.Session
	input_symbol: symbols.Symbol
	dependencies: Sequence[str] = ()
	remove_dependencies: bool = True
	state: Optional['ResourceState'] = None

	async def run(self) -> None:
		self.resource_graph.set(
			key=self.key,
			value=self.input_session.resolve(self.input_symbol, decode=False),
			type=self.input_symbol.type,
			remove_dependencies=self.remove_dependencies,
			state=self.state
		)
		if self.dependencies:
			self.resource_graph.add_dependencies(self.key, self.dependencies)


class GraphDeleteKey(ResourceGraphOperation):
	"""
	Delete some key in a resource graph.
	"""
	async def run(self) -> None:
		self.resource_graph.delete(self.key)


@dc.dataclass(frozen=True)
class Checkpointer:
	"""
	A checkpointer can store partially migrates states of a resource
	"""
	output_session: 'ResourceSession'
	output_key: str
	resource_name: str

	def checkpoint(self, value: Any, state: 'State') -> None:
		"""
		Save a partially migrated state as a checkpoing in `output_session`.
		"""
		from statey.resource import ResourceState, BoundState
		# This can change in different checkpoints, so overwrite to be safe.
		self.output_session.ns.new(self.output_key, state.type, overwrite=True)
		state = BoundState(
			resource_state=ResourceState(state=state, resource_name=self.resource_name),
			data=value
		)
		self.output_session.set(sel.key, state)


class TaskSession(session.Session):
	"""
	Session subclass that wraps a regular session but handles resources in a special manner.
	"""
	def __init__(self, session: session.Session, checkpointer: Optional[Checkpointer] = None) -> None:
		super().__init__(session.ns)
		self.session = session
		self.checkpointer = checkpointer
		self.tasks = {}
		self.pm.register(self)

	@st.hookimpl
	def before_set(self, key: str, value: Any) -> Tuple[Any, types.Type]:
		if not isinstance(value, BoundTaskSpec):
			return None
		self.tasks[key] = bound = value.bind(self)
		return bound.output_future, bound.output_future.type

	def checkpoint(self, symbol: symbols.Symbol, state: 'State') -> BoundTaskSpec:

		async def _checkpoint(value):
			if self.checkpointer is not None:
				self.checkpointer.checkpoint(value, state)
			return {}

		return FunctionTaskSpec(
			input_type=symbol.type,
			output_type=types.EmptyType,
			func=_checkpoint
		)(symbol)

	def resolve(self, symbol: symbols.Symbol, allow_unknowns: bool = False, decode: bool = True) -> Any:
		return self.session.resolve(symbol, allow_unknowns, decode)

	def set_data(self, key: str, data: Any) -> None:
		return self.session.set_data(key, data)

	def delete_data(self, key: str) -> None:
		return self.session.delete_data(key)

	def dependency_graph(self) -> nx.MultiDiGraph:
		return self.session.dependency_graph()

	def task_graph(self) -> nx.DiGraph:
		task_subgraph = self.dependency_graph()
		utils.subgraph_retaining_dependencies(task_subgraph, list(self.tasks))
		for node in task_subgraph.nodes:
			task_subgraph.nodes[node]['task'] = self.tasks[node]
		return task_subgraph

	def clone(self) -> 'TaskSession':
		cloned_session = self.session.clone()
		new_inst = type(self)(cloned_session)
		new_inst.tasks = self.tasks.copy()
		new_inst.pm = self.pm
		return new_inst
