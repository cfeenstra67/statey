import abc
import dataclasses as dc
from typing import Tuple, Any, Optional, Callable

import networkx as nx
import pluggy

import statey as st
from statey.syms import session, types, symbols, utils


class Task(abc.ABC):
	"""
	Base class for tasks. A task is a unit of computation
	"""
	@abc.abstractmethod
	def run(self) -> None:
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

	@abc.abstractmethod
	def run(self, data: Any) -> Any:
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

	def run(self, data: Any) -> Any:
		"""
		Perform a task on the given input data
		"""
		return self.func(data)

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
		new_future = symbols.Future(self.spec.output_type, session.ns.registry)
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

	def run(self) -> None:
		resolved_input = self.session.resolve(self.data)
		output = self.spec.run(resolved_input)
		resolved_output = self.session.resolve(output)
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
	overwrite_output_type: Optional[types.Type] = None

	def run(self) -> None:
		resolved_input = self.input_session.resolve(self.input_symbol, allow_unknowns=self.allow_unknowns)
		if self.overwrite_output_type is not None:
			self.output_session.ns.new(self.output_key, self.overwrite_output_type, overwrite=True)
		self.output_session.set_data(self.output_key, resolved_input)


class TaskSession(session.Session):
	"""
	Session subclass that wraps a regular session but handles resources in a special manner.
	"""
	def __init__(self, session: session.Session, unsafe: bool = False) -> None:
		super().__init__(session.ns, unsafe=unsafe)
		self.session = session
		self.tasks = {}
		self.pm.register(self)

	@st.hookimpl
	def before_set(self, key: str, value: Any) -> Tuple[Any, types.Type]:
		if not isinstance(value, BoundTaskSpec):
			return None
		self.tasks[key] = bound = value.bind(self)
		return bound.output_future, bound.output_future.type

	def resolve(self, symbol: symbols.Symbol, allow_unknowns: bool = False, decode: bool = True) -> Any:
		return self.session.resolve(symbol, allow_unknowns, decode)

	def set_data(self, key: str, data: Any) -> None:
		return self.session.set_data(key, data)

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
