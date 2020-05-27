# import dataclasses as dc

# from statey.resource import Resource, ResourceState, ResourceSession, NullState
# from statey.syms import types, symbols


# @dc.dataclass(frozen=True)
# class ModuleState(abc.ABC):
# 	"""
# 	Describes a single possible configuration for a single module
# 	"""
# 	input_type: types.Type
# 	output_type: types.Type

# 	@abc.abstractmethod
# 	def main(self, session: ResourceSession, input_ref: symbols.Symbol) -> symbols.Symbol:
# 		"""
# 		Populate the given resource session for this state and return a symbol of type
# 		self.output_type that will contain the output symbol
# 		"""
# 		raise NotImplementedError


# @dc.dataclass(frozen=True)
# class Module(Resource):
# 	"""
# 	A module basically contains a state in itself--any statey resource session
# 	can be converted to a module.
# 	"""
# 	name: str
# 	states: Sequence[ModuleState]

# 	@property
# 	def null_state(self) -> ResourceState:
# 		state = NullState('DOWN')
# 		return ResourceState(state, self.name)

# 	@abc.abstractmethod
# 	def plan(
# 		self,
# 		current: BoundState,
# 		config: BoundState,
# 		session: TaskSession,
# 		input: symbols.Symbol
# 	) -> symbols.Symbol:
# 		"""
# 		Given a task session, the current state of a resource, and a task session with
# 		corresponding input reference, return an output reference that can be fully
# 		resolved when all the tasks in the task session have been complete successfully.
# 		"""
# 		raise NotImplementedError


