# import abc
# import dataclasses as dc
# from typing import Optional, Callable

# from statey.plan import Plan
# from statey.resource import ResourceGraph, ResourceSession


# class ModuleBase(abc.ABC):
#     """

#     """
#     @abc.abstractmethod
#     async def plan(self, state: str, graph: ResourceGraph) -> Plan:
#         """

#         """
#         raise NotImplementedError


# @dc.dataclass(frozen=True)
# class ModuleState:
#     """

#     """
#     name: str
#     factory: Callable[[ResourceSession], None]
#     from_state: Optional[str] = None

#     def __post_init__(self) -> None:
#         if self.from_state is None:
#             self.__dict__['from_state'] = 'DOWN'


# class ModuleMeta(type):
#     """

#     """
#     @classmethod
#     def _validate_states(
#         cls, old_states: Sequence[ModuleState], new_states: Sequence[ModuleState]
#     ) -> Sequence[ModuleState]:

#         all_names = {state.name for state in old_states} | {state.name for state in new_states}
#         for state in new_states:
#             if state.from_state != "DOWN" and state.from_state not in all_names:
#                 raise ValueError(f'Unknown "from_state": {state.from_state}')

#         new_names = Counter((state.name, state.from_state) for state in new_states)
#         state_map = {(state.name, state.from_state): state for state in new_states}
#         if new_names and max(new_names.values()) > 1:
#             multi = [(state_map[k], v) for k, v in new_names.items() if v > 1]
#             raise ValueError(f"Duplicate states found: {multi}")

#         old_states = [state for state in old_states if (state.name, state.from_state) not in new_names]
#         return old_states + list(new_states)

#     def __new__(
#         cls, name: str, bases: Sequence[PyType], attrs: Dict[str, Any]
#     ) -> PyType:
#         super_cls = super().__new__(cls, name, bases, attrs)
#         states = super_cls.__states__ if hasattr(super_cls, "__states__") else ()
#         new_states = [val for val in attrs.values() if isinstance(val, ModuleState)]
#         states = cls._validate_states(states, new_states)
#         super_cls.__states__ = tuple(states)

#         return super_cls


# class Module(ModuleBase, metaclass=ModuleMeta):
#     """

#     """
#     async def plan(self, state: str, graph: ResourceGraph) -> Plan:


# # from statey.fsm import Machine
# # from statey.resource import AbstractState, State


# #     name: str
# #     input_type: types.Type
# #     output_type: types.Type
# #     null: bool


# # # import statey as st


# # # @dc.dataclass(frozen=True)
# # # class ModuleState(abc.ABC):
# # # 	"""
# # # 	Describes a single possible configuration for a single module
# # # 	"""
# # # 	input_type: types.Type
# # # 	output_type: types.Type

# # # 	@abc.abstractmethod
# # # 	def main(self, session: ResourceSession, input_ref: Object) -> Object:
# # # 		"""
# # # 		Populate the given resource session for this state and return a symbol of type
# # # 		self.output_type that will contain the output symbol
# # # 		"""
# # # 		raise NotImplementedError


# # # @dc.dataclass(frozen=True)
# # # class
