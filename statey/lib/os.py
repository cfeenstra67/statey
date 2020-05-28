import os
import dataclasses as dc
import tempfile
import zipfile
from functools import partial
from typing import Dict, Any

import statey as st
from statey.resource import Resource, KnownStates, State, NullState, BoundState
from statey.syms import symbols, types, utils
from statey.syms.api import struct
from statey.task import TaskSession, FunctionTaskSpec


@struct
@dc.dataclass(frozen=True)
class FileSpec(utils.Cloneable):
	"""
	Contains configuration info about a file
	"""
	location: str
	data: str


class FileResource(Resource):
	"""
	Represents a file on the file system.
	"""
	class States(KnownStates):
		UP = State('UP', st.registry.get_type(FileSpec))
		DOWN = NullState('DOWN')

	def __init__(self, name: str) -> None:
		self._name = name
		super().__init__()

	@property
	def name(self) -> str:
		return self._name

	async def remove_file(self, path: str) -> Any:
		"""
		Delete the given file
		"""
		os.remove(path)
		return {}

	async def set_file(self, data: Dict[str, Any]) -> Any:
		"""
		Delete the given file
		"""
		path = os.path.realpath(data['location'])
		with open(path, 'w+') as f:
			f.write(data['data'])
		return dict(data, location=path)

	async def refresh(self, current: BoundState) -> BoundState:
		state = current.resource_state.state
		if state == self.s.null_state.state:
			return current
		out = current.data.copy()
		if not os.path.isfile(current.data['location']):
			return BoundState(self.s.null_state, {})
		out['location'] = os.path.realpath(out['location'])
		with open(out['location']) as f:
			out['data'] = f.read()
		return BoundState(current.resource_state, out)

	def plan(
		self,
		current: BoundState,
		config: BoundState,
		session: TaskSession,
		input: symbols.Symbol
	) -> symbols.Symbol:

		current_state = current.resource_state.state
		current_data = current.data
		current_literal = current.literal(session.ns.registry)

		config_state = config.resource_state.state
		config_data = config.data
		config_literal = current.literal(session.ns.registry)

		if config_state.name == 'UP' and not isinstance(config_data['location'], symbols.Unknown):
			config_data['location'] = os.path.realpath(config_data['location'])

		# No change, just return the input ref (which will be of the correct type).
		# Also, because `current` will always be fully resolved we don't have to
		# worry too much about a very deep '==' comparison
		if (
			current_state == config_state
			and current_data == config_data
		):
			return current_literal

		delete_file = lambda **kwargs: FunctionTaskSpec(
			input_type=types.StringType(False),
			output_type=types.EmptyType,
			func=self.remove_file,
			**kwargs
		)
		set_file = lambda **kwargs: FunctionTaskSpec(
			input_type=self.s.UP.state.type,
			output_type=self.s.UP.state.type,
			func=self.set_file,
			**kwargs
		)

		def join(x, *args):
			return symbols.Function(
				func=lambda x, *args: x,
				args=(x, *args),
				semantics=x.semantics
			)

		# UP -> DOWN
		if config_state == self.s.null_state.state:
			return session['delete_file'] << delete_file(expected=config_data)(current_literal.location)

		# DOWN -> UP
		if current_state == self.s.null_state.state:
			return session['create_file'] << set_file(expected=config_data)(input)

		# UP -> UP if data different
		if current_data['location'] == config_data['location']:
			return session['update_file'] << set_file(expected=config_data)(input)

		rm_file = session['delete_file'] << delete_file()(current_literal.location)
		return session['create_file'] << set_file(expected=config_data)(join(input, rm_file))


# Declaring global resources

file_resource = FileResource('file')

# Resource state factory
File = file_resource.s


RESOURCES = [
	file_resource
]


def register() -> None:
	"""
	Register default resources in this module
	"""
	for resource in RESOURCES:
		st.registry.register_resource(resource)
