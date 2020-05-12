from typing import Any, Optional

import marshmallow as ma


class SymsError(Exception):
	"""
	Base class for errors in the syms package
	"""


class SymsTypeError(SymsError, TypeError):
	"""
	TypeError base class for type-related errors
	"""


class NoEncoderFound(SymsTypeError):
	"""
	Error to indicate we could not find an encoder for some type
	"""
	def __init__(self, type: 'Type') -> None:
		self.type = type
		super().__init__(f'No encoder found for type: {type}.')


class NamespaceError(SymsError):
	"""
	Error raised from a namespace
	"""


class DuplicateSymbolKey(NamespaceError):
	"""
	Error class indicating that we tried to insert a duplicate key into a session
	"""
	def __init__(self, key: str, ns: 'Namespace') -> None:
		self.key = key
		self.ns = ns
		super().__init__(f'Attempted to insert duplicate key "{key}" into namespace {ns}.')


class SymbolKeyError(NamespaceError, KeyError):
	"""
	Error raised by a session to indicate a requested key cannot be resolved
	"""
	def __init__(self, key: str, ns: 'Namespace') -> None:
		self.key = key
		self.ns = ns
		super().__init__(f'Key "{key}" not found in namespace {ns}.')


class SessionError(SymsError):
	"""
	Error raised from a session
	"""


class MissingDataError(SessionError):
	"""
	Error indicating that a symbol cannot be resolved because data is missing
	"""
	def __init__(self, key: str, type: 'Type', session: 'Session') -> None:
		self.key = key
		self.type = type
		self.session = session
		super().__init__(f'Key "{key}" has not been set in {session} (type: {type.name}).')


class InputValidationError(ma.ValidationError, SymsError):
	"""
	Wrapper for a marshmallow validation error, reraise it as a syms exception
	"""


class NonEncodeableTypeError(SymsTypeError):
	"""
	Raise to indicate that we tried to encode a non-encodeable value
	"""
	def __init__(self, type: 'Type') -> None:
		self.type = type
		super().__init__(f'Encountered non-serializable type: {type} while attempting to serialize or deserialize.')
