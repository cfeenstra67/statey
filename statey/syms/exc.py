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


class NoTypeFound(SymsTypeError):
	"""
	Error to indicate we could not find a type for some annotation
	"""
	def __init__(self, annotation: Any) -> None:
		self.annotation = annotation
		super().__init__(f'No type found for annotation: {annotation}.')


class NoEncoderFound(SymsTypeError):
	"""
	Error to indicate we could not find an encoder for some type
	"""
	def __init__(self, type: 'Type') -> None:
		self.type = type
		super().__init__(f'No encoder found for type: {type}.')


class NoSemanticsFound(SymsTypeError):
	"""
	Error to indicate we could not find semantics for some type
	"""
	def __init__(self, type: 'Type') -> None:
		self.type = type
		super().__init__(f'No semantics found for type: {type}.')


class NoResourceFound(SymsError):
	"""
	Error to indicate no resource could be found for a given name
	"""
	def __init__(self, resource_name: str) -> None:
		self.resource_name = resource_name
		super().__init__(f'No resource registered for name: {resource_name}.')


class NoTypeSerializerFound(SymsError):
	"""
	Base class for NoTypeSerializerFound errors
	"""


class NoTypeSerializerFoundForType(NoTypeSerializerFound):
	"""
	Error to indicate no type serializer could be found for a given name
	"""
	def __init__(self, type: 'Type') -> None:
		self.type = type
		super().__init__(f'No resource registered for type: {type}.')


class NoTypeSerializerFoundForData(NoTypeSerializerFound):
	"""
	Error to indicate no type serializer could be found for a given name
	"""
	def __init__(self, data: Any) -> None:
		self.data = data
		super().__init__(f'No resource registered for data: {data}.')


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


class SymbolAttributeError(SymsError, AttributeError):
	"""
	Error raised to indicate that an attribute reference cannot be resolved on a symbol
	"""
	def __init__(self, symbol: 'Symbol', attr: Any) -> None:
		self.symbol = symbol
		self.attr = attr
		super().__init__(f'Could not resolve attribute "{attr}" of symbol {symbol}.')


class FutureError(SymsError):
	"""
	symbols.Future-related errors
	"""


class FutureResultNotSet(FutureError):
	"""
	Error indicating that get_result() was called on a future whose
	result was not set yet
	"""
	def __init__(self, future: 'Future') -> None:
		self.future = future
		super().__init__(f'Result has not yet been set for future: {future}.')


class FutureResultAlreadySet(FutureError):
	"""
	Error indicating that get_result() was called on a future whose
	result was not set yet
	"""
	def __init__(self, future: 'Future') -> None:
		self.future = future
		super().__init__(f'Result has already been set for future: {future} as {future.get_result()}.')


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
