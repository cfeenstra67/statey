import marshmallow as ma


class StateyError(Exception):
	"""
	Base class for statey errors
	"""


class JobAborted(StateyError):
	"""
	Error raised to indicate that tasks will not be performed because a job has been aborted
	"""


class InitializationError(StateyError):
	"""
	Error type indicating that an exception was encountered during
	initialization of an object, possibly because of some validation
	logic
	"""


class InvalidSchema(InitializationError):
	"""
	Error class indicating that an error was raised because of a validation
	failure relating to a schema
	"""


class InvalidField(InitializationError):
	"""
	Error class indicating that an erro was raised because of a validation
	failure relating to a field
	"""


class InputValidationError(StateyError, ma.ValidationError):
	"""
	Error class for errors in input values
	"""


class SymbolError(StateyError):
	"""
	Errors relating to statey symbols
	"""


class ResolutionError(StateyError):
	"""
	Error in resolving a symbol value
	"""


class MissingReturnType(SymbolError):
	"""
	Error indicating that no return value was provided for a Func object and none
	was able to be inferred.
	"""

class SymbolTypeError(SymbolError, TypeError):
	"""
	Error indicating that a typeerror was raised during symbol analysis
	"""


class SessionError(StateyError):
	"""
	Errors related to sessions
	"""


class ForeignGraphError(SessionError):
	"""
	Error indicating that a session operation was attempted on a graph constructed
	using a different session
	"""


class UnnamedResourceError(SessionError):
	"""
	Error indicating that a resource that we attempted to get a path
	for through the session does not have a name
	"""


class GraphError(SessionError):
	"""
	Errors related to graphs
	"""


class GraphIntegrityError(GraphError):
	"""
	Error indicating that an operation was attempted that somehow violates
	the integrity of a graph
	"""


class InvalidReference(GraphIntegrityError):
	"""
	Error indicating that a reference found when building a graph is invalid
	"""


class PlanError(StateyError):
	"""
	Errors relating to plans
	"""


class MissingResourceError(PlanError):
	"""
	Error indicating a resource configuration could not be found for a path
	"""


class UndefinedResourceType(StateyError):
	"""
	Error indicating a Resource class can't be found for some type name
	"""
