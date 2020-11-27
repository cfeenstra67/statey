from typing import Any, Optional, Sequence, Dict

import marshmallow as ma

from statey.syms import utils


class StateyError(Exception):
    """
    Base class for statey errors
    """


class PlanError(StateyError):
    """
    Statey error during planning
    """


class NullRequired(PlanError):
    """
    Error to raise indicating that we need to recreate the planning given resource
    """


class NotADagError(StateyError):
    """
    Error indicating that some graph is not a DAG
    """

    def __init__(self, graph: "DiGraph", path: Sequence[str]) -> None:
        self.graph = graph
        self.path = path
        super().__init__(
            f'Graph {repr(graph)} is not a DAG! Cycle found: {" -> ".join(path)}.'
        )


class ErrorDuringPlanning(PlanError):
    """
    Generic error wrapping during plan()
    """

    def __init__(self, exception: Exception, tb) -> None:
        self.exception = exception
        self.tb = tb
        super().__init__(
            f"Exception raised during planning: {type(exception).__name__}:"
            f" {exception}."
        )


class ErrorDuringPlanningNode(PlanError):
    """
    Error to indicate that an error was raised while calling a resource's
    plan() method
    """

    def __init__(
        self, path: str, current: Any, config: Any, exception: Exception, tb
    ) -> None:
        self.path = path
        self.current = current
        self.config = config
        self.exception = exception
        self.tb = tb
        super().__init__(
            f"Exception raised during planning {current.state.resource}[{path}]"
            f" {current.state.name} => {config.state.name}: "
            f"{type(exception).__name__}: {exception}"
        )


class SymsError(StateyError):
    """
	Base class for errors in the syms package
	"""


class SymsTypeError(SymsError, TypeError):
    """
	TypeError base class for type-related errors
	"""


class NotFoundError(SymsTypeError):
    """
    Base class for errors indicating some object can't be found
    """


class NoTypeFound(NotFoundError):
    """
	Error to indicate we could not find a type for some annotation
	"""

    def __init__(self, annotation: Any) -> None:
        self.annotation = annotation
        super().__init__(f"No type found for annotation: {annotation}.")


class NoEncoderFound(NotFoundError):
    """
	Error to indicate we could not find an encoder for some type
	"""

    def __init__(self, type: "Type", serializable: bool = False) -> None:
        self.type = type
        super().__init__(
            f"No encoder found for type: {type} (serializable={serializable})."
        )


class NoSemanticsFound(NotFoundError):
    """
	Error to indicate we could not find semantics for some type
	"""

    def __init__(self, type: "Type") -> None:
        self.type = type
        super().__init__(f"No semantics found for type: {type}.")


class NoResourceFound(NotFoundError):
    """
	Error to indicate no resource could be found for a given name
	"""

    def __init__(self, resource_name: str) -> None:
        self.resource_name = resource_name
        super().__init__(f"No resource registered for name: {resource_name}.")


class NoDifferFound(NotFoundError):
    """
    Error to indicate we could not find a differ for some type
    """

    def __init__(self, type: "Type") -> None:
        self.type = type
        super().__init__(f"No differ found for type: {type}.")


class NoObjectFound(NotFoundError):
    """
    Error indicating we could not create an object from an arbitrary value
    """

    def __init__(self, value: Any) -> None:
        self.value = value
        super().__init__(f"Unable to create an object from {value}.")


class NoCasterFound(NotFoundError):
    """
    Error indicating we could not create an object from an arbitrary value
    """

    def __init__(self, from_type: "Type", to_type: "Type") -> None:
        self.from_type = from_type
        self.to_type = to_type
        super().__init__(f"Unable to find caster from {from_type} to {to_type}.")


class NoTypeSerializerFound(NotFoundError):
    """
	Base class for NoTypeSerializerFound errors
	"""


class NoTypeSerializerFoundForType(NoTypeSerializerFound):
    """
	Error to indicate no type serializer could be found for a given name
	"""

    def __init__(self, type: "Type") -> None:
        self.type = type
        super().__init__(f"No type serializer registered for type: {type}.")


class NoTypeSerializerFoundForData(NoTypeSerializerFound):
    """
	Error to indicate no type serializer could be found for a given name
	"""

    def __init__(self, data: Any) -> None:
        self.data = data
        super().__init__(f"No type serializer registered for data: {data}.")


class NoObjectImplementationSerializerFound(NotFoundError):
    """
    Error indicating that an ObjectImplementation serializer couldn't
    be found
    """


class NoObjectImplementationSerializerFoundForImpl(
    NoObjectImplementationSerializerFound
):
    """
    Error for the get_impl_serializer() method
    """

    def __init__(self, impl: "ObjectImplementation", type: "Type") -> None:
        self.impl = impl
        self.type = type
        super().__init__(
            f"No serializer found for object implementation: {impl}, type: {type}"
        )


class NoObjectImplementationSerializerFoundForData(
    NoObjectImplementationSerializerFound
):
    """
    Error for the get_impl_serializer_from_data() method
    """

    def __init__(self, data: Any) -> None:
        self.data = data
        super().__init__(f"No object implementation serializer found for data: {data}")


class NoObjectSerializerFound(NotFoundError):
    """
    Error indicating a serializer could not be found for an object
    """


class NoObjectSerializerFoundForObject(NoObjectSerializerFound):
    """
    Error for the get_object_serializer() method
    """

    def __init__(self, obj: "Object") -> None:
        self.obj = obj
        super().__init__(f"No serializer found for object: {obj}")


class NoObjectSerializerFoundForData(NoObjectSerializerFound):
    """
    Error for the get_object_serializer_from_data() method
    """

    def __init__(self, data: Any) -> None:
        self.data = data
        super().__init__(f"No object serializer found for data: {data}")


class NoSessionSerializerFound(NotFoundError):
    """
    Error indicating a serializer could not be found for a session
    """


class NoSessionSerializerFoundForSession(NoSessionSerializerFound):
    """
    Error for the get_session_serializer() method
    """

    def __init__(self, session: "Session") -> None:
        self.session = session
        super().__init__(f"No serializer found for session: {session}")


class NoSessionSerializerFoundForData(NoSessionSerializerFound):
    """
    Error for the get_session_serializer_from_data() method
    """

    def __init__(self, data: Any) -> None:
        self.data = data
        super().__init__(f"No session serializer found for data: {data}")


class NoNamespaceSerializerFound(NotFoundError):
    """
    Error indicating a serializer could not be found for a namespace
    """


class NoNamespaceSerializerFoundForNamespace(NoNamespaceSerializerFound):
    """
    Error for the get_namespace_serializer() method
    """

    def __init__(self, ns: "Namespace") -> None:
        self.ns = ns
        super().__init__(f"No serializer found for namespace: {ns}")


class NoNamespaceSerializerFoundForData(NoNamespaceSerializerFound):
    """
    Error for the get_namespace_serializer_from_data() method
    """

    def __init__(self, data: Any) -> None:
        self.data = data
        super().__init__(f"No namespace serializer found for data: {data}")


class NoProviderFound(NotFoundError):
    """
    Error indicating we could not get a provider for the given params.
    """

    def __init__(self, name: str, params: Dict[str, Any]) -> None:
        self.name = name
        self.params = params
        super().__init__(
            f"Unable to find provider with name {name} and params {params}."
        )


class NoStateManagerFound(NotFoundError):
    """
    Error indicating we could not get a state manager for the given params.
    """

    def __init__(self) -> None:
        super().__init__("Unable to load a state manager for the current module.")


class NoPluginInstallerFound(NotFoundError):
    """
    Error indicating we could not get a plugin installer for the given name
    """

    def __init__(self, name: str) -> None:
        super().__init__(f"Unable to load a plugin installer for plugin: {name}")


class NamespaceError(SymsError):
    """
	Error raised from a namespace
	"""


class DuplicateSymbolKey(NamespaceError):
    """
	Error class indicating that we tried to insert a duplicate key into a session
	"""

    def __init__(self, key: str, ns: "Namespace") -> None:
        self.key = key
        self.ns = ns
        super().__init__(
            f'Attempted to insert duplicate key "{key}" into namespace {ns}.'
        )


class SymbolKeyError(NamespaceError, KeyError):
    """
	Error raised by a session to indicate a requested key cannot be resolved
	"""

    def __init__(self, key: str, ns: "Namespace") -> None:
        self.key = key
        self.ns = ns
        super().__init__(f'Key "{key}" not found in namespace {ns}.')


class SymbolAttributeError(SymsError, AttributeError):
    """
	Error raised to indicate that an attribute reference cannot be resolved on a symbol
	"""

    def __init__(self, value: Any, attr: Any) -> None:
        self.value = value
        self.attr = attr
        super().__init__(f'Could not resolve attribute "{attr}" of value {value}.')


class NoSuchMethodError(SymsError):
    """
    Error to indicate a method could not be found
    """

    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(f"Unable to find method: {name}")


class FutureError(SymsError):
    """
	impl.Future-related errors
	"""


class FutureResultNotSet(FutureError):
    """
	Error indicating that get_result() was called on a future whose
	result was not set yet
	"""

    def __init__(self, future: "Future") -> None:
        self.future = future
        super().__init__(f"Result has not yet been set for future: {future}.")


class FutureResultAlreadySet(FutureError):
    """
	Error indicating that get_result() was called on a future whose
	result was not set yet
	"""

    def __init__(self, future_or_result: Any) -> None:
        from statey.syms.impl import Future, FutureResult

        self.future_or_result = future_or_result

        if isinstance(future_or_result, FutureResult):
            msg = f"Result has already been set: {future_or_result}."
        elif isinstance(future_or_result, Future):
            future = future_or_result
            msg = f"Result has already been set for future: {future} as {future.get_result()}."
        else:
            msg = f"Result has already been set."

        super().__init__(msg)


class SessionError(SymsError):
    """
	Error raised from a session
	"""


class ResolutionError(SessionError):
    """
    Error indicating that some exception was encountered during resolution
    """

    def __init__(self, stack: "ResolutionStack", exception: Exception, tb) -> None:
        self.stack = stack
        self.exception = exception
        self.tb = tb
        obj = self.stack.get_object(self.stack.resolving_obj_id)
        super().__init__(
            f"Encountered exception while resolving {obj}: "
            f"{type(exception).__name__}: {exception}."
        )


class InvalidDataError(SessionError):
    """
    Error indicating that there was an attempt to put invalid data into a session
    """

    def __init__(self, key: str, exception: Exception) -> None:
        self.key = key
        self.exception = exception
        super().__init__(
            f"Encountered error while inserting data into key {key}: "
            f"{type(exception).__name__}: {exception}"
        )


class UnknownError(SessionError):
    """
    Error to short-circuit resolution when an unknown value is encountered
    """

    def __init__(
        self, refs: Sequence["Symbol"] = (), expected: Any = utils.MISSING
    ) -> None:
        self.refs = refs
        self.expected = expected
        super().__init__(f"Unknowns found, refs: {refs}; expected: {expected}")


class MissingDataError(SessionError):
    """
	Error indicating that a symbol cannot be resolved because data is missing
	"""

    def __init__(self, key: str, type: "Type", session: "Session") -> None:
        self.key = key
        self.type = type
        self.session = session
        super().__init__(
            f'Key "{key}" has not been set in {session} (type: {type.name}).'
        )


class InputValidationError(ma.ValidationError, SymsError):
    """
	Wrapper for a marshmallow validation error, reraise it as a syms exception
	"""


class NonEncodeableTypeError(SymsTypeError):
    """
	Raise to indicate that we tried to encode a non-encodeable value
	"""

    def __init__(self, type: "Type") -> None:
        self.type = type
        super().__init__(
            f"Encountered non-serializable type: {type} while attempting to serialize or deserialize."
        )


class ResourceError(StateyError):
    """
    Error indicating some issue with a resource or resource code
    """


class InvalidModificationAction(ResourceError):
    """
    Error indicating that get_action() returned an invalid modification action
    in a SimpleMachine
    """

    def __init__(self, modification_type: "ModificationAction") -> None:
        self.modification_type = modification_type
        super().__init__(
            f"Encountered unhandled modification action {modification_type}."
        )


class ExecutionError(StateyError):
    """
    Error to raise indicating that an execution attempt was not successful
    """

    def __init__(self, exec_info: "ExecutionInfo") -> None:
        from statey.task import TaskStatus

        self.exec_info = exec_info
        tasks_by_status = exec_info.tasks_by_status()

        traces = {}
        for key in tasks_by_status.get(TaskStatus.FAILED, []):
            info = exec_info.task_graph.get_info(key)
            if info.error is not None:
                traces[key] = info.error

        trace_lines = []
        for key, error in traces.items():
            trace_lines.append(f"{key}: {error.format_error_message()}")

        trace_lines.insert(0, "")
        trace_str = "\n".join(trace_lines)

        super().__init__(
            f"Error encountered during execution of a task graph; tasks "
            f"by status: {tasks_by_status}. Errors:{trace_str}"
        )


class ProviderError(StateyError):
    """
    Errors relating to providers.
    """


class ResourceNotFound(ProviderError):
    """
    Error indicating a resource could not be found
    """

    def __init__(self, name: str, provider: "Provider") -> None:
        self.name = name
        self.provider = provider
        super().__init__(f'Provider {provider} could not locate resource "{name}".')


class TaskNotFound(ProviderError):
    """
    Error indicating a task could not be found
    """

    def __init__(self, name: str, provider: "Provider") -> None:
        self.name = name
        self.provider = provider
        super().__init__(f'Provider {provider} could not locate task "{name}".')
