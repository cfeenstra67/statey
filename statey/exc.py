"""
Exception classes for use in the Statey framework
"""
import asyncio
from typing import Sequence, Hashable, Any, Coroutine, Optional, Tuple

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


class ReservedFieldName(InvalidField):
    """
    Error indicating that the given field name is reserved
    """

    def __init__(self, field_name: str, schema_name: str) -> None:
        self.field_name = field_name
        self.schema_name = schema_name
        super().__init__(
            f"Field name {field_name} in schema {schema_name} is reserved. You cannot "
            f"name a field that. (check statey.schema.RESERVED_NAMES for the list "
            f"of reserved names). Overridding the Schema.__fields__ property of "
            f"your schema class to a dictionary of name -> field mappings can be"
            f"used as a workaround."
        )


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


class StateError(StateyError):
    """
    Errors related to states
    """


class ForeignGraphError(StateError):
    """
    Error indicating that a state operation was attempted on a graph constructed
    using a different state
    """


class UnnamedResourceError(StateError):
    """
    Error indicating that a resource that we attempted to get a path
    for through the state does not have a name
    """


class NullStateError(StateError):
    """
    Error indicating that we encountered a null snapshot while deserializing a state
    """

    def __init__(self, path: str) -> None:
        self.path = path
        super().__init__(f"Null state encountered at path: {path}.")


class GraphError(StateyError):
    """
    Errors related to graphs
    """


class GraphIntegrityError(GraphError):
    """
    Error indicating that an operation was attempted that somehow violates
    the integrity of a graph
    """


class CircularGraphError(GraphIntegrityError):
    """
    Error indicating that a circular reference was detected in a some graph
    """

    def __init__(self, nodes: Sequence[Any]) -> None:
        self.nodes = nodes
        path = " -> ".join(map(str, nodes)) + " \u21ba"
        super().__init__(f"Circular path detected: Path: {path}")


# pylint: disable=too-many-ancestors
class CircularReferenceDetected(CircularGraphError, ResolutionError):
    """
    Error indicating that a circular reference was detected in a compute graph
    """


class InvalidReference(GraphIntegrityError, ResolutionError):
    """
    Error indicating that a reference found when building a graph is invalid
    """

    def __init__(
        self,
        path: str,
        field_name: Optional[str] = None,
        nested_path: Optional[Tuple[str, ...]] = None,
    ) -> None:
        self.path = path
        self.field_name = field_name
        msg = f"Invalid reference found in graph: Path: {path}."
        if field_name is not None:
            joined_nested = nested_path or ".".join(nested_path)
            full_name = ".".join(filter(None, [joined_nested, field_name]))
            msg += f"Field: {full_name}."
        super().__init__(msg)


class PlanError(StateyError):
    """
    Errors relating to plans
    """


class MissingResourceError(PlanError):
    """
    Error indicating a resource configuration could not be found for a path
    """


class TaskGraphError(StateyError):
    """
    Errors relating to building or executing task graphs
    """


class TaskAlreadyScheduled(TaskGraphError):
    """
    Error indicating that we attempted to schedule a task that had already
    been scheduled
    """

    def __init__(self, key: Hashable, task: asyncio.Task) -> None:
        self.key = key
        self.task = task
        super().__init__(f"Task already scheduled for key {key}: {task}")


class TaskLost(TaskGraphError):
    """
    Error indicating that a task in a task graph was neither skipped nor processed.
    """

    def __init__(self, path: str, coro: Coroutine, parents: Sequence[str] = ()) -> None:
        self.path = path
        self.coro = coro
        self.parents = parents
        super().__init__(
            f"Lost task at path {path} in the task graph (i.e. it was neither "
            f"processed nor skipped after all tasks have been awaited). Coroutine: "
            f" {coro}. Parents: {parents}."
        )


class TaskStillRunning(TaskGraphError):
    """
    Error indicating that a task is still marked as running when all tasks should be
    complete.
    """

    def __init__(self, path: str, task: asyncio.Task) -> None:
        self.path = path
        self.task = task
        super().__init__(
            f"Task at path {path} is still running after all tasks have been awaited: {task}."
        )


class UndefinedResourceType(StateyError):
    """
    Error indicating a Resource class can't be found for some type name
    """
