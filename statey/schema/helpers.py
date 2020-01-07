"""
Helper methods used in the statey.schema package
"""
from functools import wraps
from typing import Any, Dict, Tuple, Callable, Optional, Union

import marshmallow as ma
import networkx as nx

from statey import exc
from statey.utils.helpers import detect_circular_references


def is_optional(annotation: Any) -> bool:
    """
	Determine if the given annotation is an Optional field
	"""
    return (
        hasattr(annotation, "__origin__")
        and annotation.__origin__ is Union
        and getattr(annotation, "__args__", None) is not None
        and len(annotation.__args__) == 2
        and annotation.__args__[-1] is type(None)
    )


def extract_modifiers(annotation: Any) -> Tuple[Any, Dict[str, Any]]:
    """
	Extract any modifiers on an annotation, such as Optional[...]. Return
	the annotation without modifiers as well as any extracted modifiers
	"""
    modifiers = {}

    if is_optional(annotation):
        modifiers["optional"] = True
        annotation = annotation.__args__[0]

    return annotation, modifiers


def validate_no_input(reason: Optional[str] = None,) -> Callable[[], Callable[[Any], None]]:
    """
	Validate that a marshmallow field does not contain any input
	"""
    if reason is None:
        reason = "This field does not accept input."

    def validate(value):
        if value is not None:
            raise ma.ValidationError(reason)

    return validate


def convert_ma_validation_error(func: Callable) -> Callable:
    """
	Wrap the given function so that any ma.ValidationErrors raised
	will be converted to exc.InputValidationError s
	"""

    @wraps(func)
    def wrapped(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ma.ValidationError as error:
            raise exc.InputValidationError(error.messages) from error

    return wrapped


def detect_circular_symbol_references(graph: nx.MultiDiGraph) -> None:
    """
    Check the given compute graph for any circular references, which will cause
    a infinite recursion error otherwise
    """

    def _key(path):
        return graph.nodes[path]["obj"]

    try:
        detect_circular_references(graph, _key)
    except exc.CircularGraphError as err:
        raise exc.CircularReferenceDetected(err.nodes) from err
