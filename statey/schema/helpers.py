"""
Helper methods used in the statey.schema package
"""
from functools import wraps
from typing import Any, Dict, Tuple, Callable, Optional

import marshmallow as ma
import networkx as nx
from networkx.algorithms.cycles import find_cycle
from networkx.exception import NetworkXNoCycle

from statey import exc


def is_optional(annotation: Any) -> bool:
    """
	Determine if the given annotation is an Optional field
	"""
    return (
        hasattr(annotation, "__args__")
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


def detect_circular_references(graph: nx.MultiDiGraph) -> None:
    """
    Check the given compute graph for any circular references, which will cause
    a infinite recursion error otherwise
    """
    try:
        cycle = find_cycle(graph)
    except NetworkXNoCycle:
        return

    symbols = []
    for idx, (from_node, to_node, _) in enumerate(cycle):
        if idx == 0:
            from_symbol = graph.nodes[from_node]["obj"]
            symbols.append(from_symbol)
        to_symbol = graph.nodes[to_node]["obj"]
        symbols.append(to_symbol)

    raise exc.CircularReferenceDetected(symbols)
