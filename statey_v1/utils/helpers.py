"""
Helper functions for use in statey
"""
import sys
from contextlib import contextmanager
from typing import Any, Optional, Callable, Type, Iterator, ContextManager, Dict

import marshmallow as ma
import networkx as nx
from networkx.algorithms.cycles import find_cycle
from networkx.exception import NetworkXNoCycle

from statey import exc


# Simplify python3.6->python3.7+ compatability by wrapping import(s) here
try:
    # pylint: disable=unused-import,ungrouped-imports
    from contextlib import asynccontextmanager
except ImportError:
    from async_generator import asynccontextmanager


# pylint: disable=too-few-public-methods
class NamedObject:
    """
    Simple object that allows a name to be passed to be used in `__repr__`
    """

    def __init__(self, name: str) -> None:
        self.name = name

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.name})"


def validate_no_value(reason: Optional[str] = None) -> Callable[[Any], None]:
    """
	Marshmallow validator that indicates no input value is accepted
	for a field, for example a computed field
	"""
    txt = "No input value is accepted."
    if reason is not None:
        txt = f"No input value is accepted: {reason}."

    def validator(value: Any) -> None:
        raise ma.ValidationError(txt)

    return validator


def get_all_subclasses(cls: Type, depth_first: bool = False) -> Iterator[Type]:
    """
	Recursively retrieve all subclasses of the given class
	"""
    if not depth_first:
        yield cls
    for subcls in cls.__subclasses__():
        yield from get_all_subclasses(subcls)
    if depth_first:
        yield cls


def truncate_string(value: str, size: int = 1000, suffix: str = "...") -> str:
    """
    Truncate a string to maximum value
    """
    return value[: size - len(suffix)] + suffix if len(value) > size else value


def detect_circular_references(
    graph: nx.MultiDiGraph, key: Callable[[Any], str] = str
) -> None:
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
            symbols.append(key(from_node))
        symbols.append(key(to_node))

    raise exc.CircularGraphError(symbols)


@contextmanager
def context_scope() -> ContextManager[Dict[str, Any]]:
    """
    Context manager that captures declared names in the local scope below
    the context manager. This dictionary will include the scope itself under
    whatever name it is given--this function is called before the value
    is bound to a name (if it is at all), and excluding the scope from the
    resulting local variables explicitly would be less transparent and could
    introduce bugs to client code.

    e.g.
    b = 2
    with context_scope() as scope:
        a = 1
    c = 3

    assert scope == {'a': 1, 'scope': scope}
    """
    frame = sys._getframe(2)  # pylint: disable=protected-access
    locals_before = frame.f_locals.copy()
    scope = {}

    try:
        yield scope
    finally:
        locals_after = frame.f_locals.copy()

        vars_set = set(locals_after) - set(locals_before)

        for key in set(locals_after) & set(locals_before):
            value1, value2 = locals_before[key], locals_after[key]
            if value1 is not value2:
                vars_set.add(key)

        scope.update({key: locals_after[key] for key in vars_set})
