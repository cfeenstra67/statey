"""
Helper functions for use in statey
"""
from typing import Any, Optional, Callable, Type, Iterator

import marshmallow as ma


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


def get_all_subclasses(cls: Type) -> Iterator[Type]:
    """
	Recursively retrieve all subclasses of the given class
	"""
    yield cls
    for subcls in cls.__subclasses__():
        yield from get_all_subclasses(subcls)
