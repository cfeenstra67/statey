import abc
import enum
import dataclasses as dc
from typing import (
    Sequence,
    Any,
    Optional,
)

import pluggy

from statey import create_plugin_manager


def create_type_plugin_manager():
    """
	Factory function for plugin managers for types, registering any default hook specs
	"""
    from .encoders import EncoderHooks

    pm = create_plugin_manager()
    pm.add_hookspecs(EncoderHooks)
    return pm


class TypeStringToken(enum.Enum):
    """
	Different possible tokens in a type string
	"""

    QUESTION_MARK = "?"
    LEFT_BRACE = "["
    RIGHT_BRACE = "]"
    COLON = ":"
    ATTR_NAME = "attr"
    TYPE_NAME = "type"
    COMMA = ","


class TypeStringRenderer:
    """
	Allow customization of how type strings are rendered
	"""

    def render(self, value: str, token: TypeStringToken) -> str:
        return value


class Type(abc.ABC):
    """
	A type encapsulates information about 
	"""

    name: str
    nullable: bool
    pm: pluggy.PluginManager

    def render_type_string(self, renderer: Optional[TypeStringRenderer] = None) -> str:
        """
		Render a nice human-readable representation of this type.
		"""
        if renderer is None:
            renderer = TypeStringRenderer()
        name_rendered = renderer.render(self.name, TypeStringToken.TYPE_NAME)
        suffix = (
            renderer.render("?", TypeStringToken.QUESTION_MARK) if self.nullable else ""
        )
        return f"{name_rendered}{suffix}"

    def __repr__(self) -> str:
        return self.render_type_string()


class ValueType(Type):
    """
	Base class for types that just have a single instance
	"""

    nullable: bool
    pm: pluggy.PluginManager

    @property
    @abc.abstractmethod
    def name(self) -> str:
        raise NotImplementedError


@dc.dataclass(frozen=True, repr=False)
class AnyType(Type):
    """
	Type that applies to any value. Not a regular dataclass to avoid issues w/ defaults :(
	"""

    pm: pluggy.PluginManager = dc.field(
        init=False, default_factory=create_type_plugin_manager, compare=False
    )

    @property
    def nullable(self) -> bool:
        return True

    @property
    def name(self) -> str:
        return "any"


class NumberType(ValueType, AnyType):
    """
	Base class for numeric types
	"""

    @abc.abstractmethod
    def value_from_number(self, original: Any) -> Any:
        raise NotImplementedError


@dc.dataclass(frozen=True, repr=False)
class IntegerType(NumberType):
    nullable: bool = True
    name: str = dc.field(init=False, repr=False, default="integer")

    def value_from_number(self, original: Any) -> Any:
        return int(original)


@dc.dataclass(frozen=True, repr=False)
class FloatType(NumberType):
    nullable: bool = True
    name: str = dc.field(init=False, repr=False, default="float")

    def value_from_number(self, original: Any) -> Any:
        return float(original)


@dc.dataclass(frozen=True, repr=False)
class BooleanType(NumberType):
    nullable: bool = True
    name: str = dc.field(init=False, repr=False, default="boolean")

    def value_from_number(self, original: Any) -> Any:
        return bool(original)


@dc.dataclass(frozen=True, repr=False)
class StringType(ValueType, AnyType):
    nullable: bool = True
    name: str = dc.field(init=False, repr=False, default="string")


@dc.dataclass(frozen=True, repr=False)
class ArrayType(AnyType):
    """
	An array with some element type
	"""

    element_type: Type
    nullable: bool = True

    @property
    def name(self) -> str:
        return "array"

    def render_type_string(self, renderer: Optional[TypeStringRenderer] = None) -> str:
        if renderer is None:
            renderer = TypeStringRenderer()
        type_name = renderer.render(self.name, TypeStringToken.TYPE_NAME)
        element_string = self.element_type.render_type_string(renderer)
        suffix = (
            renderer.render("?", TypeStringToken.QUESTION_MARK) if self.nullable else ""
        )
        lbrace = renderer.render("[", TypeStringToken.LEFT_BRACE)
        rbrace = renderer.render("]", TypeStringToken.RIGHT_BRACE)
        return "".join([type_name, lbrace, element_string, rbrace, suffix])


@dc.dataclass(frozen=True)
class StructField:
    """
	Contains information about a single field in a StructType
	"""

    name: str
    type: Type


@dc.dataclass(frozen=True, repr=False)
class StructType(AnyType):
    """
	A struct contains an ordered sequence of named fields, any of which
	may or may not be null
	"""

    fields: Sequence[StructField]
    nullable: bool = True

    def __post_init__(self) -> None:
        self.__dict__["fields"] = tuple(self.fields)

    @property
    def name(self) -> str:
        return "struct"

    def render_type_string(self, renderer: Optional[TypeStringRenderer] = None) -> str:
        if renderer is None:
            renderer = TypeStringRenderer()
        type_name = renderer.render(self.name, TypeStringToken.TYPE_NAME)
        suffix = (
            renderer.render("?", TypeStringToken.QUESTION_MARK) if self.nullable else ""
        )
        colon = renderer.render(":", TypeStringToken.COLON)
        field_strings = [
            f"{renderer.render(field.name, TypeStringToken.ATTR_NAME)}"
            f"{colon}{field.type.render_type_string(renderer)}"
            for field in self.fields
        ]
        comma_and_space = renderer.render(",", TypeStringToken.COMMA) + " "
        lbrace = renderer.render("[", TypeStringToken.LEFT_BRACE)
        rbrace = renderer.render("]", TypeStringToken.RIGHT_BRACE)
        return "".join(
            [type_name, lbrace, comma_and_space.join(field_strings), rbrace, suffix]
        )

    def __getitem__(self, name: str) -> StructField:
        """
		Fetch schema fields by name
		"""
        field = next((field for field in self.fields if field.name == name), None)
        if field is None:
            raise KeyError(name)
        return field

    def __contains__(self, name: str) -> bool:
        """
		Indicate whether this StructType has a field with the given name
		"""
        return any(field.name == name for field in self.fields)


EmptyType = StructType(())
