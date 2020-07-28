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
    TO = "=>"
    LAMBDA = "λ"


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

    @abc.abstractmethod
    def to_nullable(self, nullable: bool) -> "Type":
        """
        Return a copy of this type with the given `nullable` value
        """
        raise NotImplementedError

    def __repr__(self) -> str:
        return self.render_type_string()


class DataClassWithNullableFieldMixin:
    """
    Mixin to define to_nullable() by using dc.replace(...)
    """
    def to_nullable(self, nullable: bool) -> Type:
        inst = dc.replace(self, nullable=nullable)
        inst.__dict__['pm'] = self.pm
        return inst


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

    def to_nullable(self, nullable: bool) -> Type:
        return self


class NumberType(ValueType, AnyType):
    """
	Base class for numeric types
	"""


@dc.dataclass(frozen=True, repr=False)
class IntegerType(DataClassWithNullableFieldMixin, NumberType):
    nullable: bool = True
    name: str = dc.field(init=False, repr=False, default="integer")


@dc.dataclass(frozen=True, repr=False)
class FloatType(DataClassWithNullableFieldMixin, NumberType):
    nullable: bool = True
    name: str = dc.field(init=False, repr=False, default="float")


@dc.dataclass(frozen=True, repr=False)
class BooleanType(DataClassWithNullableFieldMixin, NumberType):
    nullable: bool = True
    name: str = dc.field(init=False, repr=False, default="boolean")


@dc.dataclass(frozen=True, repr=False)
class StringType(DataClassWithNullableFieldMixin, ValueType, AnyType):
    nullable: bool = True
    name: str = dc.field(init=False, repr=False, default="string")


@dc.dataclass(frozen=True, repr=False)
class ArrayType(DataClassWithNullableFieldMixin, AnyType):
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
class Field:
    """
	Contains information about a single field in a StructType
	"""

    name: str
    type: Type


@dc.dataclass(frozen=True, repr=False)
class StructType(DataClassWithNullableFieldMixin, AnyType):
    """
	A struct contains an ordered sequence of named fields, any of which
	may or may not be null
	"""

    fields: Sequence[Field]
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

    def __getitem__(self, name: str) -> Field:
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


@dc.dataclass(frozen=True, repr=False)
class FunctionType(StructType):
    """
    First class functions :)
    """

    args: Sequence[Field]
    return_type: Type
    nullable: bool = dc.field(init=False, default=False)
    fields: Sequence[Field] = dc.field(
        init=False, default=(Field("name", StringType(False)),)
    )

    def to_nullable(self, nullable: bool) -> Type:
        new_inst = dc.replace(self)
        new_inst.__dict__['nullable'] = nullable
        new_inst.__dict__['pm'] = self.pm
        return new_inst

    def render_type_string(self, renderer: Optional[TypeStringRenderer] = None) -> str:
        if renderer is None:
            renderer = TypeStringRenderer()
        type_name = renderer.render(self.name, TypeStringToken.TYPE_NAME)
        suffix = (
            renderer.render(
                TypeStringToken.QUESTION_MARK.value, TypeStringToken.QUESTION_MARK
            )
            if self.nullable
            else ""
        )
        colon = renderer.render(TypeStringToken.COLON.value, TypeStringToken.COLON)

        arg_strings = [
            f"{renderer.render(field.name, TypeStringToken.ATTR_NAME)}"
            f"{colon}{field.type.render_type_string(renderer)}"
            for field in self.args
        ]
        comma_and_space = (
            renderer.render(TypeStringToken.COMMA.value, TypeStringToken.COMMA) + " "
        )
        lbrace = renderer.render(
            TypeStringToken.LEFT_BRACE.value, TypeStringToken.LEFT_BRACE
        )
        rbrace = renderer.render(
            TypeStringToken.RIGHT_BRACE.value, TypeStringToken.RIGHT_BRACE
        )

        args_string = "".join(
            [lbrace, comma_and_space.join(arg_strings), rbrace, suffix]
        )

        return_type_string = self.return_type.render_type_string(renderer)

        to_string = renderer.render(TypeStringToken.TO.value, TypeStringToken.TO)
        lambda_string = renderer.render(
            TypeStringToken.LAMBDA.value, TypeStringToken.LAMBDA
        )

        func_type_string = " ".join([args_string, to_string, return_type_string])
        return "".join([lambda_string, lbrace, func_type_string, rbrace])

    @property
    def name(self) -> str:
        return "function"


@dc.dataclass(frozen=True, repr=False)
class NativeFunctionType(FunctionType):
    """
    Regular python implementation of FunctionType
    """

    def __post_init__(self) -> None:
        self.__dict__["fields"] = (
            Field("name", StringType(False)),
            # Serialized function object
            Field("serialized", StringType(False)),
        )

    @property
    def name(self) -> str:
        return "native_function"
