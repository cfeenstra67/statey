import abc
import enum
import dataclasses as dc
from typing import Sequence, Any, Optional, Dict

import pluggy

from statey import create_plugin_manager


UNHASHABLE = object()


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


def hashable_meta(meta: Dict[str, Any]) -> int:
    """
    Generate a hash for a `meta` dictionary. Note that this will omit non-hashable
    values so it will always succeed, but may not always generate the perfect hash
    """
    items = []
    for key, val in meta.items():
        try:
            hash(val)
        # If the type isn't hashable, we'll just hash the type instead. We
        # can't use the
        except TypeError:
            items.append((key, UNHASHABLE, type(val)))
        else:
            items.append((key, val))

    return frozenset(items)


class Type(abc.ABC):
    """
	A type encapsulates information about 
	"""

    name: str
    nullable: bool
    meta: Dict[str, Any]

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
    def with_nullable(self, nullable: bool) -> "Type":
        """
        Return a copy of this type with the given `nullable` value
        """
        raise NotImplementedError

    @abc.abstractmethod
    def with_meta(self, meta: Dict[str, Any]) -> "Type":
        """
        Return a copy of this type with the given `meta` value
        """
        raise NotImplementedError

    def __repr__(self) -> str:
        return self.render_type_string()

    # API methods to building types more easily
    def __call__(self, **meta: Dict[str, Any]) -> "Type":
        """
        Call method on a type modifies its attributes

        e.g. IntegerType(True)(nullable=False)
        """
        obj = self
        if "nullable" in meta:
            obj = obj.with_nullable(meta.pop("nullable"))
        current = self.meta.copy()
        current.update(meta)
        return obj.with_meta(current)

    def __invert__(self) -> "Type":
        """
        Returns a copy of this type with nullable set to True
        """
        return self.with_nullable(True)

    def __hash__(self) -> int:
        return hash((type(self).__name__, self.nullable, hashable_meta(self.meta)))


class DataClassMixin(abc.ABC):
    """
    Mixin to define with_nullable() and with_meta() by using dc.replace(...)
    """

    def with_nullable(self, nullable: bool) -> Type:
        return dc.replace(self, nullable=nullable)

    def with_meta(self, meta: Dict[str, Any]) -> Type:
        return dc.replace(self, meta=meta)


class ValueType(Type):
    """
	Base class for types that just have a single instance
	"""

    nullable: bool
    meta: Dict[str, Any]

    @property
    @abc.abstractmethod
    def name(self) -> str:
        raise NotImplementedError


@dc.dataclass(frozen=True, repr=False)
class AnyType(DataClassMixin, Type):
    """
	Type that applies to any value. Not a regular dataclass to avoid issues w/ defaults :(
	"""

    nullable: bool = False
    meta: Dict[str, Any] = dc.field(default_factory=dict)

    @property
    def name(self) -> str:
        return "any"

    __hash__ = Type.__hash__


class NumberType(ValueType):
    """
	Base class for numeric types
	"""


@dc.dataclass(frozen=True, repr=False)
class IntegerType(DataClassMixin, NumberType):
    nullable: bool = False
    meta: Dict[str, Any] = dc.field(default_factory=dict)
    name: str = dc.field(init=False, repr=False, default="integer")

    __hash__ = Type.__hash__


@dc.dataclass(frozen=True, repr=False)
class FloatType(DataClassMixin, NumberType):
    nullable: bool = False
    meta: Dict[str, Any] = dc.field(default_factory=dict)
    name: str = dc.field(init=False, repr=False, default="float")

    __hash__ = Type.__hash__


@dc.dataclass(frozen=True, repr=False)
class BooleanType(DataClassMixin, ValueType):
    nullable: bool = False
    meta: Dict[str, Any] = dc.field(default_factory=dict)
    name: str = dc.field(init=False, repr=False, default="boolean")

    __hash__ = Type.__hash__


@dc.dataclass(frozen=True, repr=False)
class StringType(DataClassMixin, ValueType):
    nullable: bool = False
    meta: Dict[str, Any] = dc.field(default_factory=dict)
    name: str = dc.field(init=False, repr=False, default="string")

    __hash__ = Type.__hash__


@dc.dataclass(frozen=True, repr=False)
class ArrayType(DataClassMixin, Type):
    """
	An array with some element type
	"""

    def __class_getitem__(cls, item: Any) -> Type:
        """
        Create array types
        """
        import statey as st

        typ = st.registry.get_type(item)
        return cls(typ)

    element_type: Type
    nullable: bool = False
    meta: Dict[str, Any] = dc.field(default_factory=dict)

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

    def with_element_type(self, element_type: Type) -> Type:
        """
        Return a copy of this type with the given element type
        """
        return dc.replace(self, element_type=element_type)

    def __hash__(self) -> int:
        return hash(
            (
                type(self).__name__,
                self.nullable,
                hashable_meta(self.meta),
                self.element_type,
            )
        )


@dc.dataclass(frozen=True)
class Field:
    """
	Contains information about a single field in a StructType
	"""

    name: str
    type: Type


@dc.dataclass(frozen=True, repr=False)
class StructType(DataClassMixin, Type):
    """
	A struct contains an ordered sequence of named fields, any of which
	may or may not be null
	"""

    def __class_getitem__(cls, value: Sequence[slice]) -> "Type":
        """
        Ability to use syntax like StructType["a": 1]
        """
        import statey as st

        if not isinstance(value, tuple):
            value = (value,)

        fields = []
        for item in value:

            if not isinstance(item, slice):
                raise TypeError(f"{item} is not a slice.")
            if not isinstance(item.start, str):
                raise TypeError(f"{item.start} is not a string.")
            if item.step is not None:
                raise ValueError(
                    f"{item} contains a non-null step, this is not allowed."
                )

            name = item.start
            typ = st.registry.get_type(item.stop)
            fields.append(Field(name, typ))

        return cls(fields)

    fields: Sequence[Field]
    nullable: bool = False
    meta: Dict[str, Any] = dc.field(default_factory=dict)

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

    def with_fields(self, fields: Sequence[Field]) -> Type:
        """
        Return a copy of this type with the fields replaced
        """
        new_inst = dc.replace(self)
        new_inst.__dict__["fields"] = tuple(fields)
        return new_inst

    def __hash__(self) -> int:
        return hash(
            (
                type(self).__name__,
                self.nullable,
                hashable_meta(self.meta),
                tuple(self.fields),
            )
        )


EmptyType = StructType((), True)


@dc.dataclass(frozen=True, repr=False)
class FunctionType(StructType):
    """
    First class functions :)
    """

    args: Sequence[Field]
    return_type: Type
    nullable: bool = dc.field(init=False, default=False)
    meta: Dict[str, Any] = dc.field(init=False, default_factory=dict)
    fields: Sequence[Field] = dc.field(
        init=False, default=(Field("name", StringType(False)),)
    )

    def with_nullable(self, nullable: bool) -> Type:
        new_inst = dc.replace(self)
        new_inst.__dict__["nullable"] = nullable
        new_inst.__dict__["meta"] = self.meta.copy()
        return new_inst

    def with_meta(self, meta: Dict[str, Any]) -> Type:
        new_inst = dc.replace(self)
        new_inst.__dict__["meta"] = meta
        new_inst.__dict__["nullable"] = self.nullable
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
        return "".join([lambda_string, lbrace, func_type_string, rbrace, suffix])

    @property
    def name(self) -> str:
        return "function"

    @property
    def args_type(self) -> Type:
        """
        Get a structtype for the arguments of this function
        """
        return StructType(self.args, all(arg.type.nullable for arg in self.args))

    def __hash__(self) -> int:
        return hash(
            (
                type(self).__name__,
                self.nullable,
                hashable_meta(self.meta),
                tuple(self.args),
                self.return_type,
            )
        )


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

    __hash__ = FunctionType.__hash__


@dc.dataclass(frozen=True, repr=False)
class MapType(DataClassMixin, Type):
    """
    An array with some element type
    """

    def __class_getitem__(cls, item: Any) -> Type:
        """
        Create array types
        """
        if not isinstance(item, tuple) or len(item) != 2:
            raise ValueError(f'Expected a tuple of length 2, got "{item}".')
        key_annotation, value_annotation = item

        import statey as st

        key_type = st.registry.get_type(key_annotation)
        value_type = st.registry.get_type(value_annotation)
        return cls(key_type, value_type)

    key_type: Type
    value_type: Type
    nullable: bool = False
    meta: Dict[str, Any] = dc.field(default_factory=dict)

    @property
    def name(self) -> str:
        return "map"

    def render_type_string(self, renderer: Optional[TypeStringRenderer] = None) -> str:
        if renderer is None:
            renderer = TypeStringRenderer()
        type_name = renderer.render(self.name, TypeStringToken.TYPE_NAME)
        key_string = self.key_type.render_type_string(renderer)
        value_string = self.value_type.render_type_string(renderer)
        suffix = (
            renderer.render(
                TypeStringToken.QUESTION_MARK.value, TypeStringToken.QUESTION_MARK
            )
            if self.nullable
            else ""
        )
        lbrace = renderer.render(
            TypeStringToken.LEFT_BRACE.value, TypeStringToken.LEFT_BRACE
        )
        rbrace = renderer.render(
            TypeStringToken.RIGHT_BRACE.value, TypeStringToken.RIGHT_BRACE
        )
        comma = renderer.render(TypeStringToken.COMMA.value, TypeStringToken.COMMA)
        return "".join(
            [type_name, lbrace, key_string, comma, " ", value_string, rbrace, suffix]
        )

    def with_key_type(self, key_type: Type) -> Type:
        """
        Return a copy of this type with the given key type
        """
        return dc.replace(self, key_type=key_type)

    def with_value_type(self, value_type: Type) -> Type:
        """
        Return a copy of this type with the given key type
        """
        return dc.replace(self, value_type=value_type)

    def __hash__(self) -> int:
        return hash(
            (
                type(self).__name__,
                self.nullable,
                hashable_meta(self.meta),
                self.key_type,
                self.value_type,
            )
        )


# Some exported objects
Struct = StructType  # Alias

Array = ArrayType  # Alias

Map = MapType  # Alias

Integer = IntegerType()

Float = FloatType()

Boolean = BooleanType()

Any = AnyType()

String = StringType()
