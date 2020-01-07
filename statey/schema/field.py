"""
A field is some attribute of a resource
"""
import abc
import datetime as dt
from functools import partial
from typing import Dict, Type, Any, Tuple, Optional, Callable, Sequence

import dataclasses as dc
import marshmallow as ma

from statey import exc
from statey.utils.helpers import get_all_subclasses, NamedObject
from .helpers import extract_modifiers, validate_no_input


# Object indicating that values are missing
MISSING = NamedObject("MISSING")
FUTURE = NamedObject("FUTURE")


def nested(
    schema_cls: Optional[Type["Schema"]] = None, **kwargs: Dict[str, Any]
) -> "Field":
    """
    Decorator to declare schema fields inline
    """
    if schema_cls is not None:
        return Field[schema_cls](**kwargs)
    return partial(nested, **kwargs)


def wrap_field(parent_annotation: Any) -> "Field":
    """
    Decorator to wrap fields in other annotations. Can be used
    on top of @nested

    e.g. (results in the same as st.Field[List[config]])

    @wrap_field(List)
    @nested
    class config(st.Schema):
        a = st.Field[int]
        b = st.Field[bool]
    """

    def dec(child_field):
        return Field[parent_annotation[child_field.annotation]](
            optional=child_field.optional,
            computed=child_field.computed,
            create_new=child_field.create_new,
            store=child_field.store,
            input=child_field.input,
            factory=child_field.factory,
            default=child_field.default,
        )

    return dec


class FieldMeta(abc.ABCMeta):
    """
	metaclass for the Field class.
	supports indexing, like Field[int]
	"""

    def __predicate__(cls, annotation: Any) -> bool:
        """
		Given an annotation, determine whether this field class should be used.
		"""
        raise NotImplementedError

    def __getitem__(cls, annotation: Any) -> Optional["FieldMeta"]:
        """
		Handle a type annotation on this field type.
		E.g. Field[int]
		"""
        clean_annotation, modifiers = extract_modifiers(annotation)
        modifiers["annotation"] = annotation

        for subcls in get_all_subclasses(cls):
            if subcls.__predicate__(clean_annotation):
                return _FieldWithModifiers(subcls, modifiers)
        raise KeyError(annotation)


# pylint: disable=too-many-instance-attributes
class Field(abc.ABC, metaclass=FieldMeta):
    """
	Base class for statey fields
	"""

    @classmethod
    def __predicate__(cls, annotation: Any) -> bool:  # pylint: disable=unused-argument
        return False

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        annotation: Any,
        optional: bool = False,
        computed: bool = False,
        create_new: bool = False,
        store: bool = True,
        input: bool = True,  # pylint: disable=redefined-builtin
        factory: Callable[["Resource"], Any] = None,
        default: Any = MISSING,
    ) -> None:
        """
		Initialize a Field instance

		`optional` - Indicate if the field is optional or not
		`computed` - Indicate if the field is computed or not
		`create_new` - Indicate if a change in this field implies the resource it belongs
		to must be destroyed and recreated
		"""
        if factory is not None and default is not MISSING:
            raise ValueError("`factory` and `default` cannot both be passed to Field.")

        if not store and optional:
            raise ValueError(
                "`store=False` and `optional=True` are mutually exclusive."
            )

        if factory is None:
            factory = lambda resource: default
            self.default = default
        else:
            self.default = FUTURE

        self.annotation = annotation
        self.optional = optional
        self.computed = computed
        self.create_new = create_new
        self.store = store
        self.factory = factory
        self.input = input

        self._schema = None
        self._name = None

    def _associate_schema(self, schema: Type["Schema"], name: str) -> None:
        self._schema = schema
        self._name = name

    def _clear_schema(self) -> None:
        self._schema = self._name = None

    @property
    def name(self):
        """
        Return the name of this field, or None if it has not been set
        """
        return self._name

    def _marshmallow_default_args(self, is_input: bool = False) -> Dict[str, Any]:
        """
		Get the default arguments to a marshmallow field based on the current
		modifiers
		"""
        args = {"required": True, "default": self.default}

        if self.optional or self.default is not MISSING:
            args["required"] = False
            args["missing"] = None if self.default is MISSING else self.default

        if not self.store and not is_input:
            args["load_only"] = True
            args["missing"] = self.default
            args["required"] = False

        if is_input and (self.computed or not self.input):
            args["validate"] = [validate_no_input("This field does not accept input.")]
            args["required"] = False
            args["missing"] = self.default

        return args

    def marshmallow_field(self, is_input: bool = False) -> ma.fields.Field:
        """
		Return a field factory for the marshmallow field corresponding to this
		field type. The factory should take the same arguments as the constructors
		for marshmallow fields.

		`is_input` - Determine if this field should be created for user input. If so,
		values for computed fields are not accepted.
		"""
        return ma.fields.Field(**self._marshmallow_default_args(is_input))

    def dataclass_field(self) -> Tuple[Type, dc.field]:
        """
		Generate an annotation and field object for a dataclass field for this attribute.
		"""
        annotation = self.annotation
        if self.optional:
            annotation = Optional[annotation]
        return annotation, dc.field(default=None)

    def reference_factory(
        self, resource: "Resource", name: str, nested_path: Tuple[str, ...] = ()
    ) -> "Reference":
        """
        Given a resource and field name, return a reference object for this field.
        """
        from .symbol import Reference

        return Reference(resource, name, self, nested_path)


# pylint: disable=too-few-public-methods
class _FieldWithModifiers:
    """
	Simple container to support the annotations API. Basically a partial, but
	meant for this specific use-case (so we don't have to handle all partials
	in the field creation code--this is a bit safer)
	"""

    def __init__(self, field_cls: Type[Field], defaults: Dict[str, Any]) -> None:
        self.field_cls = field_cls
        self.defaults = defaults

    def __call__(self, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> Field:
        return self.field_cls(*args, **self.defaults, **kwargs)


class StrField(Field):
    """
	Field class for strings
	"""

    @classmethod
    def __predicate__(cls, annotation: Any) -> bool:
        return annotation is str

    def marshmallow_field(self, is_input: bool = False) -> ma.fields.Field:
        return ma.fields.Str(**self._marshmallow_default_args(is_input))


class BoolField(Field):
    """
	Field class for bools
	"""

    @classmethod
    def __predicate__(cls, annotation: Any) -> bool:
        return annotation is bool

    def marshmallow_field(self, is_input: bool = False) -> ma.fields.Field:
        return ma.fields.Bool(**self._marshmallow_default_args(is_input))


class IntField(Field):
    """
	Field class for ints
	"""

    @classmethod
    def __predicate__(cls, annotation: Any) -> bool:
        return annotation is int

    def marshmallow_field(self, is_input: bool = False) -> ma.fields.Field:
        return ma.fields.Int(**self._marshmallow_default_args(is_input))


class FloatField(Field):
    """
	Field class for floats
	"""

    @classmethod
    def __predicate__(cls, annotation: Any) -> bool:
        return annotation is float

    def marshmallow_field(self, is_input: bool = False) -> ma.fields.Field:
        return ma.fields.Float(**self._marshmallow_default_args(is_input))


class DateTimeField(Field):
    """
    Field class for datetimes
    """

    @classmethod
    def __predicate__(cls, annotation: Any) -> bool:
        return annotation is dt.datetime

    def marshmallow_field(self, is_input: bool = False) -> ma.fields.Field:
        return ma.fields.DateTime(**self._marshmallow_default_args(is_input))


class DateField(Field):
    """
    Field class for dates
    """

    @classmethod
    def __predicate__(cls, annotation: Any) -> bool:
        return annotation is dt.date

    def marshmallow_field(self, is_input: bool = False) -> ma.fields.Field:
        return ma.fields.Date(**self._marshmallow_default_args(is_input))


class TimeField(Field):
    """
    Field class for dates
    """

    @classmethod
    def __predicate__(cls, annotation: Any) -> bool:
        return annotation is dt.time

    def marshmallow_field(self, is_input: bool = False) -> ma.fields.Field:
        return ma.fields.Time(**self._marshmallow_default_args(is_input))


class TimeDeltaField(Field):
    """
    Field class for dates
    """

    @classmethod
    def __predicate__(cls, annotation: Any) -> bool:
        return annotation is dt.timedelta

    def marshmallow_field(self, is_input: bool = False) -> ma.fields.Field:
        return ma.fields.TimeDelta(**self._marshmallow_default_args(is_input))


class ListField(Field):
    """
    Field class for lists
    """

    @classmethod
    def __predicate__(cls, annotation: Any) -> bool:
        return (
            isinstance(annotation, type)
            and issubclass(annotation, Sequence)
            and not issubclass(annotation, Tuple)
        )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if (
            getattr(self.annotation, "__args__", None) is None
            or len(self.annotation.__args__) != 1
        ):
            raise exc.InitializationError(
                f"List fields must have an item type annotation e.g. List[str]. "
                f"Got {self.annotation}."
            )

        self.item_annotation = self.annotation.__args__[0]
        self.item_field = Field[self.item_annotation]()
        self.item_ma_field = self.item_field.marshmallow_field(is_input=False)

    def marshmallow_field(self, is_input: bool = False) -> ma.fields.Field:
        return ma.fields.List(
            self.item_ma_field, **self._marshmallow_default_args(is_input)
        )


class DictField(Field):
    """
    Field class for dicts
    """

    @classmethod
    def __predicate__(cls, annotation: Any) -> bool:
        return isinstance(annotation, type) and issubclass(annotation, Dict)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if (
            getattr(self.annotation, "__args__", None) is None
            or len(self.annotation.__args__) != 2
        ):
            raise exc.InitializationError(
                f"Dict fields must have key and value annotations e.g. Dict[str, int]."
                f" Got {self.annotation}."
            )

        self.key_annotation = self.annotation.__args__[0]
        self.key_field = Field[self.key_annotation]()
        self.key_ma_field = self.key_field.marshmallow_field(is_input=False)
        self.value_annotation = self.annotation.__args__[1]
        self.value_field = Field[self.value_annotation]()
        self.value_ma_field = self.value_field.marshmallow_field(is_input=False)

    def marshmallow_field(self, is_input: bool = False) -> ma.fields.Field:
        return ma.fields.Dict(
            keys=self.key_ma_field,
            values=self.value_ma_field,
            **self._marshmallow_default_args(is_input),
        )


class TupleField(Field):
    """
    Field class for tuples
    """

    @classmethod
    def __predicate__(cls, annotation: Any) -> bool:
        return isinstance(annotation, type) and issubclass(annotation, Tuple)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if getattr(self.annotation, "__args__", None) is None:
            raise exc.InitializationError(
                f"Tuple fields must have annotations for each item e.g. "
                f"Tuple[int, bool]. Got {self.annotation}."
            )

        item_annotations = []
        item_fields = []
        item_ma_fields = []
        for arg in self.annotation.__args__:
            item_annotations.append(arg)
            field = Field[arg]()
            item_fields.append(field)
            item_ma_fields.append(field.marshmallow_field(is_input=False))

        self.item_annotations = tuple(item_annotations)
        self.item_fields = tuple(item_fields)
        self.item_ma_fields = tuple(item_ma_fields)

    def marshmallow_field(self, is_input: bool = False) -> ma.fields.Field:
        return ma.fields.Tuple(
            self.item_ma_fields, **self._marshmallow_default_args(is_input)
        )


class NestedField(Field):
    """
    Allow nesting another Schema as a field type
    """

    @classmethod
    def __predicate__(cls, annotation: Any) -> bool:
        # pylint: disable=cyclic-import
        from .schema import Schema

        return (
            isinstance(annotation, Schema)
            or isinstance(annotation, type)
            and issubclass(annotation, Schema)
        )

    def __init__(self, *args, **kwargs):
        from .schema import Schema, SchemaHelper

        super().__init__(*args, **kwargs)
        if isinstance(self.annotation, Schema):
            self.annotation = type(self.annotation)
        self.schema_helper = SchemaHelper(self.annotation)

    def marshmallow_field(self, is_input: bool = False) -> ma.fields.Field:
        nested_schema_cls = (
            self.schema_helper.input_schema_cls
            if is_input
            else self.schema_helper.schema_cls
        )
        schema = nested_schema_cls(unknown=ma.RAISE)
        return ma.fields.Nested(schema, required=True)

    def reference_factory(
        self, resource: "Resource", name: str, nested_path: Tuple[str, ...] = ()
    ) -> "Reference":
        from .symbol import SchemaReference

        return SchemaReference(resource, self, nested_path + (name,))
