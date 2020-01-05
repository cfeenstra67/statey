"""
A field is some attribute of a resource
"""
import abc
from typing import Dict, Type, Any, Tuple, Optional, Callable

import dataclasses as dc
import marshmallow as ma

from statey.utils.helpers import get_all_subclasses, NamedObject
from .helpers import extract_modifiers, validate_no_input


# Object indicating that values are missing
MISSING = NamedObject("MISSING")
FUTURE = NamedObject("FUTURE")


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
            raise ValueError("`store=False` and `optional=True` are mutually exclusive.")

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
