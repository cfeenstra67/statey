import abc
from collections import OrderedDict
from functools import partial
from typing import Dict, Type, Any, Tuple, Optional, Callable

import dataclasses as dc
import marshmallow as ma

from .helpers import extract_modifiers, validate_no_input
from statey import exc
from statey.utils.helpers import get_all_subclasses


# Object indicating that values are missing
MISSING = object()


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

	def __getitem__(cls, annotation: Any) -> Optional['FieldMeta']:
		"""
		Handle a type annotation on this field type.
		E.g. Field[int]
		"""
		clean_annotation, modifiers = extract_modifiers(annotation)
		modifiers['annotation'] = annotation

		for subcls in get_all_subclasses(cls):
			if subcls.__predicate__(clean_annotation):
				return _FieldWithModifiers(subcls, modifiers)
		raise KeyError(annotation)


class Field(abc.ABC, metaclass=FieldMeta):
	"""
	Base class for statey fields
	"""
	@classmethod
	def __predicate__(cls, annotation: Any) -> bool:
		return False

	def __init__(
				self,
				annotation: Any,
				optional: bool = False,
				computed: bool = False,
				create_new: bool = False,
				default: Any = MISSING
	) -> None:
		"""
		Initialize a Field instance

		`optional` - Indicate if the field is optional or not
		`computed` - Indicate if the field is computed or not
		`create_new` - Indicate if a change in this field implies the resource it belongs
		to must be destroyed and recreated
		"""
		self.annotation = annotation
		self.optional = optional
		self.computed = computed
		self.create_new = create_new
		self.default = default

		self._schema = None
		self._name = None

	def _associate_schema(self, schema: Type['Schema'], name: str) -> None:
		self._schema = schema
		self._name = name

	def _clear_schema(self) -> None:
		self._schema = self._name = None

	@property
	def name(self):
		return self._name

	def _marshmallow_default_args(self, is_input: bool = False) -> Dict[str, Any]:
		"""
		Get the default arguments to a marshmallow field based on the current
		modifiers
		"""
		args = {
			'required': True,
			'default': self.default
		}
		if self.optional:
			args['required'] = False
			args['missing'] = None if self.default is MISSING else self.default

		if self.computed and is_input:
			args['validate'] = [validate_no_input('Computed fields do not accept input.')]
			args['required'] = False
			args['missing'] = MISSING

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
