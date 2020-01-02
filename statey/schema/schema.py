import abc
import copy
from functools import lru_cache
from typing import Tuple, Any, Type, Dict, Iterator, Callable

import dataclasses as dc
import marshmallow as ma

from .field import Field, _FieldWithModifiers, MISSING
from .helpers import convert_ma_validation_error
from .symbol import Symbol, Reference
from statey import exc


class SchemaMeta(ma.schema.SchemaMeta):
	"""
	Metaclass for the Schema class
	"""
	@staticmethod
	def _construct_fields(attrs: Dict[str, Any]) -> Dict[str, Field]:
		fields = {}

		for name, value in attrs.items():
			field = None
			if isinstance(value, Field):
				fields[name] = value
			elif (
				isinstance(value, _FieldWithModifiers)
				or isinstance(value, type) and issubclass(value, Field)
			):
				fields[name] = value()

		return fields

	def __new__(cls, name: str, bases: Tuple[Type, ...], attrs: Dict[str, Any]) -> None:
		new_fields = cls._construct_fields(attrs)
		attrs.update(new_fields)

		new_cls = super().__new__(cls, name, bases, attrs)
		fields = {k: copy.copy(v) for k, v in getattr(new_cls, '__fields__', {}).items()}
		fields.update(new_fields)

		# Providing a mechanism to ignore superclass fields by
		# providing a non-field object in its place
		for key in list(fields):
			value = getattr(new_cls, key, None)
			if not isinstance(value, Field):
				fields[key]._clear_schema()
				del fields[key]

		# Update metadata for fields
		for name, field in fields.items():
			field._associate_schema(new_cls, name)

		new_cls.__fields__ = fields
		return new_cls


class Schema(ma.Schema, metaclass=SchemaMeta):
	"""
	Main statey schema class. This mainly just acts as a container for fields--most
	of the actual logic lives in the SchemaHelper and SchemaMeta classes

	Although this class inherits from ma.Schema, it does NOT work by defining marshmallow
	fields alone. It does, however, mean that decorators such as ma.validates_schema can be
	used to customize behavior just like a regular marshmallow schema.

	TODO: introduce decorators to schemas like ma.validates_schema
	"""


class SchemaSnapshot:
	"""
	Superclass for schema snapshots. Provides a from_schema method to
	instantiate a SchemaSnapshot class 
	"""
	_graph = None
	_resource = None
	_source_schema = None

	@staticmethod
	def _equals(snapshot1: 'SchemaSnapshot', snapshot2: 'SchemaSnapshot') -> bool:
		if type(snapshot1) is not type(snapshot2):
			return False
		return snapshot1.__dict__ == snapshot2.__dict__

	@property
	def resource(self) -> 'Resource':
		"""
		Obtain the resource associated with this instance, or None
		"""
		return self._resource

	@property
	def source_schema(self) -> Type['Resource']:
		"""
		Obtain the schema that was used to create this snapshot class
		"""
		return self._source_schema

	@classmethod
	@lru_cache(maxsize=10000)
	def from_schema(cls, schema: Schema) -> Type['SchemaSnapshot']:
		"""
		Create or retrieve a SchemaSnapsot subclass for the given
		schema class
		"""
		subcls = type(
			f'{cls.__name__}Snapshot',
			(cls,),
			{'_source_schema': schema}
		)

		fields = []
		for name, field in schema.__fields__.items():
			fields.append((name, *field.dataclass_field()))

		return dc.make_dataclass(
			subcls.__name__, fields,
			bases=(subcls,),
			frozen=True,
			eq=cls._equals
		)

	def copy(self, **kwargs: Dict[str, Any]) -> 'SchemaSnapshot':
		"""
		Return a shallow copy of the current with any replaced attributes specified
		"""
		kws = self.__dict__.copy()
		kws.update(kwargs)
		inst = type(self)(**kws)
		inst._graph = self._graph
		inst._resource = self._resource
		inst._source_schema = self._source_schema
		return inst

	# Methods to allow using snapshots like dictionaries

	def __getitem__(self, key: str) -> Any:
		"""
		Retrieve a particular value using __getitem__ syntax
		(equivalent to __getattr__, but can be used when field
		names overlap with methods or similar)
		"""
		return self.__dict__[key]

	def items(self) -> Iterator[Tuple[str, Any]]:
		"""
		Iterate over the fields for this dataclass
		"""
		return self.__dict__.items()

	# Simplify common schema snapshot operations
	def update_fields(self, func: Callable[[str, Any], Any]) -> 'SchemaSnapshot':
		"""
		Update field value according to the given function
		"""
		kws = {}
		for key, val in self.items():
			kws[key] = func(key, val)
		return self.copy(**kws)

	def resolve(self, graph: 'ResourceGraph') -> 'SchemaSnapshot':
		"""
		Resolve all fields
		"""
		return self.update_fields(lambda key, val: val.resolve(graph))

	def resolve_partial(self, graph: 'ResourceGraph') -> 'SchemaSnapshot':
		"""
		Partially resolve all fields
		"""
		return self.update_fields(lambda key, val: val.resolve_partial(graph))


class SchemaHelper:
	"""
	Helper class that wraps the Schema class so that we can define methods without
	reducing the possible names we can use for attributes.
	"""
	def __init__(self, schema_class: Type[Schema]) -> None:
		"""
		Initialize the SchemaHelper object

		`schema_class` - a Schema subclass
		"""
		self.orig_schema_cls = schema_class
		self.snapshot_cls = SchemaSnapshot.from_schema(schema_class)

		self.input_schema_cls = self.construct_marshmallow_schema(is_input=True)
		self.schema_cls = self.construct_marshmallow_schema(is_input=False)

	def construct_marshmallow_schema(self, is_input: bool = True) -> ma.Schema:
		"""
		Construct a marshmallow schema for validating input corresponding to
		`self.schema`. Determine if 
		"""
		fields = {}

		for name, value in self.orig_schema_cls.__fields__.items():
			fields[name] = value.marshmallow_field(is_input)

		return type('Schema', (self.orig_schema_cls,), fields)

	@staticmethod
	def compatible_fields(from_field: Field, to_field: Field) -> bool:
		"""
		Determine whether a return type of from_field is compatible with
		to_field
		"""
		# If the fields have different types, they aren't compatible
		if not isinstance(from_field, type(to_field)):
			return False

		# If the return type field is optional but the destination field
		# is not, they aren't compatible
		if from_field.optional and not to_field.optional:
			return False

		return True

	def validate_symbol(self, name: str, symbol: Symbol) -> None:
		"""
		Validate a specific symbol given a name
		"""
		field = self.orig_schema_cls.__fields__[name]
		if field.computed:
			raise exc.SymbolTypeError(
				f'Attempting to assign to field {repr(field)}, but it is a computed field.'
			)

		return_field = symbol.type()
		if not self.compatible_fields(return_field, field):
			raise exc.SymbolTypeError(
				f'Attempting to assign to field {repr(field)} from {repr(return_field)}'
				f' from symbol {repr(symbol)}.'
			)

	def validate_symbols(self, symbols: Dict[str, Symbol]) -> None:
		"""
		Validate any symbols found in input data
		"""
		errors = {}
		for name, symbol in symbols.items():
			try:
				self.validate_symbol(name, symbol)
			except exc.SymbolError as error:
				errors[name] = str(error)

		if len(errors) > 0:
			raise exc.InputValidationError(errors)

	def fill_missing_values(self, snapshot: SchemaSnapshot) -> SchemaSnapshot:
		"""
		Fill any MISSING values with references
		"""
		values = {}
		for key, val in snapshot.items():
			if val is MISSING:
				values[key] = Reference(snapshot.resource, snapshot.resource.Schema.__fields__[key])
		return snapshot.copy(**values)

	def load_input(self, data: Dict[str, Any]) -> Dict[str, Any]:
		"""
		Load user input for this resource type.
		"""
		symbols = {}
		values = {}

		for key, value in data.items():
			if key in self.orig_schema_cls.__fields__ and isinstance(value, Symbol):
				symbols[key] = value
			else:
				values[key] = value

		errors = []

		try:
			self.validate_symbols(symbols)
		except ma.ValidationError as error:
			errors.append(error)

		try:
			input_data = self.input_schema_cls(
				unknown=ma.RAISE,
				exclude=tuple(symbols)
			).load(values)
		except ma.ValidationError as error:
			errors.append(error)

		if len(errors) > 0:
			msg = {}
			for error in errors:
				msg.update(error.messages)
			raise exc.InputValidationError(msg)

		input_data.update(symbols)
		return input_data

	@convert_ma_validation_error
	def load(self, data: Dict[str, Any]) -> Dict[str, Any]:
		"""
		Load previously serialized data about this resource's state
		"""
		return self.schema_cls(unknown=ma.RAISE).load(data)

	@convert_ma_validation_error
	def dump(self, data: Dict[str, Any]) -> Dict[str, Any]:
		"""
		Dump state data to a serialized version.
		"""
		return self.schema_cls(unknown=ma.RAISE).dump(data)
