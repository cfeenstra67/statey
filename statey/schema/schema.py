"""
The Schema class allows resources to define data attributes
"""
import copy
from functools import lru_cache
from typing import Tuple, Any, Type, Dict, Iterator, Callable, Optional

import dataclasses as dc
import marshmallow as ma

from statey import exc
from .field import Field, _FieldWithModifiers, MISSING, FUTURE
from .helpers import convert_ma_validation_error
from .symbol import Symbol


HIDDEN_FIELDS = ("__meta__",)


class SchemaMeta(ma.schema.SchemaMeta):
    """
    Metaclass for the Schema class
    """

    @staticmethod
    def _construct_fields(attrs: Dict[str, Any]) -> Dict[str, Field]:
        fields = {}

        for name, value in attrs.items():
            if isinstance(value, Field):
                fields[name] = value
            elif (
                isinstance(value, _FieldWithModifiers)
                or isinstance(value, type)
                and issubclass(value, Field)
            ):
                fields[name] = value()

        return fields

    def __new__(cls, name: str, bases: Tuple[Type, ...], attrs: Dict[str, Any]) -> None:
        new_fields = cls._construct_fields(attrs)
        attrs.update(new_fields)

        new_cls = super().__new__(cls, name, bases, attrs)
        fields = {k: copy.copy(v) for k, v in getattr(new_cls, "__fields__", {}).items()}
        fields.update(new_fields)

        # Providing a mechanism to ignore superclass fields by
        # providing a non-field object in its place
        for key in list(fields):
            value = getattr(new_cls, key, None)
            if not isinstance(value, Field):
                fields[key]._clear_schema()
                del fields[key]

        # Update metadata for fields
        for field_name, field in fields.items():
            field._associate_schema(new_cls, field_name)

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

    _source_schema = None

    @staticmethod
    def _equals(snapshot1: "SchemaSnapshot", snapshot2: "SchemaSnapshot") -> bool:
        if type(snapshot1) is not type(snapshot2):
            return False
        return snapshot1.__dict__ == snapshot2.__dict__

    @property
    def source_schema(self) -> Type["Resource"]:
        """
        Obtain the schema that was used to create this snapshot class
        """
        return self._source_schema

    @classmethod
    @lru_cache(maxsize=10000)
    def from_schema(cls, schema: Schema) -> Type["SchemaSnapshot"]:
        """
        Create or retrieve a SchemaSnapsot subclass for the given
        schema class
        """
        subcls = type(f"{type(schema).__name__}Snapshot", (cls,), {"_source_schema": schema})

        fields = [("__meta__", Dict[str, Any], dc.field(default_factory=dict))]
        for name, field in schema.__fields__.items():
            fields.append((name, *field.dataclass_field()))

        return dc.make_dataclass(
            subcls.__name__, fields, bases=(subcls,), frozen=True, eq=cls._equals
        )

    def copy(self, **kwargs: Dict[str, Any]) -> "SchemaSnapshot":
        """
        Return a shallow copy of the current with any replaced attributes specified
        """
        kws = self.__dict__.copy()
        kws.update(kwargs)
        inst = type(self)(**kws)
        return inst

    def reset_factories(self, resource: "Resource") -> "SchemaSnapshot":
        """
        Reset all lazy fields in the given snapshot
        """
        schema = self.source_schema
        # pylint: disable=no-member
        lazy_fields = self.__meta__["lazy"] = set(self.__meta__.get("lazy", []))
        values = {}

        for key in lazy_fields:
            if key not in schema.__fields__:
                lazy_fields.remove(key)
                continue
            field = schema.__fields__[key]
            values[key] = field.factory(resource)

        return self.copy(**values)

    def fill_missing_values(self, resource: "Resource") -> "SchemaSnapshot":
        """
        Fill any MISSING, FUTURE, or lazy values with references
        """
        values = dict(self.items())
        schema = self.source_schema

        # pylint: disable=no-member
        lazy_fields = self.__meta__["lazy"] = set(self.__meta__.get("lazy", []))

        # Execute factories first, then add references (because factories can
        # resolve to MISSING)
        for key, val in values.items():
            if val is FUTURE:
                values[key] = schema.__fields__[key].factory(resource)
                field = schema.__fields__[key]
                if field.store and not field.computed:
                    lazy_fields.add(key)

        for key, val in values.items():
            if val is MISSING:
                values[key] = resource.attrs[key]
                field = schema.__fields__[key]
                if field.store and not field.computed:
                    lazy_fields.add(key)

        return self.copy(**values)

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
        return ((k, v) for k, v in self.__dict__.items() if k not in HIDDEN_FIELDS)

    # Simplify common schema snapshot operations
    def update_fields(self, func: Callable[[str, Any], Any]) -> "SchemaSnapshot":
        """
        Update field value according to the given function
        """
        kws = {}
        for key, val in self.items():
            kws[key] = func(key, val)
        return self.copy(**kws)

    def resolve(
        self, graph: "ResourceGraph", field_filter: Optional[Callable[[Field], bool]] = None,
    ) -> "SchemaSnapshot":
        """
        Resolve all fields
        """
        if field_filter is None:
            field_filter = lambda x: True

        kws = {}
        for key, val in self.items():
            field = self.source_schema.__fields__[key]
            if field_filter(field):
                kws[key] = val.resolve(graph) if isinstance(val, Symbol) else val
            else:
                kws[key] = val.resolve_partial(graph) if isinstance(val, Symbol) else val
        return self.copy(**kws)

    def resolve_partial(self, graph: "ResourceGraph") -> "SchemaSnapshot":
        """
        Partially resolve all fields
        """
        return self.update_fields(
            lambda key, val: val.resolve_partial(graph) if isinstance(val, Symbol) else val
        )


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
        `self.schema`.
        """
        fields = {}

        for name, value in self.orig_schema_cls.__fields__.items():
            fields[name] = value.marshmallow_field(is_input)

        return type("Schema", (self.orig_schema_cls,), fields)

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
                f"Attempting to assign to field {field}, but it is a computed field."
            )

        return_field = symbol.type()
        if not self.compatible_fields(return_field, field):
            raise exc.SymbolTypeError(
                f"Attempting to assign to field {type(field)} from {type(return_field)}"
                f" (symbol {symbol})."
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
            input_data = self.input_schema_cls(unknown=ma.RAISE, exclude=tuple(symbols)).load(
                values
            )
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
