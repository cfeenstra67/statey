"""
The Schema class allows resources to define data attributes
"""
import copy
from functools import lru_cache
from typing import Tuple, Any, Type, Dict, Iterator, Callable, Optional, Union

import dataclasses as dc
import marshmallow as ma

from statey import exc
from .field import Field, _FieldWithModifiers, MISSING, FUTURE, NestedField
from .helpers import convert_ma_validation_error
from .symbol import Symbol, CacheManager


RESERVED_FIELDS = ("__meta__", "__schema__")


class SchemaMeta(ma.schema.SchemaMeta):
    """
    Metaclass for the Schema class
    """

    @staticmethod
    def _construct_fields(schema_name: str, attrs: Dict[str, Any]) -> Dict[str, Field]:
        fields = {}

        for name, value in attrs.items():
            if name in RESERVED_FIELDS:
                raise exc.ReservedFieldName(name, schema_name)
            if isinstance(value, Field):
                field = value
            elif (
                isinstance(value, _FieldWithModifiers)
                or isinstance(value, type)
                and issubclass(value, Field)
            ):
                field = value()
            else:
                continue

            if not field.is_serializable() and field.store:
                raise exc.InvalidField(
                    f"Field {field} is not serializable, but `store=True`. This is"
                    f"not allowed. Set `store=False` e.g. st.Field(store=False)."
                )

            fields[name] = field

        return fields

    def __getitem__(cls, path: Union[str, Tuple[str, ...]]) -> Field:
        if isinstance(path, str):
            path = (path,)

        schema = cls
        for idx, comp in enumerate(path[:-1]):
            if isinstance(schema, NestedField):
                schema = schema.annotation
            if isinstance(schema, Field) or (idx > 0 and comp not in schema):
                raise KeyError(path)
            schema = schema[comp]

        try:
            return schema.__fields__[path[-1]]
        except KeyError as exc:
            raise KeyError(path) from exc

    def __contains__(cls, path: Union[str, Tuple[str, ...]]) -> bool:
        if isinstance(path, str):
            path = (path,)

        schema = cls
        for idx, comp in enumerate(path):
            if isinstance(schema, NestedField):
                schema = schema.annotation
            if isinstance(schema, Field) or (idx > 0 and comp not in schema):
                return False
            schema = schema[comp]

        return True

    def __new__(cls, name: str, bases: Tuple[Type, ...], attrs: Dict[str, Any]) -> None:
        new_fields = cls._construct_fields(name, attrs)
        attrs.update(new_fields)

        new_cls = super().__new__(cls, name, bases, attrs)
        fields = {
            k: copy.copy(v) for k, v in getattr(new_cls, "__fields__", {}).items()
        }
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

    Although this class inherits from ma.Schema, it does NOT work by defining
    marshmallow fields alone. It does, however, mean that decorators such as
    ma.validates_schema can be used to customize behavior just like a regular
    marshmallow schema.
    """


class SchemaSnapshot:
    """
    Superclass for schema snapshots. Provides a from_schema method to
    instantiate a SchemaSnapshot class
    """

    __meta__ = {}
    __schema__ = Schema

    @classmethod
    @lru_cache(maxsize=10000)
    def from_schema(
        cls, schema: Type[Schema], name: Optional[str] = None
    ) -> Type["SchemaSnapshot"]:
        """
        Create or retrieve a SchemaSnapsot subclass for the given
        schema class
        """
        if name is None:
            name = f"{schema.__name__}Snapshot"

        fields = [
            (
                "__meta__",
                Dict[str, Any],
                dc.field(default_factory=dict, repr=False, compare=False),
            ),
            ("__schema__", schema, dc.field(init=False, repr=False, default=schema)),
        ]
        for field_name, field in schema.__fields__.items():
            fields.append((field_name, *field.dataclass_field()))

        return dc.make_dataclass(name, fields, bases=(cls,), frozen=True, eq=True)

    def copy(self, **kwargs: Dict[str, Any]) -> "SchemaSnapshot":
        """
        Return a shallow copy of the current with any replaced attributes specified
        """
        kws = self.__dict__.copy()
        kws.update(kwargs)
        kws.pop("__schema__", None)
        inst = type(self)(**kws)
        return inst

    def reset_factories(self, resource: "Resource") -> "SchemaSnapshot":
        """
        Reset all lazy fields in the given snapshot
        """
        schema = self.__schema__
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
        schema = self.__schema__

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
        return ((k, v) for k, v in self.__dict__.items() if k not in RESERVED_FIELDS)

    def resolve(
        self,
        graph: "ResourceGraph",
        field_filter: Optional[Callable[[Field], bool]] = None,
        cache: Optional[CacheManager] = None,
    ) -> "SchemaSnapshot":
        """
        Resolve all fields
        """
        if field_filter is None:
            field_filter = lambda x: True

        kws = {}
        cache = CacheManager() if cache is None else cache

        for key, val in self.items():
            if not isinstance(val, Symbol):
                kws[key] = val
                continue
            field = self.__schema__[key]
            if field_filter(field):
                kws[key] = val.resolve(graph, cache)
            else:
                kws[key] = val.resolve_partial(graph, cache)

        return self.copy(**kws)

    def resolve_partial(
        self, graph: "ResourceGraph", cache: Optional[CacheManager] = None,
    ) -> "SchemaSnapshot":
        """
        Partially resolve all fields
        """
        cache = CacheManager() if cache is None else cache

        kws = {}
        for key, val in self.items():
            if not isinstance(val, Symbol):
                kws[key] = val
                continue
            kws[key] = val.resolve_partial(graph, cache)

        return self.copy(**kws)


class SchemaHelper:
    """
    Helper class that wraps the Schema class so that we can define methods without
    reducing the possible names we can use for attributes.
    """

    def __init__(self, schema_class: Type[Schema], name: Optional[str] = None) -> None:
        """
        Initialize the SchemaHelper object

        `schema_class` - a Schema subclass
        """
        if name is None:
            name = f"{schema_class.__name__}Snapshot"

        self.orig_schema_cls = schema_class
        self.snapshot_classes = self._get_snapshot_classes(name)
        self.snapshot_cls = self.snapshot_classes.pop(None)

        self.input_schema_cls = self.construct_marshmallow_schema(is_input=True)
        self.schema_cls = self.construct_marshmallow_schema(is_input=False)

    def _get_snapshot_classes(
        self, name: str
    ) -> Dict[Optional[str], Type[SchemaSnapshot]]:
        classes = {None: SchemaSnapshot.from_schema(self.orig_schema_cls, name)}

        for field_name, value in self.orig_schema_cls.__fields__.items():
            if isinstance(value, NestedField):
                classes[field_name] = SchemaSnapshot.from_schema(value.annotation, name)

        return classes

    def _snapshot_from_dict(self, data: Dict[str, Any]) -> SchemaSnapshot:
        out_data = {}

        for key, val in data.items():
            if key in self.snapshot_classes:
                out_data[key] = self.snapshot_classes[key](**val)
            else:
                out_data[key] = val

        return self.snapshot_cls(**out_data)

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
        field = self.orig_schema_cls[name]
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
                errors[name] = [str(error)]

        if len(errors) > 0:
            raise exc.InputValidationError(errors)

    def load_input(self, data: Dict[str, Any]) -> SchemaSnapshot:
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
                unknown=ma.RAISE, exclude=tuple(symbols)
            ).load(values)
        except ma.ValidationError as error:
            errors.append(error)

        if len(errors) > 0:
            msg = {}
            for error in errors:
                msg.update(error.messages)
            raise exc.InputValidationError(msg)

        input_data.update(symbols)
        return self._snapshot_from_dict(input_data)

    @convert_ma_validation_error
    def load(self, data: Dict[str, Any], validate: bool = True) -> SchemaSnapshot:
        """
        Load previously serialized data about this resource's state
        """
        if validate:
            data = self.schema_cls(unknown=ma.RAISE).load(data)
        return self._snapshot_from_dict(data)

    @convert_ma_validation_error
    def dump(self, data: SchemaSnapshot) -> Dict[str, Any]:
        """
        Dump state data to a serialized version.
        """
        return self.schema_cls(unknown=ma.RAISE).dump(data)
