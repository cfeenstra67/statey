import abc
import dataclasses as dc
from typing import Any, Callable, Optional, Type as PyType, Sequence, Dict, Union

import statey as st
from statey.syms import types, utils, encoders, impl, Object


class Mapper(abc.ABC):
    """
    A mapper is a "protected" execution of a function. The input and output
    values are encoded based on the types of input_type() and output_type()
    """

    input_type: types.Type
    output_type: types.Type

    @abc.abstractmethod
    def transform(self, symbol: Object, registry: st.Registry) -> Any:
        """
        Process encoded data, should produce an output that can be encoded by output_encoder().type
        """
        raise NotImplementedError

    def map(self, data: Any, registry: st.Registry) -> Any:
        """
        Map the input data to output data. This data may be fully or partially symbollic.
        """
        input_encoder = registry.get_encoder(self.input_type)
        input_data = input_encoder.encode(data)
        input_symbol = Object(impl.Data(input_data), self.input_type, registry)

        output_symbol = self.transform(input_symbol, registry)

        output_encoder = registry.get_encoder(self.output_type)
        output_data = output_encoder.encode(output_symbol)
        return output_data


class Validator(abc.ABC):
    """
    A validator validates some input data
    """

    @abc.abstractmethod
    def validate(self, data: Any) -> None:
        """
        Validate input data
        """
        raise NotImplementedError


@dc.dataclass(frozen=True)
class ValidatorSet(Validator, utils.Cloneable):
    """

    """

    validators: Sequence[Callable[[Any], None]] = ()

    def validate(self, data: Any) -> None:
        for validate in self.validators:
            validate(date)


NullValidator = ValidatorSet()


class Schema(Mapper):
    """
    A schema is a structured object that provides easy data validation and computed
    properties
    """

    metadata: Dict[str, Any]
    validator: Validator

    def __call__(
        self, arg: Any = utils.MISSING, **kwargs: Dict[str, Any]
    ) -> Object:
        if arg is utils.MISSING:
            arg = kwargs
        return self.map(arg, st.registry)

    @abc.abstractmethod
    def attr_schema(self, attr: Any) -> Optional["Schema"]:
        """
        Retrieve the schema at the given attribute from this one
        """
        raise NotImplementedError

    def path_schema(self, path: Sequence[Any]) -> Optional["Schema"]:
        """

        """
        base_schema = self
        for attr in path:
            base_schema = base_schema.attr_schema(attr)
            if base_schema is None:
                raise AttributeError(path)
        return base_schema


@dc.dataclass(frozen=True)
class StructSchemaField:
    """
    Contains information about a single field in a StructSchema
    """

    name: str
    schema: Schema
    attr: bool = True


@dc.dataclass(frozen=True)
class StructSchema(Schema):
    """

    """

    fields: Sequence[StructSchemaField]
    validator: Validator = dc.field(default=NullValidator)
    metadata: Dict[str, Any] = dc.field(default_factory=dict)

    @property
    def output_type(self) -> types.Type:
        out_fields = []
        for field in self.fields:
            out_fields.append(
                types.StructField(name=field.name, type=field.schema.output_type)
            )
        nullable = all(field.type.nullable for field in out_fields)
        return types.StructType(tuple(out_fields), nullable)

    @property
    def input_type(self) -> types.Type:
        in_fields = []
        for field in self.fields:
            if field.attr:
                in_fields.append(
                    types.StructField(name=field.name, type=field.schema.input_type)
                )
        nullable = all(field.type.nullable for field in in_fields)
        return types.StructType(tuple(in_fields), nullable)

    def transform(self, symbol: Object, registry: st.Registry) -> Any:
        def wrapped_validate(x):
            self.validator.validate(x)
            return x

        symbol = symbol.__inst.map(wrapped_validate)

        out = {}
        for field in self.fields:
            input = symbol[field.name] if field.attr else symbol
            out[field.name] = field.schema.transform(input, registry)

        return Object(impl.Data(out), self.output_type, registry)

    def attr_schema(self, attr: Any) -> Optional[Schema]:
        field_map = {field.name: field.schema for field in self.fields}
        if attr not in field_map:
            return None
        return field_map[attr]


EmptySchema = StructSchema(())


@dc.dataclass(frozen=True)
class ArraySchema(Schema):
    """

    """

    element_schema: Schema
    validator: Validator = dc.field(default=NullValidator)
    metadata: Dict[str, Any] = dc.field(default_factory=dict)

    @property
    def output_type(self) -> types.Type:
        element_type = self.element_schema.output_type
        return types.ArrayType(element_type, element_type.nullable)

    @property
    def input_type(self) -> types.Type:
        element_type = self.element_schema.input_type
        return types.ArrayType(element_type, element_type.nullable)

    def transform(self, symbol: Object, registry: st.Registry) -> Any:
        def validate_and_transform(data):
            self.validator.validate(data)
            if data is None:
                return None

            out = []
            element_semantics = registry.get_semantics(self.element_schema.input_type)
            for item in data:
                lit = Object(impl.Data(item), self.element_schema.input_type, registry)
                out.append(self.element_schema.transform(lit, registry))

            return out

        return symbol.__inst.map(validate_and_transform)

    def attr_schema(self, attr: Any) -> Optional[Schema]:
        if isinstance(attr, int):
            return self.element_schema
        if isinstance(attr, slice):
            return self
        return None


@dc.dataclass(frozen=True)
class ValueSchema(Schema):
    """

    """

    input_type: types.Type
    output_type: types.Type = dc.field(default=utils.MISSING)
    mapper: Callable[[Any], Any] = lambda x: x
    validator: Validator = dc.field(default=NullValidator)
    metadata: Dict[str, Any] = dc.field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.output_type is utils.MISSING:
            self.__dict__["output_type"] = self.input_type

    def transform(self, symbol: Object, registry: st.Registry) -> Any:
        def validate(data):
            self.validator.validate(data)
            return data

        symbol = symbol.__inst.map(validate)
        return self.mapper(symbol)

    def attr_schema(self, attr: Any) -> Optional[Schema]:
        return None


class SchemaFactory(abc.ABC, utils.Cloneable):
    """
    Schema factories provide an easy, expressive API for creating complex schemas
    """

    metadata: Dict[str, Any]
    validator: Validator
    nullable: bool

    def __invert__(self) -> "SchemaFactory":
        return self.clone(nullable=True)

    @abc.abstractmethod
    def schema(self) -> Schema:
        """
        Build a schema from this factory
        """
        raise NotImplementedError

    @property
    def s(self) -> Schema:
        """
        Alias for build()
        """
        return self.schema()

    def type(self) -> types.Type:
        return self.schema().output_type

    @property
    def t(self):
        """
        Convenient way to get the type of the underlying schema
        """
        return self.type()

    def __call__(self, **kwargs: Dict[str, Any]) -> "ComputedSchemaFactory":
        meta = self.metadata.copy()
        validator = kwargs.pop("validator", self.validator)
        nullable = kwargs.pop("nullable", self.nullable)
        meta.update(kwargs)
        return self.clone(metadata=meta, validator=validator, nullable=nullable)


@dc.dataclass(frozen=True)
class ValueSchemaFactory(SchemaFactory, utils.Cloneable):
    """

    """

    type_cls: PyType[types.Type]
    nullable: bool = False
    validator: Validator = dc.field(default=NullValidator)
    mapper: Callable[[Any], Any] = lambda x: x
    metadata: Dict[str, Any] = dc.field(default_factory=dict)

    def schema(self) -> Schema:
        typ = self.type_cls(self.nullable)
        return ValueSchema(
            input_type=typ,
            output_type=typ,
            validator=self.validator,
            metadata=self.metadata,
            mapper=lambda x: x.map(self.mapper),
        )

    def __call__(
        self, *, compute: Callable[[Any], Any] = utils.MISSING, **kwargs: Dict[str, Any]
    ) -> "ValueSchemaFactory":
        """

        """
        meta = self.metadata.copy()
        validator = kwargs.pop("validator", self.validator)
        nullable = kwargs.pop("nullable", self.nullable)
        mapper = kwargs.pop("mapper", self.mapper)
        meta.update(kwargs)

        kws = {
            "validator": validator,
            "metadata": meta,
            "nullable": nullable,
            "mapper": mapper,
        }

        if compute is not utils.MISSING:
            return ComputedSchemaFactory(type_cls=self.type_cls, mapper=compute, **kws)

        return self.clone(**kws)


# Note that this is NOT a schema factory, since they can't be built on their
# own.
@dc.dataclass(frozen=True)
class ComputedSchemaFactory(utils.Cloneable):
    """

    """

    type_cls: PyType[types.Type]
    nullable: bool = False
    mapper: Callable[[Any], Any] = lambda x: x
    validator: Validator = dc.field(default=NullValidator)
    metadata: Dict[str, Any] = dc.field(default_factory=dict)

    def __invert__(self) -> None:
        return self.clone(nullable=True)

    def schema(self, input_type: types.Type) -> Schema:
        typ = self.type_cls(self.nullable)
        return ValueSchema(
            input_type=input_type,
            output_type=typ,
            validator=self.validator,
            metadata=self.metadata,
            mapper=self.mapper,
        )


@dc.dataclass(frozen=True)
class ArraySchemaFactory(SchemaFactory, utils.Cloneable):
    """

    """

    # ignored
    nullable: bool = False
    element_schema: Schema = dc.field(default=utils.MISSING)
    validator: Validator = dc.field(default=NullValidator)
    metadata: Dict[str, Any] = dc.field(default_factory=dict)

    def schema(self) -> Schema:
        schema = self.element_schema
        if schema is utils.MISSING:
            schema = EmptySchema
        if isinstance(schema, SchemaFactory):
            schema = schema.schema()

        return ArraySchema(
            element_schema=schema, validator=self.validator, metadata=self.metadata
        )

    def __getitem__(
        self, element_schema: Union[Schema, "SchemaFactory"]
    ) -> "ArraySchemaFactory":
        if isinstance(element_schema, SchemaFactory):
            element_schema = element_schema.schema()
        return self.clone(element_schema=element_schema)


@dc.dataclass(frozen=True)
class StructSchemaFactory(SchemaFactory, utils.Cloneable):
    """
    Builder for struct schemas
    """

    # ignored
    nullable: bool = False
    fields: Sequence[StructSchemaField] = dc.field(default_factory=tuple)
    validator: Validator = dc.field(default=NullValidator)
    metadata: Dict[str, Any] = dc.field(default_factory=dict)

    def schema(self) -> Schema:
        return StructSchema(
            fields=self.fields, validator=self.validator, metadata=self.metadata
        )

    def add(
        self, name: str, schema: Union["SchemaFactory", Schema, ComputedSchemaFactory]
    ) -> "StructSchemaFactory":
        """
        Add a field to this schema factory
        """
        if isinstance(schema, SchemaFactory):
            field = StructSchemaField(name, schema.schema())
        if isinstance(schema, ComputedSchemaFactory):
            schema = schema.schema(self.schema().input_type)
            field = StructSchemaField(name, schema, attr=False)
        return self.clone(fields=tuple(self.fields) + (field,))

    def __getitem__(self, fields: Sequence[slice]) -> "ArraySchemaFactory":
        """
        struct['a': integer, 'b': array[integer]] - use slices to add fields
        """
        inst = self
        if not isinstance(fields, (list, tuple)):
            fields = [fields]
        for tup in fields:
            inst = inst.add(tup.start, tup.stop)
        return inst
