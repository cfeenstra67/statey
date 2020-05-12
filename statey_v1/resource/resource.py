"""
A resource class provides an implementation for entities that can be managed by
a statey resource graph
"""
import abc
import copy
from typing import Dict, Any, Optional, Type, Tuple

from statey import exc
from statey.schema import (
    Field,
    SchemaSnapshot,
    Reference,
    Schema,
    SchemaHelper,
    SchemaReference,
    MISSING,
)


class ResourceMeta(abc.ABCMeta):
    """
    Metaclass for the Resource class. Enables __getitem__ support on resource
    classes to set the name of the resource
    """

    type_name = None

    _type_cache = {}

    def __getitem__(cls, annotation: Any) -> Type["Resource"]:
        """
        Using __getitem__ syntax, we can set names for resources using a syntax
        like: Resource['my_resource'](a=b, c=d, ...)
        """
        if not isinstance(annotation, str):
            raise ValueError(
                f"The __getitem__ syntax for resources is used to set a default name"
                f" for the resource. The value must be a string, got "
                f"{repr(annotation)}."
            )

        def factory(*args, **kwargs):
            resource = cls(*args, **kwargs)
            # pylint: disable=attribute-defined-outside-init,protected-access
            resource._name = annotation
            return resource

        return factory

    def from_snapshot(cls, snapshot: SchemaSnapshot) -> "Resource":
        """
        Generate a new Resource instance from a snapshot
        """
        data = {}
        refs = []

        for key, val in snapshot.items():
            # Skip computed/nonexistent fields
            if key not in cls.Schema.__fields__:
                continue
            field = cls.Schema.__fields__[key]
            if field.computed:
                continue
            if field.input and val is not MISSING:
                data[key] = val
            else:
                empty_ref = data[key] = Reference(None, key, field)
                refs.append(empty_ref)

        # pylint: disable=no-value-for-parameter,arguments-differ
        instance = cls(data)

        for ref in refs:
            ref.resource = instance

        return instance

    def find(cls, type_name: str) -> Optional[Type["Resource"]]:
        """
        Get a resource class by type name
        """
        return cls._type_cache.get(type_name)

    # pylint: disable=arguments-differ
    def __new__(
        cls, name: str, bases: Tuple[Type, ...], attrs: Dict[str, Any]
    ) -> "ResourceMeta":
        if "type_name" not in attrs:
            raise exc.InitializationError(
                f"type_name must be defined for every new Resource subclass."
            )

        new_cls = super().__new__(cls, name, bases, attrs)

        type_name = attrs["type_name"]
        if type_name in cls._type_cache:
            existing = cls._type_cache[type_name]
            raise exc.InitializationError(
                f'An existing Resource subclass exists with type_name "{type_name}": '
                f"{existing}. Unable to initialize {new_cls}."
            )

        cls._type_cache[type_name] = new_cls

        return new_cls


class Resource(abc.ABC, metaclass=ResourceMeta):
    """
    A resource can be any entity with the following properties:
    - The entity be created, destroyed, and (optionally) updated
    - The state of the entity can be refreshed given the attributes in the schema
    """

    type_name: str = "resource"

    Schema: Type[Schema] = Schema

    Credentials: Optional[Type[Credentials]] = None

    # class Schema:
    #     some_field: Field[int]
    #     other_field: Field[bool](optional=True)

    #     @st.schema.wrap(List)
    #     @st.schema.nested
    #     class A:
    #         b: Field[int]
    #         blah_blah_blah: Field[str]

    # # We can 
    # class Creds:
    #     pg_main: PostgresCredentials
    #     aws_main: AwsCredentials
    #     pg_replica: PostgresCredentials

    def __init__(self, *args, **kwargs) -> None:
        if len(args) > 1:
            raise exc.InitializationError(
                "No more than one positional argument can be passed to a resource."
            )

        if len(kwargs) > 0 and len(args) > 0:
            raise exc.InitializationError(
                "Resources can be initialized using a dictionary or keyword arguments"
                " but not both."
            )

        self.ref = SchemaReference(self, Field[self.Schema]())
        self.f = self.ref.f

        self.schema_helper = SchemaHelper(self.Schema, f"{type(self).__name__}Snapshot")

        # Configure snapshot instance
        field_data = kwargs or (args and args[0]) or {}
        self.snapshot = self.schema_helper.load_input(field_data)
        self.snapshot = self.snapshot.fill_missing_values(self)

        # The name cannot be set via an argument, it must be set after the
        # instance is initialized. It should not be set by users, but via
        # __getitem__ syntax or the `name` argument in graph.add
        self._name = None

    def copy(self) -> "Resource":
        """
        Copy the current resource, excluding any graph information
        """
        naive = copy.copy(self)
        return naive

    @property
    def name(self):
        """
        Retrieve the name of this resource
        """
        return self._name

    def __str__(self) -> str:
        return f"{type(self).__name__}(type_name={self.type_name}, name={self.name})"

    @abc.abstractmethod
    async def create(self, current: SchemaSnapshot) -> SchemaSnapshot:
        """
        Create this resource. Return the latest snapshot
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def destroy(self, current: SchemaSnapshot) -> None:
        """
        Destroy this resource
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def refresh(self, current: SchemaSnapshot) -> Optional[SchemaSnapshot]:
        """
        Refresh the state of this resource

        Returns Snapshot if the resource exists, otherwise None
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def update(
        self, old: SchemaSnapshot, current: SchemaSnapshot, spec: "Update"
    ) -> SchemaSnapshot:
        """
        Update this resource with the values given by `spec`.
        """
        raise NotImplementedError
