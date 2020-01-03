"""
A resource class provides an implementation for entities that can be managed by
a statey resource graph
"""
import abc
import copy
from typing import Dict, Any, Optional, Type, Tuple

from statey import exc
from statey.schema import (
    SchemaSnapshot,
    Reference,
    Schema,
    SchemaHelper,
    MISSING,
)
from statey.utils.helpers import get_all_subclasses


class _AttrAccessor:
    """
    Helper to allow smart attribute access on resource.attrs objects
    """

    def __init__(self, resource: "Resource") -> None:
        self.resource = resource

    def __getattr__(self, name: str) -> Any:
        """
        Get a reference to a field in the resource's schema
        """
        field = getattr(self.resource.Schema, name)
        return Reference(self.resource, name, field)

    def __getitem__(self, key: str) -> Any:
        """
        Enable using the attribute accessor with __getitem__ syntax
        """
        try:
            return getattr(self, key)
        except AttributeError as exc:
            raise KeyError(key) from exc


class ResourceMeta(abc.ABCMeta):
    """
    Metaclass for the Resource class. Enables __getitem__ support on resource
    classes to set the name of the resource
    """

    type_name = None

    def __getitem__(cls, annotation: Any) -> Type["Resource"]:
        """
        Using __getitem__ syntax, we can set names for resources using a syntax
        like: Resource['my_resource'](a=b, c=d, ...)
        """
        if not isinstance(annotation, str):
            raise ValueError(
                f"The __getitem__ syntax for resources is used to set a default name"
                f" for the resource. The value must be a string, got {repr(annotation)}."
            )

        def factory(*args, **kwargs):
            resource = cls(*args, **kwargs)
            # pylint: disable=attribute-defined-outside-init,protected-access
            resource._name = annotation
            return resource

        return factory

    def find(cls, type_name: str) -> Optional[Type["Resource"]]:
        """
        Get a resource class by type name
        """
        for subcls in get_all_subclasses(cls):
            if subcls.type_name == type_name:
                return subcls
        return None

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

    # pylint: disable=arguments-differ
    def __new__(cls, name: str, bases: Tuple[Type, ...], attrs: Dict[str, Any]) -> "ResourceMeta":
        # Handle case where Resource is not yet defined. In that case, just init normally
        try:
            Resource
        except NameError:
            return super().__new__(cls, name, bases, attrs)

        if "type_name" not in attrs:
            raise exc.InitializationError(
                f"type_name must be defined for every new Resource subclass."
            )

        type_name = attrs["type_name"]
        existing = Resource.find(type_name)
        new_cls = super().__new__(cls, name, bases, attrs)

        if existing is not None:
            raise exc.InitializationError(
                f'An existing Resource subclass exists with type_name "{type_name}": '
                f"{repr(existing)}. Unable to initialize {repr(new_cls)}."
            )

        return new_cls


class Resource(abc.ABC, metaclass=ResourceMeta):
    """
    A resource can be any entity with the following properties:
    - The entity be created, destroyed, and (optional) updated
    - The state of the entity can be refreshed given the attributes in the schema
    """

    type_name = "resource"

    Schema = Schema

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

        self.attrs = _AttrAccessor(self)

        field_data = kwargs or (args and args[0]) or {}

        self.schema_helper = SchemaHelper(self.Schema)
        self.field_data = self.schema_helper.load_input(field_data)

        # Configure snapshot instance
        self.snapshot = self.schema_helper.snapshot_cls(**self.field_data)
        self.snapshot = self.snapshot.fill_missing_values(self)

        # The name cannot be set via an argument, it must be set after the
        # instance is initialized. It should not be set by users, but via
        # __getitem__ syntax or the `name` argument in session.add
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

    @abc.abstractmethod
    def create(self, current: SchemaSnapshot) -> SchemaSnapshot:
        """
        Create this resource. Return the latest snapshot
        """
        raise NotImplementedError

    @abc.abstractmethod
    def destroy(self, current: SchemaSnapshot) -> None:
        """
        Destroy this resource
        """
        raise NotImplementedError

    @abc.abstractmethod
    def refresh(self, current: SchemaSnapshot) -> Optional[SchemaSnapshot]:
        """
        Refresh the state of this resource

        Returns Snapshot if the resource exists, otherwise None
        """
        raise NotImplementedError

    @abc.abstractmethod
    def update(
        self, old: SchemaSnapshot, current: SchemaSnapshot, spec: "Update"
    ) -> SchemaSnapshot:
        """
        Update this resource with the values given by `spec`.
        """
        raise NotImplementedError
