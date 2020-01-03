"""
Change classes are used to apply changes to resources
"""
import abc
from typing import Dict, Any, Tuple, Optional

from statey.schema import SchemaSnapshot, Field


# pylint: disable=too-few-public-methods
class Change(abc.ABC):
    """
	Base class for resource changes. A change is some action performed on a resource
	"""

    null = False

    def __init__(
        self,
        resource: "Resource",
        snapshot: SchemaSnapshot,
        old_snapshot: Optional[SchemaSnapshot] = None,
    ) -> None:
        """
		Set up Change objects. Each change object contains a resource
		and a specific snapshot of some state of that resource
		"""
        self.resource = resource
        self.snapshot = snapshot
        self.old_snapshot = old_snapshot

    @abc.abstractmethod
    def apply(self) -> SchemaSnapshot:
        """
		Apply this change.
		"""
        raise NotImplementedError


class NoChange(Change):
    """
	Change class indicating a no-op on a resource
	"""

    null = True

    def apply(self) -> SchemaSnapshot:
        return self.snapshot


class Create(Change):
    """
	Create a resource
	"""

    def apply(self) -> SchemaSnapshot:
        return self.resource.create(self.snapshot)


class Delete(Change):
    """
	Delete a resource
	"""

    def apply(self) -> SchemaSnapshot:
        return self.resource.destroy(self.old_snapshot)


class DeleteAndRecreate(Change):
    """
	Delete a resource and recreate it from scratch
	"""

    def apply(self) -> SchemaSnapshot:
        self.resource.destroy(self.old_snapshot)
        return self.resource.create(self.snapshot)


class Update(Change):
    """
	Update a resource's attributes. The attributes can be considered
	safe to use without any validation
	"""

    def __init__(
        self,
        resource: "Resource",
        snapshot: SchemaSnapshot,
        old_snapshot: SchemaSnapshot,
        values: Dict[Field, Tuple[Any, Any]],
    ) -> None:
        super().__init__(resource, snapshot, old_snapshot)
        self.values = values

    def apply(self) -> SchemaSnapshot:
        return self.resource.update(self.old_snapshot, self.snapshot, self)
