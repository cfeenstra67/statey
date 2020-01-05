"""
Change classes are used to apply changes to resources
"""
import abc
from typing import Optional, Set

from statey.schema import SchemaSnapshot


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
    async def apply(self) -> Optional[SchemaSnapshot]:
        """
		Apply this change.
		"""
        raise NotImplementedError


class NoChange(Change):
    """
	Change class indicating a no-op on a resource
	"""

    null = True

    async def apply(self) -> SchemaSnapshot:
        return self.snapshot


class Create(Change):
    """
	Create a resource
	"""

    async def apply(self) -> SchemaSnapshot:
        return await self.resource.create(self.snapshot)


class Delete(Change):
    """
	Delete a resource
	"""

    async def apply(self) -> None:
        return await self.resource.destroy(self.old_snapshot)


class DeleteAndRecreate(Change):
    """
	Delete a resource and recreate it from scratch
	"""

    async def apply(self) -> SchemaSnapshot:
        self.resource.destroy(self.old_snapshot)
        return await self.resource.create(self.snapshot)


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
        fields: Set[str],
    ) -> None:
        super().__init__(resource, snapshot, old_snapshot)
        self.fields = fields

    async def apply(self) -> SchemaSnapshot:
        return await self.resource.update(self.old_snapshot, self.snapshot, self)
