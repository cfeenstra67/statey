from typing import Optional

try:
    import aioboto3
except ImportError as err:
    raise RuntimeError("Aioboto3 is required to use AWS resources.") from err


from .instance import Instance, InstanceType, InstanceConfigType

from .security_group import SecurityGroup, SecurityGroupType, SecurityGroupConfigType

from .subnet import Subnet, SubnetType, SubnetConfigType

from .vpc import Vpc, VpcType, VpcConfigType


def register(registry: Optional["Registry"] = None) -> None:
    from . import instance, security_group, vpc, subnet

    if registry is None:
        from statey import registry

    instance.register(registry)
    security_group.register(registry)
    vpc.register(registry)
    subnet.register(registry)
