try:
    import aioboto3
except ImportError as err:
    raise RuntimeError("Aioboto3 is required to use AWS resources.") from err


from .ec2 import EC2Instance, EC2InstanceType, EC2InstanceConfigType

from .security_group import SecurityGroup, SecurityGroupType, SecurityGroupConfigType


def register() -> None:
    from . import ec2, security_group

    ec2.register()
    security_group.register()
