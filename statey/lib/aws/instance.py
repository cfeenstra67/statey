import asyncio
import contextlib
from typing import Dict, Any, Optional

import aioboto3
import botocore

import statey as st
from statey.fsm import transition, MachineResource, SimpleMachine


InstanceConfigType = st.S.Struct[
    "ami" : st.S.String, "instance_type" : st.S.String, "key_name" : st.S.String
].t


InstanceType = st.S.Struct[
    "ami" : st.S.String,
    "instance_type" : st.S.String,
    "key_name" : st.S.String,
    # Exported
    "id" : st.S.String,
    "ebs_optimized" : st.S.Boolean,
    "placement_group" : st.S.String,
    "public_ip" : ~st.S.String,
    "public_dns" : st.S.String,
    "private_ip" : st.S.String,
    "private_dns" : st.S.String,
].t


class InstanceMachine(SimpleMachine):
    """
    Machine for an EC2 Instance
    """

    UP = st.State("UP", InstanceConfigType, InstanceType)

    @contextlib.asynccontextmanager
    async def resource_ctx(self):
        async with aioboto3.resource("ec2") as ec2:
            yield ec2

    @staticmethod
    async def convert_instance(instance: "Instance") -> Dict[str, Any]:
        out = {"id": instance.id}
        (
            out["ami"],
            out["ebs_optimized"],
            out["instance_type"],
            placement,
            out["private_ip"],
            out["private_dns"],
            out["public_ip"],
            out["public_dns"],
            out["key_name"],
        ) = await asyncio.gather(
            instance.image_id,
            instance.ebs_optimized,
            instance.instance_type,
            instance.placement,
            instance.private_ip_address,
            instance.private_dns_name,
            instance.public_ip_address,
            instance.public_dns_name,
            instance.key_name,
        )
        out["placement_group"] = placement["GroupName"]
        return out

    async def refresh_state(self, data: Any) -> Optional[Any]:
        async with self.resource_ctx() as ec2:
            instance = await ec2.Instance(data["id"])
            try:
                await instance.load()
            except botocore.exceptions.ClientError:
                return None
            return await self.convert_instance(instance)

    async def create_task(self, config: InstanceConfigType) -> InstanceType:
        """
        Create a new EC2 Instance
        """
        async with self.resource_ctx() as ec2:
            (instance,) = await ec2.create_instances(
                ImageId=config["ami"],
                InstanceType=config["instance_type"],
                KeyName=config["key_name"],
                MinCount=1,
                MaxCount=1,
            )
            # Checkpoint after creation
            yield await self.convert_instance(instance)
            await instance.wait_until_running()
            await instance.load()
            yield await self.convert_instance(instance)

    async def delete_task(self, current: InstanceType) -> st.EmptyType:
        """
        Delete the EC2 Instance
        """
        async with self.resource_ctx() as ec2:
            instance = await ec2.Instance(current["id"])
            await instance.terminate()
            yield {}
            await instance.wait_until_terminated()


instance_resource = MachineResource("aws_instance", InstanceMachine)

# Resource state factory
Instance = instance_resource.s


RESOURCES = [instance_resource]


def register(registry: Optional["Registry"] = None) -> None:
    """
    Register resources in this module
    """
    if registry is None:
        registry = st.registry

    for resource in RESOURCES:
        registry.register_resource(resource)
