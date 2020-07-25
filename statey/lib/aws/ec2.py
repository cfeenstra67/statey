import asyncio
import contextlib
from typing import Dict, Any, Optional

import aioboto3
import botocore

import statey as st
from statey.fsm import transition, MachineResource, SimpleMachine


EC2InstanceConfigType = st.S.Struct[
    "ami": st.S.String,
    "instance_type": st.S.String,
    "key_name": st.S.String
].t


EC2InstanceType = st.S.Struct[
    "ami": st.S.String,
    "instance_type": st.S.String,
    "key_name": st.S.String,
    # Exported
    "id": st.S.String,
    "ebs_optimized": st.S.Boolean,
    "placement_group": st.S.String,
    "public_ip": ~st.S.String,
    "public_dns": st.S.String,
    "private_ip": st.S.String,
    "private_dns": st.S.String
].t


class EC2InstanceMachine(SimpleMachine):
    """
    Machine for an EC2 Instance
    """
    UP = st.State("UP", EC2InstanceConfigType, EC2InstanceType)

    @contextlib.asynccontextmanager
    async def resource_ctx(self):
        async with aioboto3.resource('ec2') as ec2:
            yield ec2

    def get_expected(self, config: st.StateConfig) -> Any:
        unknown = st.Object(st.Unknown(return_type=EC2InstanceType))
        return {
            "ami": config.data['ami'],
            "instance_type": config.data['instance_type'],
            'key_name': config.data['key_name'],
            "ebs_optimized": unknown.ebs_optimized,
            "public_ip": unknown.public_ip,
            "id": unknown.id,
            "placement_group": unknown.placement_group,
            "public_dns": unknown.public_dns,
            "private_ip": unknown.private_ip,
            "private_dns": unknown.private_dns
        }

    @staticmethod
    async def convert_instance(instance: "Instance") -> Dict[str, Any]:
        out = {'id': instance.id}
        (
            out['ami'],
            out['ebs_optimized'],
            out['instance_type'],
            placement,
            out['private_ip'],
            out['private_dns'],
            out['public_ip'],
            out['public_dns'],
            out['key_name']
        ) = await asyncio.gather(
            instance.image_id,
            instance.ebs_optimized,
            instance.instance_type,
            instance.placement,
            instance.private_ip_address,
            instance.private_dns_name,
            instance.public_ip_address,
            instance.public_dns_name,
            instance.key_name
        )
        out['placement_group'] = placement['GroupName']
        return out

    async def refresh_state(self, data: Any) -> Optional[Any]:
        async with self.resource_ctx() as ec2:
            instance = await ec2.Instance(data['id'])
            try:
                await instance.load()
            except botocore.exceptions.ClientError:
                return None
            return await self.convert_instance(instance)

    async def create_task(self, config: EC2InstanceConfigType) -> EC2InstanceType:
        """
        Create a new EC2 Instance
        """
        async with self.resource_ctx() as ec2:
            instance, = await ec2.create_instances(
                ImageId=config['ami'],
                InstanceType=config['instance_type'],
                KeyName=config['key_name'],
                MinCount=1,
                MaxCount=1
            )
            return await self.convert_instance(instance)

    async def delete_task(self, current: EC2InstanceType) -> st.EmptyType:
        """
        Delete the EC2 Instance
        """
        async with self.resource_ctx() as ec2:
            instance = await ec2.Instance(current['id'])
            await instance.terminate()
            return {}

    async def modify_task(self, current: EC2InstanceType, config: EC2InstanceConfigType) -> EC2InstanceType:
        """
        Modify the EC2 Instance
        """
        # Should not be called
        raise NotImplementedError


ec2_instance_resource = MachineResource("ec2_instance", EC2InstanceMachine)

# Resource state factory
EC2Instance = ec2_instance_resource.s


RESOURCES = [
    ec2_instance_resource
]


def register() -> None:
    """
    Register resources in this module
    """
    for resource in RESOURCES:
        st.registry.register_resource(resource)
