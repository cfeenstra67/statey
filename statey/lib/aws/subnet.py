import asyncio
import contextlib
from typing import Dict, Any, Optional

import aioboto3
import botocore

import statey as st


SubnetConfigType = st.Struct[
    "vpc_id": st.String,
    "cidr_block": st.String,
    # Optional args
    "ipv6_cidr_block": ~st.String,
    "map_public_ip_on_launch": st.Boolean(default=False),
    "assign_ipv6_address_on_creation": st.Boolean(default=False),
    # Missing: tags
]


SubnetType = st.Struct[
    "vpc_id": st.String,
    "cidr_block": st.String,
    "ipv6_association_id": ~st.String,
    "ipv6_cidr_block": ~st.String,
    "map_public_ip_on_launch": st.Boolean,
    "assign_ipv6_address_on_creation": st.Boolean,
    # Missing: tags
    "id": st.String,
    "owner_id": st.Integer
]


class SubnetMachine(st.SimpleMachine):
    """
    Maching representing an AWS subnet
    """
    UP = st.State("UP", SubnetConfigType, SubnetType)

    @contextlib.asynccontextmanager
    async def resource_ctx(self):
        async with aioboto3.resource("ec2") as ec2:
            yield ec2

    @contextlib.asynccontextmanager
    async def client_ctx(self):
        async with aioboto3.client("ec2") as client:
            yield client

    @staticmethod
    async def convert_instance(subnet: "Subnet") -> Dict[str, Any]:
        out = {'id': subnet.id}
        ipv6_associations = []
        (
            out['owner_id'],
            out['cidr_block'],
            # ipv6_associations,
            out['map_public_ip_on_launch'],
            out['assign_ipv6_address_on_creation'],
            out['vpc_id']
        ) = await asyncio.gather(
            subnet.owner_id,
            subnet.cidr_block,
            # subnet.ipv6_cidr_block_assocation_set,
            subnet.map_public_ip_on_launch,
            subnet.assign_ipv6_address_on_creation,
            subnet.vpc_id
        )
        if ipv6_associations:
            association = ipv6_associations[0]
            out['ipv6_association_id'] = association['AssociationId']
            out['ipv6_cidr_block'] = association['Ipv6CidrBlock']
        else:
            out['ipv6_association_id'] = None
            out['ipv6_cidr_block'] = None
        return out

    async def refresh_state(self, data: Any) -> Optional[Any]:
        async with self.resource_ctx() as ec2:
            instance = await ec2.Subnet(data["id"])
            try:
                await instance.load()
            except botocore.exceptions.ClientError:
                return None
            return await self.convert_instance(instance)

    async def create_task(self, config: SubnetConfigType) -> SubnetType:
        """
        Create a new subnet
        """
        async with self.resource_ctx() as ec2, self.client_ctx() as client:
            kws = {
                'CidrBlock': config['cidr_block'],
                'VpcId': config['vpc_id']
            }
            if config['ipv6_cidr_block'] is not None:
                kws['Ipv6CidrBlock'] = config['ipv6_cidr_block']

            subnet = await ec2.create_subnet(**kws)
            yield await self.convert_instance(subnet)

            map_public_ip_on_launch = await subnet.map_public_ip_on_launch
            if map_public_ip_on_launch != config['map_public_ip_on_launch']:
                await client.modify_subnet_attribute(
                    MapPublicIpOnLaunch={'Value': config['map_public_ip_on_launch']},
                    SubnetId=subnet.id
                )
                await subnet.load()
                yield await self.convert_instance(subnet)

            assign_ipv6_address_on_creation = await subnet.assign_ipv6_address_on_creation
            if assign_ipv6_address_on_creation != config['assign_ipv6_address_on_creation']:
                await client.modify_subnet_attribute(
                    AssignIpv6AddressOnCreation={'Value': config['assign_ipv6_address_on_creation']},
                    SubnetId=subnet.id
                )
                await subnet.load()
                yield await self.convert_instance(subnet)

    async def delete_task(self, current: SubnetType) -> st.EmptyType:
        """
        Delete the subnet
        """
        async with self.resource_ctx() as ec2:
            subnet = await ec2.Subnet(current['id'])
            await subnet.delete()


subnet_resource = st.MachineResource("aws_subnet", SubnetMachine)

Subnet = subnet_resource.s

RESOURCES = [subnet_resource]


def register(registry: Optional["Registry"] = None) -> None:
    """
    Register resources in this module
    """
    if registry is None:
        registry = st.registry

    for resource in RESOURCES:
        registry.register_resource(resource)
