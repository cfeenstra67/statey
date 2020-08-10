import asyncio
import contextlib
from typing import Dict, Any, Optional

import aioboto3
import botocore

import statey as st


VpcConfigType = st.Struct[
    "cidr_block":str,
    "instance_tenancy":str,
    # "enable_dns_support": st.S.Boolean,
    # "enable_dns_hostnames": st.S.Boolean,
    # "enable_classiclink": st.S.Boolean,
    # "enable_classiclink_dns_support": st.S.Boolean,
    "assign_generated_ipv6_cidr_block":bool,
    # Missing: Tags
]


VpcType = st.Struct[
    "cidr_block":str,
    "instance_tenancy":str,
    # "enable_dns_support": st.S.Boolean,
    # "enable_dns_hostnames": st.S.Boolean,
    # "enable_classiclink": st.S.Boolean,
    # "enable_classiclink_dns_support": st.S.Boolean,
    "assign_generated_ipv6_cidr_block":bool,
    "id":str,
    "main_route_table_id" : Optional[str],
    "default_network_acl_id" : Optional[str],
    "ipv6_association_id" : Optional[str],
    "ipv6_cidr_block" : Optional[str],
    "owner_id":int,
    # Missing: Tags
]


class VpcMachine(st.SimpleMachine):
    """
    AWS VPC resource
    """

    UP = st.State("UP", VpcConfigType, VpcType)

    @contextlib.asynccontextmanager
    async def resource_ctx(self):
        async with aioboto3.resource("ec2") as ec2:
            yield ec2

    @contextlib.asynccontextmanager
    async def client_ctx(self):
        async with aioboto3.client("ec2") as client:
            yield client

    async def convert_instance(
        self, data: Dict[str, Any], vpc: "Vpc"
    ) -> Dict[str, Any]:
        out = {
            "id": vpc.id,
            "assign_generated_ipv6_cidr_block": data[
                "assign_generated_ipv6_cidr_block"
            ],
        }
        (
            out["cidr_block"],
            out["instance_tenancy"],
            ipv6_associations,
            out["owner_id"],
        ) = await asyncio.gather(
            vpc.cidr_block,
            vpc.instance_tenancy,
            vpc.ipv6_cidr_block_association_set,
            vpc.owner_id,
        )
        if ipv6_associations:
            association = ipv6_associations[0]
            out["ipv6_association_id"] = association["AssociationId"]
            out["ipv6_cidr_block"] = association["Ipv6CidrBlock"]
        else:
            out["ipv6_association_id"] = None
            out["ipv6_cidr_block"] = None

        async with self.client_ctx() as client:
            main_rt_resp = await client.describe_route_tables(
                Filters=[
                    {"Name": "vpc-id", "Values": [vpc.id]},
                    {"Name": "association.main", "Values": ["true"]},
                ]
            )
            if main_rt_resp["RouteTables"]:
                out["main_route_table_id"] = main_rt_resp["RouteTables"][0][
                    "RouteTableId"
                ]
            else:
                out["main_route_table_id"] = None

            default_acl_resp = await client.describe_network_acls(
                Filters=[
                    {"Name": "vpc-id", "Values": [vpc.id]},
                    {"Name": "default", "Values": ["true"]},
                ]
            )
            if default_acl_resp["NetworkAcls"]:
                out["default_network_acl_id"] = default_acl_resp["NetworkAcls"][0][
                    "NetworkAclId"
                ]
            else:
                out["default_network_acl_id"] = None

        return out

    async def refresh_state(self, data: Any) -> Optional[Any]:
        async with self.resource_ctx() as ec2:
            instance = await ec2.Vpc(data["id"])
            try:
                await instance.load()
            except botocore.exceptions.ClientError:
                return None
            return await self.convert_instance(data, instance)

    async def create_task(self, config: VpcConfigType) -> VpcType:
        """
        Create a new VPC
        """
        async with self.resource_ctx() as ec2:
            vpc = await ec2.create_vpc(
                CidrBlock=config["cidr_block"],
                InstanceTenancy=config["instance_tenancy"],
                AmazonProvidedIpv6CidrBlock=config["assign_generated_ipv6_cidr_block"],
            )
            yield await self.convert_instance(config, vpc)
            await vpc.wait_until_available()
            await vpc.load()
            yield await self.convert_instance(config, vpc)

    async def delete_task(self, current: VpcType) -> st.EmptyType:
        """
        Delete the VPC
        """
        async with self.resource_ctx() as ec2:
            instance = await ec2.Vpc(current["id"])
            await instance.delete()
            return {}


vpc_resource = st.MachineResource("aws_vpc", VpcMachine)

Vpc = vpc_resource.s

RESOURCES = [vpc_resource]


def register(registry: Optional["Registry"] = None) -> None:
    """
    Register resources in this module
    """
    if registry is None:
        registry = st.registry

    for resource in RESOURCES:
        registry.register_resource(resource)
