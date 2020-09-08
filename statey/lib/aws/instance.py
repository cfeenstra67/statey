import asyncio
from typing import Dict, Any, Optional

import botocore

import statey as st
from statey.lib.aws.base import AWSMachine


InstanceConfigType = st.Struct[
    # Required args
    "ami" : st.String,
    "instance_type" : st.String,
    "key_name" : st.String,
    # Optional args
    "vpc_security_group_ids" : ~st.Array[st.String],
    "subnet_id" : ~st.String,
    "tags": ~st.Map[st.String, st.String],
    "availability_zone": ~st.String,
    "placement_group": ~st.String,
    "tenancy": st.String(default='default'),
    "host_id": ~st.String,
    "cpu_core_count": ~st.Integer,
    "cpu_threads_per_core": ~st.Integer,
    "ebs_optimized": st.Boolean(default=False),
    "disable_api_termination": st.Boolean(default=False)
]


InstanceType = st.Struct[
    "ami":str,
    "instance_type":str,
    "key_name":str,
    "availability_zone": st.String,
    "tags": st.Map[st.String, st.String],
    "placement_group": st.String,
    "host_id": ~st.String,
    'tenancy': st.String,
    "cpu_core_count": ~st.Integer,
    "cpu_threads_per_core": ~st.Integer,
    "ebs_optimized": st.Boolean,
    "disable_api_termination": st.Boolean,
    # Exported
    "id": str,
    "ebs_optimized":bool,
    "public_ip" : Optional[str],
    "public_dns":str,
    "private_ip":str,
    "private_dns":str,
    "vpc_security_group_ids" : st.Array[st.String],
    "subnet_id" : st.String,
    "state": st.String
]


class InstanceMachine(st.SimpleMachine, AWSMachine):
    """
    Machine for an EC2 Instance
    """

    service: str = "ec2"

    UP = st.State("UP", InstanceConfigType, InstanceType)

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
            security_groups,
            out["subnet_id"],
            tags,
            placement,
            cpu_options,
            out["ebs_optimized"],
            termination_protection,
            state_info
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
            instance.security_groups,
            instance.subnet_id,
            instance.tags,
            instance.placement,
            instance.cpu_options,
            instance.ebs_optimized,
            instance.describe_attribute(Attribute='disableApiTermination'),
            instance.state
        )
        out["vpc_security_group_ids"] = [group["GroupId"] for group in security_groups]
        out["tags"] = {tag['Key']: tag['Value'] for tag in tags or []}
        out["placement_group"] = placement["GroupName"]
        out["availability_zone"] = placement["AvailabilityZone"]
        out['tenancy'] = placement['Tenancy']
        out['host_id'] = placement.get('HostId')
        out['disable_api_termination'] = termination_protection['DisableApiTermination']['Value']
        out['state'] = state_info['Name']

        if cpu_options:
            out['cpu_core_count'] = cpu_options.get('CoreCount')
            out['cpu_threads_per_core'] = cpu_options.get('ThreadsPerCore')
        else:
            out['cpu_core_count'] = None
            out['cpu_threads_per_core'] = None

        return out

    async def refresh_state(self, data: Any) -> Optional[Any]:
        async with self.resource_ctx() as ec2:
            instance = await ec2.Instance(data["id"])
            try:
                await instance.load()
            except botocore.exceptions.ClientError:
                return None
            return await self.convert_instance(instance)

    def get_diff(
        self,
        current: st.StateSnapshot,
        config: st.StateConfig,
        session: st.TaskSession,
    ) -> st.Diff:

        differ = session.ns.registry.get_differ(config.state.input_type)
        diffconfig = differ.config()

        def ignore_if_new_none(old, new):
            if new is None:
                return True
            # Pass it along to the element-level logic
            return NotImplemented

        diffconfig.set_comparison("subnet_id", ignore_if_new_none)
        diffconfig.set_comparison("vpc_security_group_ids", ignore_if_new_none)
        diffconfig.set_comparison("availability_zone", ignore_if_new_none)
        diffconfig.set_comparison("cpu_core_count", ignore_if_new_none)
        diffconfig.set_comparison("cpu_threads_per_core", ignore_if_new_none)

        def none_as_empty_dict(old, new):
            new = {} if new is None else new
            old = {} if old is None else old
            if new == old:
                return True
            return NotImplemented

        diffconfig.set_comparison("tags", none_as_empty_dict)

        def none_as_empty_string(old, new):
            new = '' if new is None else new
            old = '' if old is None else old
            if new == old:
                return True
            return NotImplemented

        diffconfig.set_comparison("placement_group", none_as_empty_string)

        current_as_config = st.filter_struct(current.obj, config.type)
        out_diff = differ.diff(current_as_config, config.obj, session, diffconfig)
        return out_diff

    async def get_expected(self, current: st.StateSnapshot, config: st.StateConfig) -> Any:
        output = st.Unknown[config.state.output_type]
        if not current.state.null:
            output = current.obj

        replaced = st.struct_replace(
            config.obj,
            False,
            subnet_id=st.ifnull(config.obj.subnet_id, output.subnet_id),
            vpc_security_group_ids=st.ifnull(
                config.obj.vpc_security_group_ids, output.vpc_security_group_ids
            ),
            tags=st.ifnull(config.obj.tags, {}),
            availability_zone=st.ifnull(config.obj.availability_zone, output.availability_zone),
            placement_group=st.ifnull(config.obj.placement_group, '')
        )

        return st.fill(replaced, config.state.output_type, output)

    async def create_task(self, config: InstanceConfigType) -> InstanceType:
        """
        Create a new EC2 Instance
        """
        async with self.resource_ctx() as ec2:
            kws = {
                "ImageId": config["ami"],
                "InstanceType": config["instance_type"],
                "KeyName": config["key_name"],
                "MinCount": 1,
                "MaxCount": 1,
                "DisableApiTermination": config['disable_api_termination'],
                "EbsOptimized": config['ebs_optimized']
            }
            if config["vpc_security_group_ids"] is not None:
                kws["SecurityGroupIds"] = config["vpc_security_group_ids"]
            if config["subnet_id"] is not None:
                kws["SubnetId"] = config["subnet_id"]

            tags = config["tags"] or {}
            tags_list = [{'Key': key, 'Value': value} for key, value in tags.items()]
            specs = []
            if tags_list:
                specs.append({
                    'ResourceType': 'instance',
                    'Tags': tags_list
                })

            kws['TagSpecifications'] = specs
            placement = kws["Placement"] = {"Tenancy": config['tenancy']}

            if config["availability_zone"] is not None:
                placement["AvailabilityZone"] = config["availability_zone"]

            if config["placement_group"] is not None:
                placement["GroupName"] = config['placement_group']

            if config['host_id'] is not None:
                placement["HostId"] = config['host_id']

            if config['cpu_core_count'] is not None:
                opts = kws['CpuOptions'] = {'CoreCount': config['cpu_core_count']}
                if config['cpu_threads_per_core'] is not None:
                    opts['ThreadsPerCore'] = config['cpu_threads_per_core']

            (instance,) = await ec2.create_instances(**kws)
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

    def get_action(self, diff: st.Diff) -> st.ModificationAction:
        if not diff:
            return st.ModificationAction.NONE
        if (
            'ami' in diff
            or 'instance_type' in diff
            or 'key_name' in diff
            or 'availability_zone' in diff
            or 'placement_group' in diff
            or 'tenancy' in diff
            or 'host_id' in diff
            or 'cpu_core_count' in diff
            or 'cpu_threads_per_core' in diff
        ):
            return st.ModificationAction.DELETE_AND_RECREATE
        return st.ModificationAction.MODIFY

    async def modify_task(
        self,
        diff: st.Diff,
        current: InstanceType,
        config: InstanceConfigType,
    ) -> InstanceType:
        """
        Modify the EC2 Instance
        """
        async with self.resource_ctx() as ec2:

            instance = await ec2.Instance(current['id'])
            await instance.load()

            # This means the new value is not null
            if 'subnet_id' in diff:
                kws = {'SubnetId': config['subnet_id']}
                if config['vpc_security_group_ids'] is not None:
                    kws['Groups'] = config['vpc_security_group_ids']
                new_ni = await ec2.create_network_interface(**kws)
                current_ni_data = (await instance.network_interfaces_attribute)[0]
                current_ni = await ec2.NetworkInterface(current_ni_data['NetworkInterfaceId'])
                await current_ni.detach()
                await new_ni.attach(
                    DeviceIndex=0,
                    InstanceId=current['id']
                )

                await instance.load()
                yield await self.convert_instance(instance)

            elif 'vpc_security_group_ids' in diff:
                current_ni_data = (await instance.network_interfaces_attribute)[0]
                current_ni = await ec2.NetworkInterface(current_ni_data['NetworkInterfaceId'])
                group_ids = config['vpc_security_group_ids']
                if not group_ids:
                    await current_ni.detach()
                else:
                    await current_ni.modify_attribute(Groups=group_ids)

                await instance.load()
                yield await self.convert_instance(instance)

            if 'tags' in diff:
                new_tags = config['tags'] or {}
                remove_tags = [key for key in current['tags'] if key not in new_tags]
                if remove_tags:
                    await instance.delete_tags(Tags=[{'Key': key} for key in remove_tags])

                set_tags = [{'Key': key, 'Value': val} for key, val in new_tags.items()]
                if set_tags:
                    await instance.create_tags(Tags=set_tags)

                await instance.load()
                yield await self.convert_instance(instance)

            if 'disable_api_termination' in diff:
                await instance.modify_attribute(
                    Attribute='disableApiTermination',
                    Value=str(config['disable_api_termination']).lower()
                )

                await instance.load()
                yield await self.convert_instance(instance)

            yield await self.convert_instance(instance)


instance_resource = st.MachineResource("aws_instance", InstanceMachine)

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
