# from typing import Optional, Sequence

# import yaml

# import statey as st


# BoltPlanResourceType = st.Struct[
#     "type": str,
#     "title": str,
#     "parameters": st.Map[str, str](default=dict)
# ]


# BoltPlanStepType = st.Struct[
#     "name": str,
#     "resources": st.Array[BoltPlanResourceType]
# ]


# BoltPlanInventoryType = st.Struct[
#     "target_uris": st.Array[str],
#     "key_path": str,
#     "user": st.String(default='root'),
#     "group_name": st.String(default='hosts'),
#     "file_name": st.String(default='statey-inventory.yaml')
# ]


# BoltPlanType = st.Struct[
#     "steps": st.Array[BoltPlanStepType],
#     "project": str,
#     "inventory": BoltPlanInventoryType,
#     "module_name": st.String(default='statey')
#     "plan_name": st.String(default='plan')
# ]


# class BoltPlanMachine(st.SimpleMachine):
#     """
#     Machine to apply a bolt plan
#     """
#     @staticmethod
#     def load_inventory(
#         path: str,
#         group_name: str
#     ) -> Optional[BoltPlanInventoryType]:
#         """

#         """
#         if not os.path.isfile(path):
#             return None
#         try:
#             with open(path) as f:
#                 data = yaml.load(f, Loader=yaml.Loader)
#         except st.parser.ParseError:
#             return None

#         groups = data.get('groups')
#         if groups is None:
#             return None

#         groups_by_name = {grp['name']: grp for grp in groups}
#         if group_name not in groups_by_name:
#             return None

#         group = groups_by_name[group_name]
#         targets = group.get('targets')
#         config = group.get('config')

#         target_uris = []
#         if targets:
#             target_uris = [target['uri'] for target in targets]

#         target_user = 'root'
#         target_key_path = ''
#         if config and config.get('ssh'):
#             ssh_config = config['ssh']
#             if 'run-as' in ssh_config:
#                 target_user = ssh_config['run-as']
#             if 'private-key' in ssh_config:
#                 target_key_path = ssh_config['private-key']

#         return {
#             'target_uris': target_uris,
#             'key_path': target_key_path,
#             'user': target_user
#         }

#     @staticmethod
#     def load_steps(path: str) -> Sequence[BoltPlanStepType]:
#         """

#         """
#         if not os.path.isfile(path):
#             return []

#         try:
#             with open(path) as f:
#                 data = yaml.load(f, Loader=yaml.Loader)
#         except yaml.parser.ParseError:
#             return []

#         out_steps = []
#         steps = data.get('steps')
#         if not steps:
#             return out_steps

#         for step in steps:
#             step_name = step.get('name', '')
#             resources = step.get('resources', [])
#             out_resources = []

#             for resource in resources:
#                 out_resources.append({
#                     'type': resource.get('type', ''),
#                     'title': resource.get('title', ''),
#                     'parameters': {
#                         key: str(val)
#                         for key, val in resource.get('parameters', {}).items()
#                     }
#                 })

#             out_steps.append({
#                 'name': step_name,
#                 'resources': out_resources
#             })

#         return out_steps

#     @classmethod
#     def load_plan(
#         cls,
#         path: str,
#         module_name: str,
#         plan_name: str,
#         inventory_file_name: str
#     ) -> Optional[BoltPlanType]:
#         """

#         """
#         inventory_path = os.path.join(path, 'inventory.yaml')
#         inventory = cls.load_inventory(inventory_path)
#         inventory = inventory or {
#             'target_uris': [],
#             'key_path': ''
#         }

#         site_mods_path = os.path.join(path, 'site-modules')
#         module_path = os.path.join(site_mods_path, module_name)

#         plans_path = os.path.join(module_path, 'plans')
#         plan_path = os.path.join(plans_path, plan_name + '.yaml')
#         plan_steps = cls.load_steps(plan_path)

#         return {
#             'steps': plan_steps,
#             'inventory': inventory,
#             'project': path,
#             'module_name': module_name,
#             'plan_name': plan_name
#         }

#     async def refresh_state(self, data: Any) -> Optional[Any]:
#         return self.load_plan(
#             path=data['project'],
#             module_name=data['module_name'],
#             plan_name=data['plan_name']
#         )

#     async def create_task(self, config: BoltPlanType) -> BoltPlanType:


#     async def delete_task(self, current: InstanceType) -> st.EmptyType:
#         pass
