import statey as st

from statey.lib.pulumi.provider_api import API

api = API()

print("HERE", api)

print("HERE2", api.aws)

print("HERE3", api.aws.s3)

print("HERE4", api.aws.s3.Bucket)
