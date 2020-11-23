import statey as st


aws = st.registry.get_provider('pulumi/aws', {'region': 'us-east-1'})

bucket = aws.get_resource('aws:s3/bucket:Bucket')

up = bucket.States.UP

print("UP", up.output_type)
