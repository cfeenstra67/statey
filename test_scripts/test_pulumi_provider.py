import statey as st


aws = st.registry.get_provider('pulumi/aws')

print("AWS", aws)

print("AWS2", st.registry.get_provider('pulumi/aws'))

bucket_object = aws.get_resource('aws:s3/bucketObject:BucketObject')

print("bucket object", bucket_object)

print("INPUT", bucket_object.s.UP.input_type)

print("OUTPUT", bucket_object.s.UP.output_type)
