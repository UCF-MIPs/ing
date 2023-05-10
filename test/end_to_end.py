from ing.src import S3Access

s3a = S3Access()
print(s3a.get_buckets_list())