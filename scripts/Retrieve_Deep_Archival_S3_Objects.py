import sys  
from awsglue.transforms import *  
from awsglue.utils import getResolvedOptions  
from pyspark.context import SparkContext  
from awsglue.context import GlueContext  
from awsglue.job import Job  
import boto3  
from tqdm import tqdm
from botocore.exceptions import ClientError
  
## @params: [JOB_NAME, bucket_name, prefix, restore_request_day]  
args = getResolvedOptions(sys.argv, [  
    'JOB_NAME',  
    'bucket_name',  
    'prefix',  
    'restore_request_day'  
])  
sc = SparkContext()  
glueContext = GlueContext(sc)  
spark = glueContext.spark_session  
job = Job(glueContext)  
job.init(args['JOB_NAME'] +args['bucket_name']+args['prefix'], args)  
  
def list_glacier_objects(bucket_name, prefix):  
    s3 = boto3.client('s3')  
    continuation_token = None  
  
    while True:  
        list_params = {  
            'Bucket': bucket_name,  
            'Prefix': prefix,  
            'Delimiter': '/'  
        }  
  
        if continuation_token:  
            list_params['ContinuationToken'] = continuation_token  
  
        response = s3.list_objects_v2(**list_params)  
        objects = response.get('Contents', [])  
  
        for obj in objects:  
            if 'StorageClass' in obj and obj['StorageClass'] == 'DEEP_ARCHIVE':  
                if 'Restore' in obj and obj['Restore'] == 'ongoing-request':  
                    continue  # Skip objects that are already in progress  
                yield obj['Key']  
  
        prefixes = response.get('CommonPrefixes', [])  
        for prefix in prefixes:  
            subfolder_prefix = prefix['Prefix']  
            yield from list_glacier_objects(bucket_name, subfolder_prefix)  
  
        if not response.get('IsTruncated', False):  
            break  
  
        continuation_token = response['NextContinuationToken']  
  
def restore_objects_expedited(bucket_name, prefix, restore_request_day):  
    print(bucket_name, prefix, restore_request_day)  
    s3 = boto3.client('s3')  
    total_objects = 0  
    restored_count = 0  
  
    with tqdm(unit='objects') as pbar:  
        if prefix:  
            for key in list_glacier_objects(bucket_name, prefix):  
                total_objects += 1  
                try:  
                    s3.restore_object(  
                        Bucket=bucket_name,  
                        Key=key,  
                        RestoreRequest={'Days': int(restore_request_day), 'GlacierJobParameters': {'Tier': 'Standard'}}  
                    )  
                    restored_count += 1  
                    pbar.update(1)  
                    pbar.set_description(f"Restoring: {key} ({restored_count}/{total_objects})")  
                except ClientError as e:  
                    if e.response['Error']['Code'] == 'RestoreAlreadyInProgress':  
                        print(f"Restore already in progress for object: {key}. Skipping...")  
                    else:  
                        print(f"Error restoring object: {key}. {e}")  
  
        else:  
            for key in list_glacier_objects(bucket_name, ''):  
                total_objects += 1  
                try:  
                    s3.restore_object(  
                        Bucket=bucket_name,  
                        Key=key,  
                        RestoreRequest={'Days': int(restore_request_day), 'GlacierJobParameters': {'Tier': 'Standard'}}  
                    )  
                    restored_count += 1  
                    pbar.update(1)  
                    pbar.set_description(f"Restoring: {key} ({restored_count}/{total_objects})")  
                except ClientError as e:  
                    if e.response['Error']['Code'] == 'RestoreAlreadyInProgress':  
                        print(f"Restore already in progress for object: {key}. Skipping...")  
                    else:  
                        print(f"Error restoring object: {key}. {e}")  
  
    print(f"All {restored_count} objects in the bucket restored successfully!")  
  
if __name__ == '__main__':  
    bucket_name = args['bucket_name']  
    prefix = args['prefix'] if args['prefix'] != 'None' else None  # Use None if prefix is not specified  
    restore_request_day = args['restore_request_day']  
  
    restore_objects_expedited(bucket_name, prefix, restore_request_day)  
  
job.commit()  
