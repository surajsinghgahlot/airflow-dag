#=================================================================================================================
import boto3
import os
import sys

ACCESS_KEY=os.environ['AWS_ACCESS_KEY_ID']
SECRET_KEY=os.environ['AWS_SECRET_ACCESS_KEY']
REGION=os.environ['REGION']
BUCKET_NAME=os.environ['BUCKET_NAME']

#Console Login:
def get_s3_client():
    session=boto3.session.Session(aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY,region_name=REGION)
    s3_client=session.client('s3')
    return s3_client

#Code:
def create_bucket():
    s3_response=get_s3_client()
    try:
        s3_response.create_bucket(Bucket=BUCKET_NAME,CreateBucketConfiguration={'LocationConstraint': REGION})
        print(BUCKET_NAME,"is created")
    except Exception as e:
        if e.response['Error']['Code']=="BucketAlreadyOwnedByYou":
            print("Bucket Already Owned By You")
            sys.exit(0)
        elif e.response['Error']['Code']=="BucketAlreadyExists":
            print("Bucket Already Exist")
            sys.exit(0)
        elif e.response['Error']['Code']=="InvalidBucketName":
            print("Bucket Name Is Invalid")
            sys.exit(0)
        else:
            print(e)
            sys.exit(0)
    return None

if __name__=="__main__":
    create_bucket()