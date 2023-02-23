#=================================================================================================================
import boto3
from airflow.models import Variable

ACCESS_KEY=Variable.get("ACCESS_KEY")
SECRET_KEY=Variable.get("SECRET_KEY")
REGION="ap-south-1"
BUCKET_NAME="ss14suraj1234"

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
        elif e.response['Error']['Code']=="BucketAlreadyExists":
            print("Bucket Already Exist")
        elif e.response['Error']['Code']=="InvalidBucketName":
            print("Bucket Name Is Invalid")
        else:
            print(e)
    return None
