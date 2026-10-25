import boto3

def get_s3_region(bucket: str) -> str:  
    """Get the region of the S3 bucket"""
    s3 = boto3.client("s3")
    response = s3.get_bucket_location(Bucket=bucket)
    # AWS returns None for us-east-1, otherwise returns the region name
    return response["LocationConstraint"] or "us-east-1"
