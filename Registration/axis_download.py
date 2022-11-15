import boto3

client = boto3.client(
    's3',
    aws_access_key_id = 'AKIA3LIJCIOGB62OOBQP',
    aws_secret_access_key = '0YBO1zpkk+jvDC0ngMr+HbBjV1/vXrcEOleTy73r',
    region_name = 'ap-south-1'
)

client.download_file(
    Bucket="tangofastestfinger", Key="sample_ques.csv", Filename="dir.csv"
)