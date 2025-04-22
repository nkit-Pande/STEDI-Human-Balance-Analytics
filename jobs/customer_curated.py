import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1744134219808 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://simple2903-data-lake/accelerometer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1744134219808")

# Script generated for node Amazon S3
AmazonS3_node1744037956558 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://simple2903-data-lake/customer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1744037956558")

# Script generated for node Join
Join_node1744134277012 = Join.apply(frame1=AmazonS3_node1744037956558, frame2=AmazonS3_node1744134219808, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1744134277012")

# Script generated for node Drop Fields
DropFields_node1744135365414 = DropFields.apply(frame=Join_node1744134277012, paths=["timestamp", "email", "phone", "sharewithfriendsasofdate", "sharewithpublicasofdate", "sharewithresearchasofdate", "lastupdatedate", "registrationdate", "serialnumber", "birthday"], transformation_ctx="DropFields_node1744135365414")

# Script generated for node Amazon S3
AmazonS3_node1744038225712 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1744135365414, connection_type="s3", format="json", connection_options={"path": "s3://simple2903-data-lake/customer/curated/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1744038225712")

job.commit()