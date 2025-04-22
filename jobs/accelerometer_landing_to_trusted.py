import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1744210105311 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://simple2903-data-lake/accelerometer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1744210105311")

# Script generated for node Drop Duplicates
DropDuplicates_node1744210238462 =  DynamicFrame.fromDF(AmazonS3_node1744210105311.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1744210238462")

# Script generated for node Amazon S3
AmazonS3_node1744210247985 = glueContext.write_dynamic_frame.from_options(frame=DropDuplicates_node1744210238462, connection_type="s3", format="json", connection_options={"path": "s3://simple2903-data-lake/accelerometer/trusted/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1744210247985")

job.commit()