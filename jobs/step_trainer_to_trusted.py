import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1744211190246 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://simple2903-data-lake/step_trainer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1744211190246")

# Script generated for node Amazon S3
AmazonS3_node1745304467160 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://simple2903-data-lake/customer/trusted/"], "recurse": True}, transformation_ctx="AmazonS3_node1745304467160")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT * 
FROM step_trainer_landing
JOIN customer_curated 
  ON step_trainer_landing.serialnumber = customer_curated.serialnumber;

'''
SQLQuery_node1744211243141 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer_landing":AmazonS3_node1744211190246, "customer_curated":AmazonS3_node1745304467160}, transformation_ctx = "SQLQuery_node1744211243141")

# Script generated for node Amazon S3
AmazonS3_node1744211251742 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1744211243141, connection_type="s3", format="json", connection_options={"path": "s3://simple2903-data-lake/step_trainer/trusted/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1744211251742")

job.commit()