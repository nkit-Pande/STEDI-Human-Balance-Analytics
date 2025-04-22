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
AmazonS3_node1744210105311 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://simple2903-data-lake/accelerometer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1744210105311")

# Script generated for node Amazon S3
AmazonS3_node1745303065265 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://simple2903-data-lake/customer/trusted/"], "recurse": True}, transformation_ctx="AmazonS3_node1745303065265")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT DISTINCT accelerometer_landing.*
FROM accelerometer_landing
JOIN customer_trusted
  ON accelerometer_landing.user = customer_trusted.email

'''
SQLQuery_node1745302652930 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"accelerometer_landing":AmazonS3_node1744210105311, "customer_trusted":AmazonS3_node1745303065265}, transformation_ctx = "SQLQuery_node1745302652930")

# Script generated for node Amazon S3
AmazonS3_node1744210247985 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1745302652930, connection_type="s3", format="json", connection_options={"path": "s3://simple2903-data-lake/accelerometer/trusted/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1744210247985")

job.commit()