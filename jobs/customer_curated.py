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

# Script generated for node accelerometer
accelerometer_node1744134219808 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://simple2903-data-lake/accelerometer/trusted/"], "recurse": True}, transformation_ctx="accelerometer_node1744134219808")

# Script generated for node customer
customer_node1744037956558 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://simple2903-data-lake/customer/trusted/"], "recurse": True}, transformation_ctx="customer_node1744037956558")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT DISTINCT customer_trusted.*, accelerometer_trusted.*
FROM customer_trusted
JOIN accelerometer_trusted
  ON customer_trusted.email = accelerometer_trusted.user;

'''
SQLQuery_node1745301524078 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"accelerometer_trusted":accelerometer_node1744134219808, "customer_trusted":customer_node1744037956558}, transformation_ctx = "SQLQuery_node1745301524078")

# Script generated for node Amazon S3
AmazonS3_node1744038225712 = glueContext.getSink(path="s3://simple2903-data-lake/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1744038225712")
AmazonS3_node1744038225712.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
AmazonS3_node1744038225712.setFormat("json")
AmazonS3_node1744038225712.writeFrame(SQLQuery_node1745301524078)
job.commit()