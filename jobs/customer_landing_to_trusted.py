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
AmazonS3_node1744209342061 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://simple2903-data-lake/customer/landing/customer-1691348231425.json"], "recurse": True}, transformation_ctx="AmazonS3_node1744209342061")

# Script generated for node SQL Query
SqlQuery6490 = '''
SELECT *
FROM myDataSource
WHERE shareWithResearchAsOfDate IS NOT NULL
'''
SQLQuery_node1744209513690 = sparkSqlQuery(glueContext, query = SqlQuery6490, mapping = {"myDataSource":AmazonS3_node1744209342061}, transformation_ctx = "SQLQuery_node1744209513690")

# Script generated for node Amazon S3
AmazonS3_node1744209592892 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1744209513690, connection_type="s3", format="json", connection_options={"path": "s3://simple2903-data-lake/customer/trusted/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1744209592892")

job.commit()