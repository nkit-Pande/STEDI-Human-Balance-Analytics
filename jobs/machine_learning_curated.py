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

# Script generated for node Step Trainer
StepTrainer_node1744212231366 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://simple2903-data-lake/step_trainer/trusted/"], "recurse": True}, transformation_ctx="StepTrainer_node1744212231366")

# Script generated for node Accelerometer
Accelerometer_node1744212286852 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://simple2903-data-lake/accelerometer/trusted/"], "recurse": True}, transformation_ctx="Accelerometer_node1744212286852")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT * 
FROM accelerometer_trusted
JOIN step_trainer_trusted 
  ON accelerometer_trusted.timestamp = step_trainer_trusted.sensorreadingtime;

'''
SQLQuery_node1744219278553 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer_trusted":StepTrainer_node1744212231366, "accelerometer_trusted":Accelerometer_node1744212286852}, transformation_ctx = "SQLQuery_node1744219278553")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1744219220103 = glueContext.write_dynamic_frame.from_catalog(frame=SQLQuery_node1744219278553, database="stedi", table_name="machine_learning_curated", transformation_ctx="AWSGlueDataCatalog_node1744219220103")

job.commit()