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

# Script generated for node Customer
Customer_node1744212260157 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://simple2903-data-lake/customer/trusted/"], "recurse": True}, transformation_ctx="Customer_node1744212260157")

# Script generated for node Step Trainer
StepTrainer_node1744212231366 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://simple2903-data-lake/step_trainer/trusted/"], "recurse": True}, transformation_ctx="StepTrainer_node1744212231366")

# Script generated for node Accelerometer
Accelerometer_node1744212286852 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://simple2903-data-lake/accelerometer/trusted/"], "recurse": True}, transformation_ctx="Accelerometer_node1744212286852")

# Script generated for node Join
Join_node1744212317124 = Join.apply(frame1=Customer_node1744212260157, frame2=StepTrainer_node1744212231366, keys1=["serialNumber"], keys2=["serialnumber"], transformation_ctx="Join_node1744212317124")

# Script generated for node Join
Join_node1744212309889 = Join.apply(frame1=Accelerometer_node1744212286852, frame2=Customer_node1744212260157, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1744212309889")

# Script generated for node Join
Join_node1744212371705 = Join.apply(frame1=Join_node1744212309889, frame2=Join_node1744212317124, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1744212371705")

# Script generated for node Machine Learning
MachineLearning_node1744212403119 = glueContext.write_dynamic_frame.from_options(frame=Join_node1744212371705, connection_type="s3", format="json", connection_options={"path": "s3://simple2903-data-lake/machine_learning/curated/", "partitionKeys": []}, transformation_ctx="MachineLearning_node1744212403119")

job.commit()