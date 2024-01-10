import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node step-trainer
steptrainer_node1704877133605 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="steptrainer_node1704877133605",
)

# Script generated for node customertrusted
customertrusted_node1704877133093 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customertrusted_node1704877133093",
)

# Script generated for node Join
Join_node1704894108702 = Join.apply(
    frame1=steptrainer_node1704877133605,
    frame2=customertrusted_node1704877133093,
    keys1=["serialnumber"],
    keys2=["serialnumber"],
    transformation_ctx="Join_node1704894108702",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1704877137419 = glueContext.getSink(
    path="s3://stedi-lake-house/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="step_trainer_trusted_node1704877137419",
)
step_trainer_trusted_node1704877137419.setCatalogInfo(
    catalogDatabase="stedi2", catalogTableName="step_trainer_trusted"
)
step_trainer_trusted_node1704877137419.setFormat("json")
step_trainer_trusted_node1704877137419.writeFrame(Join_node1704894108702)
job.commit()
