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


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customer_curated
customer_curated_node1704903302649 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house/customer/curated1"],
        "recurse": True,
    },
    transformation_ctx="customer_curated_node1704903302649",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1704904444271 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_trusted_node1704904444271",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1704903303183 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house/step_trainer/trusted1/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_trusted_node1704903303183",
)

# Script generated for node SQL Query
SqlQuery242 = """
select s.sensorreadingtime,s.distancefromobject from a join s on a.serialnumber=s.serialnumber
"""
SQLQuery_node1704904129567 = sparkSqlQuery(
    glueContext,
    query=SqlQuery242,
    mapping={
        "c": customer_curated_node1704903302649,
        "s": step_trainer_trusted_node1704903303183,
        "a": accelerometer_trusted_node1704904444271,
    },
    transformation_ctx="SQLQuery_node1704904129567",
)

# Script generated for node machine_learning
machine_learning_node1704903314486 = glueContext.getSink(
    path="s3://stedi-lake-house/machine_learning/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="machine_learning_node1704903314486",
)
machine_learning_node1704903314486.setCatalogInfo(
    catalogDatabase="stedi2", catalogTableName="machine_learning"
)
machine_learning_node1704903314486.setFormat("json")
machine_learning_node1704903314486.writeFrame(SQLQuery_node1704904129567)
job.commit()
