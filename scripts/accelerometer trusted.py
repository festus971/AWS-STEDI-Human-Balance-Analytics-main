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

# Script generated for node accelerometer landing
accelerometerlanding_node1704869887641 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometerlanding_node1704869887641",
)

# Script generated for node customer trusted
customertrusted_node1704869894327 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customertrusted_node1704869894327",
)

# Script generated for node Join
Join_node1704870092572 = Join.apply(
    frame1=accelerometerlanding_node1704869887641,
    frame2=customertrusted_node1704869894327,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1704870092572",
)

# Script generated for node accelerometer trusted
accelerometertrusted_node1704869891719 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1704870092572,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://stedi-lake-house/accelerometer/trusted/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="accelerometertrusted_node1704869891719",
)

job.commit()
