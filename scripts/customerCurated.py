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

# Script generated for node CustomerTrusted
CustomerTrusted_node1704870664704 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1704870664704",
)

# Script generated for node AccelerometerTrusted
AccelerometerTrusted_node1704870664106 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://stedi-lake-house/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1704870664106",
)

# Script generated for node Join
Join_node1704871402747 = Join.apply(
    frame1=AccelerometerTrusted_node1704870664106,
    frame2=CustomerTrusted_node1704870664704,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1704871402747",
)

# Script generated for node Drop Fields
DropFields_node1704871405700 = DropFields.apply(
    frame=Join_node1704871402747,
    paths=[
        "serialNumber",
        "`.customerName`",
        "birthDay",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "`.shareWithPublicAsOfDate`",
        "shareWithFriendsAsOfDate",
        "`.birthDay`",
        "`.lastUpdateDate`",
        "`.phone`",
        "shareWithPublicAsOfDate",
        "phone",
        "`.shareWithResearchAsOfDate`",
        "lastUpdateDate",
        "`.serialNumber`",
        "`.registrationDate`",
        "`.shareWithFriendsAsOfDate`",
        "`.email`",
        "email",
    ],
    transformation_ctx="DropFields_node1704871405700",
)

# Script generated for node CustomerCurated
CustomerCurated_node1704870669641 = glueContext.getSink(
    path="s3://stedi-lake-house/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCurated_node1704870669641",
)
CustomerCurated_node1704870669641.setCatalogInfo(
    catalogDatabase="stedi2", catalogTableName="CustomerCurated"
)
CustomerCurated_node1704870669641.setFormat("json")
CustomerCurated_node1704870669641.writeFrame(DropFields_node1704871405700)
job.commit()
