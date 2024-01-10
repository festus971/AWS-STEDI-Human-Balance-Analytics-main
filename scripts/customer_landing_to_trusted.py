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

# Script generated for node Amazon S3
AmazonS3_node1704792905525 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1704792905525",
)

# Script generated for node SQL Query
SqlQuery203 = """
select * from myDataSource
where shareWithResearchAsOfDate
is not null

"""
SQLQuery_node1704869254489 = sparkSqlQuery(
    glueContext,
    query=SqlQuery203,
    mapping={"myDataSource": AmazonS3_node1704792905525},
    transformation_ctx="SQLQuery_node1704869254489",
)

# Script generated for node Amazon S3
AmazonS3_node1704792917027 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1704869254489,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1704792917027",
)

job.commit()
