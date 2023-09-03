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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1693193435799 = glueContext.create_dynamic_frame.from_catalog(
    database="db_youtube_cleaned",
    table_name="tableschema_cleaned_raw_statistics",
    transformation_ctx="AWSGlueDataCatalog_node1693193435799",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1693193434477 = glueContext.create_dynamic_frame.from_catalog(
    database="db_youtube_cleaned",
    table_name="cleaned_statistics_reference_data",
    transformation_ctx="AWSGlueDataCatalog_node1693193434477",
)

# Script generated for node Join
Join_node1693193543734 = Join.apply(
    frame1=AWSGlueDataCatalog_node1693193435799,
    frame2=AWSGlueDataCatalog_node1693193434477,
    keys1=["category_id"],
    keys2=["id"],
    transformation_ctx="Join_node1693193543734",
)

# Script generated for node Amazon S3
AmazonS3_node1693193942988 = glueContext.getSink(
    path="s3://de-analytical",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["region", "category_id"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1693193942988",
)
AmazonS3_node1693193942988.setCatalogInfo(
    catalogDatabase="de_youtube_analytics", catalogTableName="final_analytics"
)
AmazonS3_node1693193942988.setFormat("glueparquet")
AmazonS3_node1693193942988.writeFrame(Join_node1693193543734)
job.commit()
