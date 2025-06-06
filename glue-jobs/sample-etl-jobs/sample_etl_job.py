import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read CSV from S3
input_path = "s3://<your-bucket>/input/sample.csv"
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [input_path], "recurse": True},
    format="csv",
    format_options={"withHeader": True}
)

# Filter rows (example: keep rows where 'age' > 25)
filtered_data = Filter.apply(
    frame=datasource,
    f=lambda x: x["age"] > 25 if "age" in x else False
)

# Write output as Parquet
output_path = "s3://<your-bucket>/output/"
glueContext.write_dynamic_frame.from_options(
    filtered_data,
    connection_type="s3",
    connection_options={"path": output_path},
    format="parquet"
)

# Commit job
job.commit()