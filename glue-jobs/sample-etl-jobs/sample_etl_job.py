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
input_path = "s3://nishant-glue-bucket-234/input/sample.csv"
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [input_path], "recurse": True},
    format="csv",
    format_options={"withHeader": True}
)
 
# Debugging: Print schema, count, and sample data
print("Datasource schema:")
datasource.printSchema()
print("Number of rows in datasource:", datasource.count())
datasource.show(5)
 
# Filter rows (keep rows where 'age' > 25, converting string to int)
filtered_data = Filter.apply(
    frame=datasource,
    f=lambda x: int(x["age"]) > 25 if "age" in x and x["age"].isdigit() else False
)
 
# Debugging: Print filtered count and sample data
print("Number of rows in filtered_data:", filtered_data.count())
filtered_data.show(5)
 
# Write output as Parquet only if there is data
output_path = "s3://nishant-glue-bucket-234/output/"
if filtered_data.count() > 0:
    glueContext.write_dynamic_frame.from_options(
        filtered_data,
        connection_type="s3",
        connection_options={"path": output_path},
        format="parquet"
    )
else:
    print("No data to write after filtering")
 
# Commit job
job.commit()
