import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# change data source & sink here
glue_db = 'eeet-asm3-test1'
glue_table = 'traffic_raw'
mongo_uri = 'mongodb+srv://viphilongnguyen:egVQ0C3HhJRuVYaZ@cluster0.khgwh.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0'
mongo_db = 'ASM3'
mongo_collection = 'traffic_clean'
mongo_username = 'viphilongnguyen'
mongo_password = 'egVQ0C3HhJRuVYaZ'
s3_path = 's3://datpham-003/traffic_clean/'

from awsglue.dynamicframe import DynamicFrame
from awsglue.dynamicframe import DynamicFrameCollection

# Script generated for node Custom Transform
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    import pandas as pd
    from datetime import datetime
    import time
    from dateutil import parser
    
    def dfc_to_pd(dfc):
        df_spark = dfc.select(list(dfc.keys())[0]).toDF()
        return df_spark.toPandas()
    
    
    def pd_to_dfc(df_pandas, context): 
        df_spark = spark.createDataFrame(df_pandas) 
        dyf = DynamicFrame.fromDF(df_spark, context, "df_custom")
        return DynamicFrameCollection({"CustomTransform0": dyf}, context)
    
    
    def parse_json_column(df, json_columns):
        """
        Parses specified JSON columns in a DataFrame and extracts required fields into new columns.

        Parameters:
        - df (pd.DataFrame): The input DataFrame.
        - json_columns (list): List of column names containing JSON data.

        Returns:
        - pd.DataFrame: The DataFrame with extracted fields as new columns and JSON columns dropped.
        """
        # Extract fields from parsed JSON
        df['city'] = df['properties'].apply(lambda x: x['city'] )
        df['iconCategory'] = df['properties'].apply(lambda x: x['iconCategory'] )
        df['magnitudeOfDelay'] = df['properties'].apply(lambda x: x['magnitudeOfDelay'] )
        df['startTime'] = df['properties'].apply(lambda x: x['startTime'] )
        df['endTime'] = df['properties'].apply(lambda x: x['endTime'] )
        df['length'] = df['properties'].apply(lambda x: x['length'] )
        df['delay'] = df['properties'].apply(lambda x: x['delay'] )
        
        # get first element in event array
        event = df['properties'].apply(lambda x: x['events'][0])
        df['event_code'] = event.apply(lambda x: x['code'])
        df['event_desc'] = event.apply(lambda x: x['description'] )

        # Drop parsed columns
        df.drop(columns=json_columns, inplace=True, errors='ignore')

        return df

    def iso_to_local(iso_timestamp):
      if iso_timestamp == None:
        return None

      # Convert to local time
      utc_time = parser.isoparse(iso_timestamp)
      local_time = utc_time.astimezone()
      # print(f"Local time: {local_time}")
      return local_time


    def data_convert_time(df_input):
      df_output = df_input.copy()
      df_output['report_time'] = df_output['startTime'].apply(iso_to_local)
      df_output['report_time'] = pd.to_datetime(df_output['startTime']).dt.tz_localize(None)
      df_output.drop(columns=['startTime', 'endTime'], inplace=True, errors='ignore')

      return df_output

    def split_report_time(df, column_name='report_time'):
        # Ensure the column is in datetime format
        df[column_name] = pd.to_datetime(df[column_name])

        # Create new columns for date and time as strings
        df['date'] = df[column_name].dt.date.astype(str)
        df['time'] = df[column_name].dt.time.astype(str)

        # Optionally drop the original report_time column if no longer needed
        df = df.drop(columns=[column_name])

        return df

    def clean_data(df_input: pd.DataFrame):
        df = df_input.copy()
        # Drop duplicates
        df.drop_duplicates(inplace=True)

        # Drop rows with missing values
        df.dropna(inplace=True)

        return df


    df_pd = dfc_to_pd(dfc)
    df_pd = parse_json_column(df_pd, ['geometry', 'properties', 'type', 'partition_0', 'partition_1', 'partition_2', 'partition_3'])
    df_pd = data_convert_time(df_pd)
    df_pd = split_report_time(df_pd)
    df_pd = clean_data(df_pd)

    print(df_pd.columns.tolist())
    print(df_pd.head())

    return pd_to_dfc(df_pd, glueContext)

    
# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1736675916080 = glueContext.create_dynamic_frame.from_catalog(
    database=glue_db, table_name=glue_table, transformation_ctx="AWSGlueDataCatalog_node1736675916080")

# Script generated for node Custom Transform
CustomTransform_node1736675825335 = MyTransform(glueContext, DynamicFrameCollection({"AWSGlueDataCatalog_node1736675916080": AWSGlueDataCatalog_node1736675916080}, glueContext))

# Script generated for node Select From Collection
SelectFromCollection_node1736675844320 = SelectFromCollection.apply(dfc=CustomTransform_node1736675825335, key=list(CustomTransform_node1736675825335.keys())[0], transformation_ctx="SelectFromCollection_node1736675844320")


# Script generated for node MongoDB
mongo_connection_options={
        "connection.uri": mongo_uri,
        "database": mongo_db,
        "collection": mongo_collection,
        "username": mongo_username,
        "password": mongo_password,
        "disableUpdateUri": "false",
        "retryWrites": "true", 
    }

MongoDB_node1736677371224 = glueContext.write_dynamic_frame.from_options(
    frame=SelectFromCollection_node1736675844320, connection_type="mongodb", 
    connection_options=mongo_connection_options, transformation_ctx="MongoDB_node1736677371224")


from awsgluedq.transforms import EvaluateDataQuality

# Script generated for node Amazon S3
# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""
EvaluateDataQuality().process_rows(frame=SelectFromCollection_node1736675844320, 
                                   ruleset=DEFAULT_DATA_QUALITY_RULESET, 
                                   publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1736675386147", "enableDataQualityResultsPublishing": True}, 
                                   additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})

AmazonS3_node1736675849865 = glueContext.write_dynamic_frame.from_options(
    frame=SelectFromCollection_node1736675844320, connection_type="s3", 
    format="json", connection_options={"path": s3_path, "partitionKeys": []}, transformation_ctx="AmazonS3_node1736675849865")

job.commit()