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
glue_table = 'weather_raw'
mongo_uri = 'mongodb+srv://viphilongnguyen:egVQ0C3HhJRuVYaZ@cluster0.khgwh.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0'
mongo_db = 'ASM3'
mongo_collection = 'weather_clean'
mongo_username = 'viphilongnguyen'
mongo_password = 'egVQ0C3HhJRuVYaZ'
s3_path = 's3://datpham-003/weather_clean/'

from awsglue.dynamicframe import DynamicFrame
from awsglue.dynamicframe import DynamicFrameCollection

# Script generated for node Custom Transform
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    import pandas as pd
    from datetime import datetime
    import time
    
    
    def dfc_to_pd(dfc):
        df_spark = dfc.select(list(dfc.keys())[0]).toDF()
        return df_spark.toPandas()
    
    
    def pd_to_dfc(df_pandas, context): 
        df_spark = spark.createDataFrame(df_pandas) 
        dyf = DynamicFrame.fromDF(df_spark, context, "df_custom")
        return DynamicFrameCollection({"CustomTransform0": dyf}, context)
    
    
    def parse_json_column(df_input: pd.DataFrame):
        df = df_input.copy()

        # Extract fields from parsed JSON
        df['longitude'] = df['coord'].apply(lambda x: x['lon'])
        df['latitude'] = df['coord'].apply(lambda x: x['lat'])
        df['weather_main'] = df['weather'].apply(lambda x: x[0]['main'])
        df['weather_desc'] = df['weather'].apply(lambda x: x[0]['description'])
        df['temperature'] = df['main'].apply(lambda x: x['temp'])
        df['feels_like'] = df['main'].apply(lambda x: x['feels_like'])
        df['pressure'] = df['main'].apply(lambda x: x['pressure'])
        df['humidity'] = df['main'].apply(lambda x: x['humidity'])
        df['wind_speed'] = df['wind'].apply(lambda x: x['speed'])
        df['wind_deg'] = df['wind'].apply(lambda x: x['deg'])
        df['cloudiness'] = df['clouds'].apply(lambda x: x['all'])
        df['country'] = df['sys'].apply(lambda x: x['country'])
        df['sunrise'] = df['sys'].apply(lambda x: datetime.utcfromtimestamp(x['sunrise']))
        df['sunset'] = df['sys'].apply(lambda x: datetime.utcfromtimestamp(x['sunset']))

        json_columns = ['coord', 'weather', 'main', 'wind', 'clouds', 'sys']
        # Drop parsed columns
        df.drop(columns=json_columns, inplace=True)
        return df

    
    def drop_columns(df_input: pd.DataFrame):
        df = df_input.copy()
        columns_to_drop = ['base', 'dt', 'cod', 'id', 'timezone', 'sunrise', 'sunset','country', 'partition_0', 'partition_1', 'partition_2', 'partition_3']
        df = df.drop(columns=columns_to_drop, errors='ignore')
        return df

    
    def rename_columns(df_input: pd.DataFrame):
        df = df_input.copy()
        df.rename(columns={
            'name': 'city',
        }, inplace=True)
        return df

    
    def split_report_time(df_input: pd.DataFrame, column_name='report_time'):
        df = df_input.copy()
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
    df_pd = parse_json_column(df_pd)
    df_pd = drop_columns(df_pd)
    df_pd = rename_columns(df_pd)
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