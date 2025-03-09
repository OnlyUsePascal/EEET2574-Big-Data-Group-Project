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
mongo_uri = 'mongodb+srv://viphilongnguyen:egVQ0C3HhJRuVYaZ@cluster0.khgwh.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0'
mongo_db = 'ASM3'
mongo_collection = 'combine_clean'
mongo_username = 'viphilongnguyen'
mongo_password = 'egVQ0C3HhJRuVYaZ'
s3_path = 's3://datpham-003/combine_clean/'

from awsglue.dynamicframe import DynamicFrame
from awsglue.dynamicframe import DynamicFrameCollection

# Script generated for node Custom Transform
def TransformTopic(glueContext, dfc) -> DynamicFrameCollection:
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
    
    
    def change_city_name(df: pd.DataFrame):
        df["city"] = df["city"].replace("Ap Ba", "Da Nang")
        df["city"] = df["city"].replace("Hanoi", "Ha Noi")
        df["city"] = df["city"].str.lower()
        return df 

    def split_time_to_hour_minute(df: pd.DataFrame):
        df["hour"] = df["time"].str.split(":").str[0].astype(int)
        df["minute"] = df["time"].str.split(":").str[1].astype(int)
        df.drop(columns=["time"], inplace=True)
        return df


    df_pd = dfc_to_pd(dfc)
    df_pd = change_city_name(df_pd)
    df_pd = split_time_to_hour_minute(df_pd)

    print(df_pd.columns.tolist())
    print(df_pd.head())

    return pd_to_dfc(df_pd, glueContext)


# Script generated for node AWS Glue Data Catalog
DataNote_Traffic = glueContext.create_dynamic_frame.from_catalog(database="eeet-asm3-test1", table_name="traffic_clean", transformation_ctx="DataNote_Traffic")

# Script generated for node Custom Transform
DFC_Traffic = TransformTopic(glueContext, DynamicFrameCollection({"DataNote_Traffic": DataNote_Traffic}, glueContext))

# Script generated for node AWS Glue Data Catalog
DataNote_Weather = glueContext.create_dynamic_frame.from_catalog(database="eeet-asm3-test1", table_name="weather_clean", transformation_ctx="DataNote_Weather")

# Script generated for node Custom Transform
DFC_Weather = TransformTopic(glueContext, DynamicFrameCollection({"DataNote_Weather": DataNote_Weather}, glueContext))

# Script generated for node AWS Glue Data Catalog
DataNote_Air = glueContext.create_dynamic_frame.from_catalog(database="eeet-asm3-test1", table_name="air_clean", transformation_ctx="DataNote_Air")

# Script generated for node Custom Transform
DFC_Air = TransformTopic(glueContext, DynamicFrameCollection({"DataNote_Air": DataNote_Air}, glueContext))

from awsglue.dynamicframe import DynamicFrame
from awsglue.dynamicframe import DynamicFrameCollection
import pandas as pd

# Script generated for node Custom Transform
def TransformMerge(glueContext, dfc_tuple) -> DynamicFrameCollection:
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
    
    
    def change_city_name(df: pd.DataFrame):
        df["city"] = df["city"].replace("Ap Ba", "Da Nang")
        df["city"] = df["city"].replace("Hanoi", "Ha Noi")
        df["city"] = df["city"].str.lower()
        return df 

    def split_time_to_hour_minute(df: pd.DataFrame):
        df["hour"] = df["time"].str.split(":").str[0].astype(int)
        df["minute"] = df["time"].str.split(":").str[1].astype(int)
        df.drop(columns=["time"], inplace=True)
        return df
    
    from sklearn.cluster import KMeans
    from sklearn.preprocessing import StandardScaler

    def classify_pollution(df, n_clusters=3, random_state=42):
        # Select relevant features
        features = ['humidity', 'pressure', 'temperature', 'wind_speed','magnitudeOfDelay', 'pm10', 'pm2_5', 'co', 'so2', 'o3']
        X = df[features]

        # Handle missing values
        X = X.fillna(X.mean())

        # Standardize the data
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        # Apply K-Means clustering
        kmeans = KMeans(n_clusters=n_clusters, random_state=random_state)
        df['pollution_cluster'] = kmeans.fit_predict(X_scaled)

        # Inverse transform centroids for interpretation
        centroids = scaler.inverse_transform(kmeans.cluster_centers_)
        centroid_df = pd.DataFrame(centroids, columns=features)

        # Map cluster IDs to pollution levels (customize mapping as needed)
        pollution_mapping = {0: 'High', 1: 'Moderate', 2: 'Low'}  # Adjust based on cluster analysis
        df['pollution_level'] = df['pollution_cluster'].map(pollution_mapping)

        df.drop(columns=['pollution_cluster'], inplace=True)
        return df
    
    
    def clean_data(df_input: pd.DataFrame):
        df = df_input.copy()
        # Drop duplicates
        df.drop_duplicates(inplace=True)

        # Drop rows with missing values
        df.dropna(inplace=True)

        return df
    
    def convert_length(row):
        (_float, _int) = row
        if (_float is not None):
            return _float
        return _int
    
    
    # merge
    dfc_traffic, dfc_weather, dfc_air = dfc_tuple
    
    df_traffic = dfc_to_pd(dfc_traffic)
    df_weather = dfc_to_pd(dfc_weather)
    df_air = dfc_to_pd(dfc_air)
    
    df_traffic['length'] = df_traffic['length'].map(convert_length)
    
    df_pd = df_traffic.merge(df_weather, on=["date", "city", "hour", "minute"], how="inner") \
                        .merge(df_air, on=["date", "city", "hour", "minute"], how="inner")
    
    # feature engineering
    df_pd = classify_pollution(df_pd)
    df_pd = clean_data(df_pd)
    
    print(df_pd.columns.tolist())
    print(df_pd.head())

    # df_pd = df_traffic
    return pd_to_dfc(df_pd, glueContext)

# Merge
DFC_Merge = TransformMerge(glueContext, (DFC_Traffic, DFC_Weather, DFC_Air))

# Script generated for node Select From Collection
SelectDFC_Merge = SelectFromCollection.apply(dfc=DFC_Merge, key=list(DFC_Merge.keys())[0], transformation_ctx="SelectDFC_Merge")


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
    frame=SelectDFC_Merge, connection_type="mongodb", 
    connection_options=mongo_connection_options, transformation_ctx="MongoDB_node1736677371224")


from awsgluedq.transforms import EvaluateDataQuality

# Script generated for node Amazon S3
# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""
EvaluateDataQuality().process_rows(frame=SelectDFC_Merge, ruleset=DEFAULT_DATA_QUALITY_RULESET, 
    publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1736675386147", "enableDataQualityResultsPublishing": True}, 
    additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})

AmazonS3_node1736675849865 = glueContext.write_dynamic_frame.from_options(
    frame=SelectDFC_Merge, connection_type="s3", format="json", 
    connection_options={"path": s3_path, "partitionKeys": []}, 
    transformation_ctx="AmazonS3_node1736675849865")

job.commit()