import os
import pickle
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.classification import RandomForestClassificationModel

def model_fn(model_dir):
    """Load model for inference"""
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("PollutionPredictionInference") \
        .getOrCreate()
    
    # Load models
    preprocessing_model = PipelineModel.load(os.path.join(model_dir, "preprocessing_pipeline"))
    rf_model = RandomForestClassificationModel.load(os.path.join(model_dir, "rf_model"))
    
    return {
        'preprocessing_model': preprocessing_model,
        'rf_model': rf_model,
        'spark': spark
    }

def input_fn(request_body, request_content_type):
    """Parse input data from pickle"""
    if request_content_type == 'application/python-pickle':
        data = pickle.loads(request_body)
        spark = SparkSession.builder.getOrCreate()
        return spark.createDataFrame([data])
    else:
        raise ValueError(f"Unsupported content type: {request_content_type}")

def predict_fn(input_data, model):
    """Make prediction"""
    processed_data = model['preprocessing_model'].transform(input_data)
    predictions = model['rf_model'].transform(processed_data)
    return predictions

def output_fn(prediction, accept):
    """Format prediction output using pickle"""
    if accept == 'application/python-pickle':
        predictions_list = prediction.select('prediction', 'probability').toPandas().to_dict(orient='records')
        return pickle.dumps(predictions_list)
    raise ValueError(f"Unsupported accept type: {accept}")