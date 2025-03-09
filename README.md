# EEET2574 - Big Data Engineering - Group Project

Team Members

- Dat Pham      s3927188
- Huan Nguyen   3927467
- Nhan Truong   s3929215
- Long Nguyen   s3904632

Documents

- [Report](https://drive.google.com/file/d/18xOkAOjVIkfqlHZNV1XJBSmVpUfFgHe5/view?usp=sharing)
- [Slide](https://drive.google.com/file/d/15u-5M9-b0vrD8AmcaXMUhtySXWkjuddZ/view?usp=sharing)

# Project Structure

```
.
├── etl-glue/
├── producer-air/
├── producer-traffic/
├── producer-weather/
├── training/
├── docker-compose.yml
├── README.md
└── training/
    ├── DataInspection.ipynb
    └── train.csv
```

- `etl-glue`: contains glucozo
- `producer-*`: module for API ingestion and uploading to S3 storage via Firehose
- `training`: ???

# How to run Project

## Producers

Environmental Setup
- Firehose Streams
- S3 Buckets
- A running EC2 instance with its key pair in `.pem` format
- AWS access key and session token 

1. Setup access token for AWS and MongoDB servies

- `produer-*.py`

```python
# Firehose
firehose_stream = 'RAW-AIR-cpFmY'
firehose = boto3.client(
    service_name = 'firehose', 
    region_name = 'us-east-1',
    aws_access_key_id = AWS_ACCESS_ID,
    aws_secret_access_key = AWS_ACCESS_KEY,
    aws_session_token = AWS_SESSSION_TOKEN)

# Function to connect to MongoDB
def connect_to_db():
    client = MongoClient(mongo_uri)
    print("Connected to MongoDB")
    db = client['ASM3'] 
    weather_collection = db['weather_raw'] 
    return weather_collection
```

- `.env` 

```
OPENWEATHER_ACCESS_TOKEN=...
TOMTOM_KEY = ...
WEATHER_API_ACCESS_TOKEN=...
MONGO_URI=...
aws_access_key_id=...
aws_secret_access_key=...
aws_session_token=...
```

2. Upload project to EC2

```bash
# ec2 example variables
EC2_CRED="~/.ssh/labsuser.pem"
EC2_USER="ec2-user"
EC2_DNS="ec2-107-23-22-182.compute-1.amazonaws.com"
EC2_PATH="/home/${EC2_USER}/projects/test1"

# upload to ec2 instance
scp -i ${EC2_CRED} -r ./* ${EC2_USER}@${EC2_DNS}:${EC2_PATH}

# access ec2 remotely
ssh -i ${EC2_CRED} "${EC2_USER}@${EC2_DNS}"
```

3. Run containers on EC2 (assuming we have Docker and Docker-compose installed)

```bash
# access ec2 remotely
ssh -i ${EC2_CRED} "${EC2_USER}@${EC2_DNS}"

# run producers
cd ${EC2_PATH}

sudo docker-compose up --build --detach 
```

## Extract - Transform - Load 

1. Modify Glue database source, S3 sink, Mongodb sink to your need

- `topic-etl.py` 

```python
# change data source & sink here
glue_db = 'eeet-asm3-test1'
glue_table = 'air_raw'
mongo_uri = 'mongodb+srv://cluster0.1xjq9.mongodb.net'
mongo_db = 'asm3-test1'
mongo_collection = 'air_clean'
mongo_username = 'user1'
mongo_password = '123'
s3_path = 's3://datpham-003/air_clean/'
```

2. Upload the scripts from module `etl-glue` to AWS Glue Jobs.

3. Run jobs for each topic `air, weather, traffic`, then finally `combine-etl` for combining three dataset. 

## Model Traning & Deploy

Environment Setup

- AWS SageMaker Studio
- Jupyter Lab in SageMaker
- SparkMLlib for data processing and model training
- Python and required dependencies (ensure `inference.py` dependencies are installed)

1. Upload Notebooks to SageMaker
- Open AWS SageMaker Studio.
- Navigate to the Jupyter Lab interface.
- Upload all the files from the `training/` directory to your SageMaker workspace.

2. Model Training & Export
- Open and run the `model_training.ipynb` notebook:
- **Step 1:** Perform Exploratory Data Analysis (EDA) to understand the dataset.
- **Step 2:** Train the classification model using SparkMLlib.
- **Step 3:** Export the trained model and upload it to an S3 bucket for deployment.

3. Model Deployment
- Run the `deploy.ipynb` notebook to:
- **Create an Endpoint:** Configure and deploy the trained model endpoint using SageMaker.
- **Deploy Model:** Ensure successful endpoint creation by monitoring deployment status.

4. Configure Inference Dependencies
- Ensure all dependencies listed in `inference.py` are correctly installed in the SageMaker environment.

5. Model Invocation for Prediction
- Open and run the `invoke.ipynb` notebook to:
- **Send Requests to the Endpoint:** Use sample input data for testing.
- **Receive Predictions:** Confirm the endpoint’s response for air pollution classification tasks.

Additional Notes
- Monitor SageMaker logs during each step to catch potential errors.
- Verify S3 bucket permissions to ensure smooth upload and access to the model.
- Keep track of the endpoint name for invoking predictions effectively.

# Visualization

MongoDB Chart: https://charts.mongodb.com/charts-eeet2574-asm3-wcltbpp/public/dashboards/677f772c-613a-4130-8384-e5993bf03ffa
