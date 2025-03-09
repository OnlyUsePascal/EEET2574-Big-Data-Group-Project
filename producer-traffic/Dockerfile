FROM python:3.10.16

WORKDIR /usr/app

ADD ./requirements.txt ./

RUN pip install -r requirements.txt

# Create a new user and group
RUN groupadd -r appuser && useradd -r -g appuser appuser

# # Create the .aws directory and set permissions
# RUN mkdir -p /home/appuser/.aws && chown -R appuser:appuser /home/appuser/.aws

# # Copy the AWS credentials file and set permissions
# COPY ./.aws_credentials /home/appuser/.aws/credentials
# RUN chown appuser:appuser /home/appuser/.aws/credentials

# Copy the rest of the application code and set permissions
COPY ./ ./
RUN chown -R appuser:appuser /usr/app

# Switch to the new user
USER appuser

# Specify the command to run the application
CMD ["python3", "-u", "producer-traffic.py"]