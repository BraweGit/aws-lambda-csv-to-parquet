# Set a base image that includes lambda runtime API
# Source: https://hub.docker.com/r/amazon/aws-lambda-python
FROM amazon/aws-lambda-python:3.8

# Optional: ensure that pip is up to date
RUN /var/lang/bin/python3.8 -m pip install --upgrade pip

# First we copy requirements.txt to ensure later builds
# with changes to our src code will be faster due to caching of this layer
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy all from src/ directory to docker image
COPY src/ .

# Specify lambda handler that will be invoked on container start
CMD ["lambda.lambda_handler"]