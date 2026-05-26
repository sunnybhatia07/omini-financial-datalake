# AWS Lambda Python 3.12 base image
FROM public.ecr.aws/lambda/python:3.12

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt --no-cache-dir

# Copy project files
COPY ingestion/ ./ingestion/
COPY utils/ ./utils/
COPY lambda_handler.py .

# Lambda entry point — file.function
CMD ["lambda_handler.lambda_handler"]