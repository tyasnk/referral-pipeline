# Referral Pipeline

This project implements a data pipeline for processing referral data using Apache Spark and AWS S3.

## Prerequisites

Before you begin, ensure you have met the following requirements:

* Python 3.12 or higher
* Docker (optional, for containerized execution)
* AWS account with S3 access

## Installation

1. Create a virtual environment (optional but recommended):
   ```
   python -m venv venv
   source venv/bin/activate
   ```

2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

## Configuration

1. Create a `.env` file in the root directory of the project.
2. Add your AWS credentials to the `.env` file:
   ```
   AWS_ACCESS_KEY=YOUR_AWS_ACCESS_KEY
   AWS_SECRET_ACCESS_KEY=YOUR_AWS_SECRET_ACCESS_KEY
   ```

**Note:** Never commit your `.env` file to version control. Make sure it's listed in your `.gitignore` file.

## Usage

### Running Locally

To run the pipeline locally, use the following command:

```
python main.py -i [s3_source_data_bucket_prefix] -o [s3_output_bucket_path]
```

Replace `[s3_source_data_bucket_prefix]` with the S3 bucket prefix where your source data is located, and `[s3_output_bucket_path]` with the S3 path where you want to save the output. For example:
```
python main.py -i s3a://referral-pipeline/data/raw -o s3a://referral-pipeline/data/result
```

### Running with Docker

1. Build the Docker image:
   ```
   docker build --tag referral-pipeline:0.0.1 .
   ```

2. Run the Docker container:
   ```
   docker run --env-file .env referral-pipeline:0.0.1 -i [s3_source_data_bucket_prefix] -o [s3_output_bucket_path]
   ```

   Make sure to replace `[s3_source_data_bucket_prefix]` and `[s3_output_bucket_path]` with your actual S3 paths.
