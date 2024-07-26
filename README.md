# Referral Pipeline

This project implements a data pipeline for processing referral data using Apache Spark and AWS S3.

## Prerequisites

Before you begin, ensure you have met the following requirements:

* Python 3.12
* Docker (for containerized execution)
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

## Data Dictionary

### Output DataFrame

| Column Name | Data Type | Description | Example |
|-------------|-----------|-------------|---------|
| referral_details_id | Long | Unique identifier for each row in the output dataset | 1
| referral_id | String | Unique identifier for each referral | 962788593e04380982da789b978dcad6 |
| referral_source | String | Source of the referral (e.g., 'User Sign Up', 'Draft Transaction', 'Lead') | Draft Transaction
| referral_source_category | String | Category of the referral source ('Online', 'Offline', or lead source category) | Offline
| referral_at | Timestamp | Local timestamp when the referral was made | 2024-05-01T19:22:16.000+07:00 |
| referrer_id | String | Unique identifier of the user who made the referral | 2c71c5d66c7e12a0b3c200ba6ed3b78e |
| referrer_name | String | Name of the user who made the referral | 6380232145160dca709cdb11ae47fb2a |
| referrer_phone_number | String | Phone number of the user who made the referral | 87e6571bf783832fffc616a308563e7e |
| referrer_homeclub | String | Home club of the user who made the referral | PLUIT |
| referee_id | String | Unique identifier of the user who was referred | 09761f520c39620c1fc95b31d2a3047d |
| referee_name | String | Name of the user who was referred | 4bbc7361bafa86ca236399f1029f537f |
| referee_phone | String | Phone number of the user who was referred | d2c7bbb4e088e23e6612c0f2a95e022b |
| referral_status | String | Current status of the referral (e.g., 'Berhasil', 'Menunggu', 'Tidak Berhasil') | Menunggu |
| num_reward_days | Integer | Number of days granted as a reward for the referral | 10 |
| transaction_id | String | Unique identifier of the associated transaction | 62afd56341d234457415c97c9c866aa9 |
| transaction_status | String | Status of the associated transaction (e.g., 'PAID') | PAID |
| transaction_at | Timestamp | Local timestamp when the transaction occurred | 2024-05-03T16:00:59.409+07:00 |
| transaction_location | String | Location where the transaction took place | MAMPANG |
| transaction_type | String | Type of the transaction (e.g., 'NEW') | NEW |
| updated_at | Timestamp | Local timestamp when the referral was last updated | 2024-05-13T17:30:51.000+07:00 |
| reward_granted_at | Timestamp | Local timestamp when the reward was granted (null if not granted) | 2024-05-01T19:17:31.000+07:00
| is_business_logic_valid | Boolean | Indicates whether the referral meets all business logic criteria | true |