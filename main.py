from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    when,
    col,
    to_timestamp,
    upper,
    trim,
)
from pyspark.sql.types import DoubleType, IntegerType, BooleanType
from pyspark import SparkConf
import os

# Initialize Spark session
conf = SparkConf()
conf.set(
    "spark.jars.packages",
    "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
)
conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY"))
conf.set("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))

spark = (
    SparkSession.builder.master("local")
    .config(conf=conf)
    .appName("ReferralDataPipeline")
    .getOrCreate()
)


# Load data
def load_csv(file_path: str) -> SparkSession:
    return (
        spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
    )


# Load all dataframes
dfs = {
    "user_referrals": load_csv("data/user_referrals.csv"),
    "user_referral_logs": load_csv("data/user_referral_logs.csv"),
    "user_logs": load_csv("data/user_logs.csv"),
    "user_referral_statuses": load_csv("data/user_referral_statuses.csv"),
    "referral_rewards": load_csv("data/referral_rewards.csv"),
    "paid_transactions": load_csv("data/paid_transactions.csv"),
    "lead_logs": load_csv("data/lead_log.csv"),
}


# Data cleaning function
def clean_dataframe(df: DataFrame, name: str) -> DataFrame:
    # Remove duplicates
    useless_id = ["lead_logs", "user_referral_logs", "user_logs"]
    if name in useless_id:
        used_columns = df.columns[1:]
        df = df.dropDuplicates(used_columns)
    else:
        df = df.dropDuplicates()

    # Handle missing values and correct data types
    for column, data_type in df.dtypes:
        if data_type == "string":
            df = df.withColumn(column, trim(col(column)))  # Trim whitespace
            df = df.withColumn(
                column, when(col(column) == "", None).otherwise(col(column))
            )  # Replace empty strings with None
        elif data_type == "double":
            df = df.withColumn(column, col(column).cast(DoubleType()))
        elif data_type == "int":
            df = df.withColumn(column, col(column).cast(IntegerType()))

        # Convert timestamp columns
        if column in ["referral_at", "created_at", "updated_at", "transaction_at"]:
            df = df.withColumn(column, to_timestamp(col(column)))

        # Convert boolean columns
        if column in ["is_deleted", "is_reward_granted"]:
            df = df.withColumn(column, col(column).cast(BooleanType()))

    # Specific cleaning for each dataframe
    if name == "user_logs":
        df = df.withColumn("homeclub", upper(col("homeclub")))
    elif name == "lead_logs":
        df = df.dropDuplicates(["lead_id"])

    return df


# Clean all dataframes
cleaned_dfs = {name: clean_dataframe(df, name) for name, df in dfs.items()}

# Create temporary views
for name, df in cleaned_dfs.items():
    df.createOrReplaceTempView(name)

# Data processing
spark.sql(
    """
CREATE OR REPLACE TEMPORARY VIEW cleaned_data AS
SELECT 
    ur.*,
    url.is_reward_granted,
    url.created_at as url_created_at,
    ul.name as referrer_name,
    ul.phone_number as referrer_phone_number,
    ul.homeclub as referrer_homeclub,
    ul.membership_expired_date,
    ul.is_deleted,
    urs.description as referral_status,
    CAST(SUBSTRING(rr.reward_value, 1, POSITION(' days' IN rr.reward_value) - 1) AS INTEGER) AS reward_value,
    pt.transaction_status,
    pt.transaction_at,
    pt.transaction_location,
    pt.transaction_type,
    ll.source_category as lead_source_category,
    from_utc_timestamp(ur.referral_at, 'Asia/Jakarta') as referral_at_local,
    from_utc_timestamp(pt.transaction_at, pt.timezone_transaction) as transaction_at_local,
    from_utc_timestamp(ur.updated_at, 'Asia/Jakarta') as updated_at_local,
    from_utc_timestamp(url.created_at, 'Asia/Jakarta') as reward_granted_at_local
FROM user_referrals ur
LEFT JOIN user_referral_logs url ON ur.referral_id = url.user_referral_id AND ur.updated_at = url.created_at
LEFT JOIN user_logs ul ON ur.referrer_id = ul.user_id
LEFT JOIN user_referral_statuses urs ON ur.user_referral_status_id = urs.id
LEFT JOIN referral_rewards rr ON ur.referral_reward_id = rr.id
LEFT JOIN paid_transactions pt ON ur.transaction_id = pt.transaction_id
LEFT JOIN lead_logs ll ON ur.referee_id = ll.lead_id AND ur.referral_source = 'Lead'
"""
)

# Business logic implementation and final output (unchanged)
output_df: DataFrame = spark.sql(
    """
SELECT
    monotonically_increasing_id() as referral_details_id,
    referral_id,
    referral_source,
    CASE
        WHEN referral_source = 'User Sign Up' THEN 'Online'
        WHEN referral_source = 'Draft Transaction' THEN 'Offline'
        WHEN referral_source = 'Lead' THEN lead_source_category
        ELSE NULL
    END as referral_source_category,
    referral_at_local as referral_at,
    referrer_id,
    referrer_name,
    referrer_phone_number,
    referrer_homeclub,
    referee_id,
    referee_name,
    referee_phone,
    referral_status,
    reward_value as num_reward_days,
    transaction_id,
    transaction_status,
    transaction_at_local as transaction_at,
    transaction_location,
    transaction_type,
    updated_at_local as updated_at,
    CASE WHEN is_reward_granted THEN reward_granted_at_local END as reward_granted_at,
    CASE
        WHEN (
            (reward_value > 0) AND
            (referral_status = 'Berhasil') AND
            (transaction_id IS NOT NULL) AND
            (transaction_status = 'Paid') AND
            (transaction_type = 'New') AND
            (transaction_at_local > referral_at_local) AND
            (MONTH(transaction_at_local) = MONTH(referral_at_local)) AND
            (membership_expired_date > CURRENT_DATE()) AND
            (is_deleted = FALSE) AND
            (is_reward_granted = TRUE)
        ) OR (
            (referral_status IN ('Menunggu', 'Tidak Berhasil')) AND
            (reward_value IS NULL)
        ) THEN TRUE
        WHEN (
            ((reward_value > 0) AND (referral_status != 'Berhasil')) OR
            ((reward_value > 0) AND (transaction_id IS NULL)) OR
            ((reward_value IS NULL) AND (transaction_id IS NOT NULL) AND (transaction_status = 'Paid') AND (transaction_at_local > referral_at_local)) OR
            ((referral_status = 'Berhasil') AND (reward_value IS NULL OR reward_value = 0)) OR
            (transaction_at_local < referral_at_local) OR
            ((reward_value > 0) AND (is_reward_granted IS NULL))
        ) THEN FALSE
        ELSE NULL
    END as is_business_logic_valid
FROM cleaned_data
"""
)

# Output
output_path = "referral_system_analysis"
output_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
    f"s3a://referral-pipeline/{output_path}"
)

print(f"Analysis complete. Results saved to {output_path}")

# Stop Spark session
spark.stop()
