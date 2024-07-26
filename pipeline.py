import os
import logging
from typing import Dict

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    when,
    col,
    to_timestamp,
    upper,
    trim,
    monotonically_increasing_id,
    rank,
    desc,
)
from pyspark.sql.types import DoubleType, IntegerType, BooleanType
from pyspark import SparkConf


class ReferralDataPipeline:
    def __init__(self):
        self.logger = self._setup_logger()
        self.spark = self._create_spark_session()
        self.dfs: Dict[str, DataFrame] = {}
        self.cleaned_dfs: Dict[str, DataFrame] = {}
        self.output_df: DataFrame = None

    def _setup_logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def _create_spark_session(self) -> SparkSession:
        self.logger.info("Creating Spark session")
        conf = SparkConf()
        conf.set(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        conf.set("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY"))
        conf.set("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))

        return (
            SparkSession.builder.master("local")
            .config(conf=conf)
            .appName("ReferralDataPipeline")
            .getOrCreate()
        )

    def load(self, input_data_prefix_path: str):
        self.logger.info("Loading data from S3")
        file_names = [
            "user_referrals",
            "user_referral_logs",
            "user_logs",
            "user_referral_statuses",
            "referral_rewards",
            "paid_transactions",
            "lead_log",
        ]

        for name in file_names:
            file_path = f"{input_data_prefix_path}/{name}.csv"
            self.logger.info(f"Loading {name} from {file_path}")
            self.dfs[name] = self._load_csv(file_path)

    def _load_csv(self, file_path: str) -> DataFrame:
        return (
            self.spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(file_path)
        )

    def transform(self):
        self.logger.info("Transforming data")
        self._clean_dataframes()
        self._create_temp_views()
        self._process_data()

    def _clean_dataframes(self):
        self.logger.info("Cleaning dataframes")
        for name, df in self.dfs.items():
            self.cleaned_dfs[name] = self._clean_dataframe(df, name)

    def _clean_dataframe(self, df: DataFrame, name: str) -> DataFrame:
        self.logger.info(f"Cleaning dataframe: {name}")
        # Remove duplicates
        useless_id = ["lead_log", "user_referral_logs", "user_logs"]
        if name in useless_id:
            used_columns = df.columns[1:]
            df = df.dropDuplicates(used_columns)
        else:
            df = df.dropDuplicates()

        # Handle missing values and correct data types
        for column, data_type in df.dtypes:
            if data_type == "string":
                df = df.withColumn(column, trim(col(column)))
                df = df.withColumn(
                    column, when(col(column) == "", None).otherwise(col(column))
                )
                df = df.withColumn(
                    column, when(col(column) == "null", None).otherwise(col(column))
                )
            elif data_type == "double":
                df = df.withColumn(column, col(column).cast(DoubleType()))
            elif data_type == "int":
                df = df.withColumn(column, col(column).cast(IntegerType()))

            if column in ["referral_at", "created_at", "updated_at", "transaction_at"]:
                df = df.withColumn(column, to_timestamp(col(column)))

            if column in ["is_deleted", "is_reward_granted"]:
                df = df.withColumn(column, col(column).cast(BooleanType()))

        # Specific cleaning for each dataframe
        # capitalize homeclub
        if name == "user_logs":
            df = df.withColumn("homeclub", upper(col("homeclub")))
        # drop duplicate lead log by lead id
        elif name == "lead_log":
            df = df.dropDuplicates(["lead_id"])
        # drop duplicate user_referral_logs. keep the latest data
        elif name == "user_referral_logs":
            df = (
                df.withColumn(
                    "rank",
                    rank().over(
                        Window.partitionBy("user_referral_id").orderBy(
                            desc("created_at")
                        )
                    ),
                )
                .filter("rank = 1")
                .drop("rank")
            )

        return df

    def _create_temp_views(self):
        self.logger.info("Creating temporary views")
        for name, df in self.cleaned_dfs.items():
            df.createOrReplaceTempView(name)

    def _process_data(self):
        self.logger.info("Processing data")
        self.spark.sql(
            """
            CREATE OR REPLACE TEMPORARY VIEW cleaned_data AS
            SELECT 
                ur.*,
                url.is_reward_granted,
                ul.name as referrer_name,
                ul.phone_number as referrer_phone_number,
                ul.homeclub as referrer_homeclub,
                ul.membership_expired_date,
                ul.is_deleted,
                urs.description as referral_status,
                CAST(SUBSTRING(rr.reward_value, 1, POSITION(' days' IN rr.reward_value) - 1) AS INTEGER) AS reward_value,
                INITCAP(pt.transaction_status) AS transaction_status,
                pt.transaction_at,
                pt.transaction_location,
                INITCAP(pt.transaction_type) AS transaction_type,
                ll.source_category as lead_source_category,
                from_utc_timestamp(ur.referral_at, 'Asia/Jakarta') as referral_at_local,
                from_utc_timestamp(pt.transaction_at, pt.timezone_transaction) as transaction_at_local,
                from_utc_timestamp(ur.updated_at, 'Asia/Jakarta') as updated_at_local,
                from_utc_timestamp(url.created_at, 'Asia/Jakarta') as reward_granted_at_local
            FROM user_referrals ur
            LEFT JOIN user_referral_logs url ON ur.referral_id = url.user_referral_id
            LEFT JOIN user_logs ul ON ur.referrer_id = ul.user_id
            LEFT JOIN user_referral_statuses urs ON ur.user_referral_status_id = urs.id
            LEFT JOIN referral_rewards rr ON ur.referral_reward_id = rr.id
            LEFT JOIN paid_transactions pt ON ur.transaction_id = pt.transaction_id
            LEFT JOIN lead_log ll ON ur.referee_id = ll.lead_id AND ur.referral_source = 'Lead'
            """
        )

        self.output_df = self.spark.sql(
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
                        ((reward_value > 0) AND (is_reward_granted IS NULL OR is_reward_granted = FALSE))
                    ) THEN FALSE
                    ELSE NULL
                END as is_business_logic_valid
            FROM cleaned_data
            """
        )

    def save(self, output_path: str):
        self.logger.info("Saving processed data")
        self.output_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
            output_path
        )
        self.logger.info(f"Results saved to {output_path}")

    def run(
        self,
        input_data_prefix_path: str,
        output_path: str,
    ):
        self.logger.info("Starting Referral Data Pipeline")
        try:
            self.load(input_data_prefix_path=input_data_prefix_path)
            self.transform()
            self.save(output_path=output_path)
            self.logger.info("Referral Data Pipeline completed successfully")
        except Exception as e:
            self.logger.error(f"An error occurred during pipeline execution: {str(e)}")
        finally:
            self.logger.info("Stopping Spark session")
            self.spark.stop()
