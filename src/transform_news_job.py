import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql.functions import col, regexp_replace, to_date, lit
from logging import getLogger, basicConfig, INFO
from botocore.exceptions import ClientError
from pyspark.sql.types import StringType
from boto3 import client as boto3_client
from pyspark.sql import DataFrame
from datetime import datetime


glue_client = boto3_client("glue")
athena_client = boto3_client("athena", region_name="us-east-1")

basicConfig(
    level=INFO, format="%(asctime)s - %(levelname)s - %(funcName)s - %(message)s"
)
logger = getLogger(__name__)


SOURCE_DB = "news_db"
SOURCE_TABLE = "news_raw"
TARGET_BUCKET = "stockpy"
TARGET_PREFIX = "refined/news"
TARGET_TABLE = "news_clean"

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


@staticmethod
def normalize_column_names(df: DataFrame) -> DataFrame:
    """
    Normalize column names by stripping whitespace and converting to lowercase.
    Args:
        df (DataFrame): Input DataFrame with raw column names.
    Returns:
        DataFrame: DataFrame with normalized column names.
    """
    logger.info("Normalizing column names...")
    column_mapping = {}
    expected_columns = [
        "title",
        "link",
        "source",
        "published_time",
        "company",
        "ticker",
        "sector",
        "search_term",
        "extracted_at",
    ]
    current_columns = [col.lower().strip() for col in df.columns]
    missing_columns = []
    for expected_col in expected_columns:
        if expected_col not in current_columns:
            possible_matches = [
                col
                for col in current_columns
                if expected_col.replace("_", "") in col.replace("_", "")
            ]
            if possible_matches:
                column_mapping[possible_matches[0]] = expected_col
                logger.info(f"Mapping '{possible_matches[0]}' to '{expected_col}'")
            else:
                missing_columns.append(expected_col)
    if missing_columns:
        for missing_col in missing_columns:
            df = df.withColumn(missing_col, lit(None).cast(StringType()))
            logger.info(f"Added column '{missing_col}' with null values")
    for old_name, new_name in column_mapping.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
    return df


@staticmethod
def clean_data(df: DataFrame) -> DataFrame:
    """
    Clean the input DataFrame by normalizing column names and filtering rows.
    Args:
        df (DataFrame): Input DataFrame to be cleaned.
    Returns:
        DataFrame: Cleaned DataFrame.
    """
    logger.info("Starting data cleaning process...")
    df = normalize_column_names(df)
    required_columns = ["published_time", "extracted_at"]

    for req_col in required_columns:
        if req_col not in df.columns:
            raise ValueError(f"Column '{req_col}' is required but was not found")

    df = df.withColumn(
        "published_date_str", regexp_replace(col("published_time"), "T.*", "")
    ).withColumn("extracted_date_str", regexp_replace(col("extracted_at"), "T.*", ""))

    df = df.withColumn(
        "published_date", to_date(col("published_date_str"), "yyyy-MM-dd")
    ).withColumn("extracted_date", to_date(col("extracted_date_str"), "yyyy-MM-dd"))

    df = df.filter(
        col("published_date").isNotNull()
        & col("extracted_date").isNotNull()
        & (col("published_date") == col("extracted_date"))
    )

    df = df.drop(
        "published_date_str", "extracted_date_str", "published_date", "extracted_date"
    )
    df = df.dropDuplicates()

    if "link" in df.columns:
        df = df.dropDuplicates(["link"])

    critical_columns = ["title", "link", "source"]
    existing_critical_columns = [c for c in critical_columns if c in df.columns]
    if existing_critical_columns:
        df = df.na.drop(subset=existing_critical_columns)

    if df.count() == 0:
        logger.warning("DataFrame is empty after cleaning!")
    df.show()
    return df


@staticmethod
def save_to_s3_and_catalog(df: DataFrame) -> None:
    """
    Save the cleaned DataFrame to S3 in Parquet format and register it in the Glue Catalog.
    Args:
        df (DataFrame): Cleaned DataFrame to be saved.
    """
    logger.info("Saving cleaned data to S3 and registering in Glue Catalog...")
    if df.count() == 0:
        logger.warning("DataFrame is empty, skipping save...")
        return

    proc_date = datetime.now().strftime("%Y%m%d")
    output_path = f"s3://{TARGET_BUCKET}/{TARGET_PREFIX}"
    logger.info(f"Saving to {output_path}")

    df = df.withColumn("dataproc", lit(proc_date))
    df.write.mode("append").option("compression", "snappy").partitionBy(
        "dataproc"
    ).parquet(output_path)

    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
    glueContext.write_dynamic_frame.from_catalog(
        frame=dynamic_frame,
        database=SOURCE_DB,
        table_name=TARGET_TABLE,
        additional_options={"partitionKeys": ["dataproc"]},
    )
    logger.info("Data registered in Glue Catalog.")


@staticmethod
def ensure_target_table_exists() -> None:
    """Ensure that the target table exists in the Glue Catalog"""
    try:
        glue_client.get_table(DatabaseName=SOURCE_DB, Name=TARGET_TABLE)
        logger.info(f"Tabela {SOURCE_DB}.{TARGET_TABLE} já existe no Glue Catalog.")
    except glue_client.exceptions.EntityNotFoundException:
        logger.warning(f"Tabela {SOURCE_DB}.{TARGET_TABLE} não existe. Criando...")

        try:
            glue_client.create_table(
                DatabaseName=SOURCE_DB,
                TableInput={
                    "Name": TARGET_TABLE,
                    "StorageDescriptor": {
                        "Columns": [
                            {"Name": "title", "Type": "string"},
                            {"Name": "link", "Type": "string"},
                            {"Name": "source", "Type": "string"},
                            {"Name": "published_time", "Type": "string"},
                            {"Name": "company", "Type": "string"},
                            {"Name": "ticker", "Type": "string"},
                            {"Name": "sector", "Type": "string"},
                            {"Name": "search_term", "Type": "string"},
                            {"Name": "extracted_at", "Type": "string"},
                        ],
                        "Location": f"s3://{TARGET_BUCKET}/{TARGET_PREFIX}/",
                        "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                        "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                        "SerdeInfo": {
                            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                            "Parameters": {"serialization.format": "1"},
                        },
                    },
                    "PartitionKeys": [{"Name": "dataproc", "Type": "string"}],
                    "TableType": "EXTERNAL_TABLE",
                    "Parameters": {
                        "classification": "parquet",
                        "compressionType": "snappy",
                    },
                },
            )
            logger.info(f"Tabela {SOURCE_DB}.{TARGET_TABLE} criada com sucesso!")
        except ClientError as ce:
            logger.error(f"Erro ao criar tabela: {ce}")
            raise


@staticmethod
def add_partition_if_not_exists(
    database: str, table: str, proc_date: str, location: str
) -> None:
    """
    Add a partition to the Glue Catalog if it does not exist.
    Args:
        database (str): The database name.
        table (str): The table name.
        proc_date (str): The partition value (e.g., '20231010').
        location (str): The S3 location for the partition.
    """
    try:
        glue_client.get_partition(
            DatabaseName=database, TableName=table, PartitionValues=[proc_date]
        )
        logger.info(f"Partição {proc_date} já existe no Glue Catalog.")
    except glue_client.exceptions.EntityNotFoundException:
        logger.info(f"Partição {proc_date} não existe. Criando...")
        glue_client.create_partition(
            DatabaseName=database,
            TableName=table,
            PartitionInput={
                "Values": [proc_date],
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": "title", "Type": "string"},
                        {"Name": "link", "Type": "string"},
                        {"Name": "source", "Type": "string"},
                        {"Name": "published_time", "Type": "string"},
                        {"Name": "company", "Type": "string"},
                        {"Name": "ticker", "Type": "string"},
                        {"Name": "sector", "Type": "string"},
                        {"Name": "search_term", "Type": "string"},
                        {"Name": "extracted_at", "Type": "string"},
                    ],
                    "Location": location,
                    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                        "Parameters": {"serialization.format": "1"},
                    },
                },
            },
        )
        logger.info(f"Partition {proc_date} created successfully.")


@staticmethod
def repair_table_partitions(database: str, table: str) -> None:
    """
    Repair table partitions using Spark SQL, Athena, or manual addition.
    Args:
        database (str): The database name.
        table (str): The table name.
    """
    try:
        spark.sql(f"MSCK REPAIR TABLE {database}.{table}")
        logger.info(f"MSCK REPAIR TABLE executed on Spark for {database}.{table}")

        partitions_df = spark.sql(f"SHOW PARTITIONS {database}.{table}")
        logger.info(f"Total partitions: {partitions_df.count()}")
        for row in partitions_df.collect():
            logger.info(f"  - {row[0]}")

    except Exception as e:
        logger.warning(f"MSCK REPAIR TABLE failed on Spark: {e}")

        try:
            query = f"MSCK REPAIR TABLE {table}"
            response = athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={"Database": database},
                ResultConfiguration={
                    "OutputLocation": f"s3://{TARGET_BUCKET}/athena-query-results/"
                },
            )
            qid = response["QueryExecutionId"]
            logger.info(f"MSCK REPAIR TABLE started on Athena (QueryExecutionId={qid})")
        except Exception as athena_error:
            logger.warning(f"MSCK REPAIR TABLE failed on Athena: {athena_error}")
            try:
                proc_date = datetime.now().strftime("%Y%m%d")
                partition_location = (
                    f"s3://{TARGET_BUCKET}/{TARGET_PREFIX}/dataproc={proc_date}/"
                )
                add_partition_if_not_exists(
                    database, table, proc_date, partition_location
                )
            except Exception as manual_error:
                logger.error(f"Error adding partition manually: {manual_error}")


def main():
    try:
        logger.info("Ensuring output table in Catalog...")
        ensure_target_table_exists()

        logger.info("Reading Raw Data")
        df_raw = None

        if SOURCE_TABLE:
            logger.info(f"Reading from Catalog: {SOURCE_DB}.{SOURCE_TABLE}")
            dyf_raw = glueContext.create_dynamic_frame.from_catalog(
                database=SOURCE_DB, table_name=SOURCE_TABLE
            )
            df_raw = dyf_raw.toDF()

        if df_raw is None or df_raw.count() == 0:
            proc_date = datetime.now().strftime("%Y%m%d")
            input_path = f"s3://{TARGET_BUCKET}/raw/news/dataproc={proc_date}/"
            logger.info(f"Catalog is empty. Reading directly from S3: {input_path}")
            df_raw = spark.read.parquet(input_path)

        if df_raw.count() == 0:
            logger.warning("Source DataFrame is empty!")
            return

        logger.info("Cleaning Data")
        df_clean = clean_data(df_raw)

        logger.info("Saving to S3 and Catalog")
        save_to_s3_and_catalog(df_clean)

        logger.info("Repairing Partitions")
        repair_table_partitions(SOURCE_DB, TARGET_TABLE)

        logger.info("Job finished successfully!")

    except Exception as e:
        logger.error(f"Erro durante execução do job: {e}")
        raise e
    finally:
        job.commit()


if __name__ == "__main__":
    main()
