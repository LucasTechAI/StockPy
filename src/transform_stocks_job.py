import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame

from pyspark.sql.types import StringType, StructType, StructField, DoubleType, LongType
from pyspark.sql.functions import col, to_date, lit, avg, sum, lag
from logging import getLogger, basicConfig, INFO
from pyspark.sql.window import Window
from datetime import datetime

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

basicConfig(level=INFO, format="%(asctime)s - %(levelname)s - %(funcName)s - %(message)s")
logger = getLogger(__name__)


DATABASE = "stock_db"
TABLE = "stocks_clean"
PROCESS_DATE = datetime.now().strftime("%Y%m%d")

RAW_PATH = f"s3://stockpy/raw/stocks/dataproc={PROCESS_DATE}/"
OUTPUT_PATH = "s3://stockpy/refined/stocks/"


@staticmethod
def table_exists(database: str, table: str) -> bool:
    """
    Check if a table exists in the Glue Catalog.
    Args:
        database (str): The database name.
        table (str): The table name.
    Returns:
        bool: True if the table exists, False otherwise.
    """
    try:
        spark.sql(f"DESCRIBE TABLE {database}.{table}")
        logger.info(f"Table found: {database}.{table}")
        return True
    except Exception:
        logger.warning(f"Table NOT found: {database}.{table}")
        return False


@staticmethod
def create_table_if_not_exists(database: str, table: str, location: str) -> None:
    """
    Create a Glue Catalog table if it does not exist.
    Args:
        database (str): The database name.
        table (str): The table name.
        location (str): The S3 location for the table data.
    """

    if not table_exists(database, table):
        ddl = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {database}.{table} (
            codigoAcao STRING,
            nomeEmpresa STRING,
            data DATE,
            precoFechamento DOUBLE,
            precoMaximo DOUBLE,
            precoMinimo DOUBLE,
            precoAbertura DOUBLE,
            volumeNegociacao BIGINT,
            mediaFechamento DOUBLE,
            totalVolume BIGINT,
            variacaoFechamento DOUBLE
        )
        PARTITIONED BY (dataproc STRING, setor STRING)
        STORED AS PARQUET
        LOCATION '{location}'
        """
        spark.sql(ddl)
        logger.info(f"Table {database}.{table} created successfully!")


@staticmethod
def repair_table_partitions(database: str, table: str) -> None:
    """
    Repair table partitions in the Glue Catalog.
    Args:
        database (str): The database name.
        table (str): The table name.
    """
    try:
        spark.sql(f"MSCK REPAIR TABLE {database}.{table}")
        logger.info(f"MSCK REPAIR TABLE executed {database}.{table}")
        partitions_df = spark.sql(f"SHOW PARTITIONS {database}.{table}")
        total_part = partitions_df.count()
        logger.info(f"Total partitions discovered: {total_part}")
    except Exception as e:
        logger.error(f"Failed REPAIR TABLE: {e}")


@staticmethod
def get_raw_data(path: str) -> DataFrame:
    """
    Read raw data from S3 in Parquet format.
    Args:
        path (str): The S3 path to read data from.
    Returns:
        DataFrame: The loaded DataFrame.
    """
    df = spark.read.parquet(path)
    count = df.count()
    logger.info(f"Records read: {count}")
    df.printSchema()
    return df


@staticmethod
def clean_data(df: DataFrame) -> DataFrame:
    """
    Clean the raw DataFrame by casting types and filtering invalid data.
    Args:
        df (DataFrame): The raw DataFrame.
    Returns:
        DataFrame: The cleaned DataFrame.
    """
    df_clean = df \
        .withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
        .withColumn("close", col("close").cast(DoubleType())) \
        .withColumn("high", col("high").cast(DoubleType())) \
        .withColumn("low", col("low").cast(DoubleType())) \
        .withColumn("open", col("open").cast(DoubleType())) \
        .withColumn("volume", col("volume").cast(LongType())) \
        .filter(col("sector").isNotNull()) \
        .filter(col("close") > 0) \
        .filter(col("volume") > 0)
    return df_clean


@staticmethod
def rename_columns(df: DataFrame) -> DataFrame:
    """
    Rename columns to match the desired schema.
    Args:
        df (DataFrame): The DataFrame to rename columns for.
    Returns:
        DataFrame: The DataFrame with renamed columns.
    """
    df = df \
        .withColumnRenamed("date", "data") \
        .withColumnRenamed("close", "precoFechamento") \
        .withColumnRenamed("high", "precoMaximo") \
        .withColumnRenamed("low", "precoMinimo") \
        .withColumnRenamed("open", "precoAbertura") \
        .withColumnRenamed("volume", "volumeNegociacao") \
        .withColumnRenamed("ticker", "codigoAcao") \
        .withColumnRenamed("sector", "setor") \
        .withColumnRenamed("company", "nomeEmpresa")
    return df


@staticmethod
def calculate_aggregations(df: DataFrame) -> DataFrame:
    """
    Calculate required aggregations: average closing price and total trading volume by sector.
    Args:
        df (DataFrame): The input DataFrame.
    Returns:
        DataFrame: The DataFrame with aggregated values.
    """
    df_agg = df.groupBy("setor").agg(
        avg("precoFechamento").alias("mediaFechamento"),
        sum("volumeNegociacao").alias("totalVolume")
    )
    return df.join(df_agg, on="setor", how="left")


@staticmethod
def calculate_date_diff(df: DataFrame) -> DataFrame:
    """
    Calculate the difference in closing price between consecutive days.
    Args:
        df (DataFrame): The input DataFrame.
    Returns:
        DataFrame: The DataFrame with date difference columns.
    """
    windowSpec = Window.partitionBy("codigoAcao").orderBy("data")
    df = df.withColumn("fechamentoAnterior", lag("precoFechamento").over(windowSpec))
    df = df.withColumn("variacaoFechamento", col("precoFechamento") - col("fechamentoAnterior"))
    df = df.drop("fechamentoAnterior")
    return df


@staticmethod
def save_to_s3(df: DataFrame, output_path: str) -> None:
    """
    Save the DataFrame to S3 in Parquet format.
    Args:
        df (DataFrame): The DataFrame to save.
        output_path (str): The S3 path to save the DataFrame to.
    """
    df.show(10, truncate=False)
    df.write \
        .mode("append") \
        .option("compression", "snappy") \
        .partitionBy("dataproc", "setor") \
        .parquet(output_path)
    logger.info(f"Data saved to: {output_path}")


def main():
    try:
        logger.info("Innitializing stock transformation job")

        logger.info(f"Reading raw data from: {RAW_PATH}")
        df_raw = get_raw_data(RAW_PATH)
        df_raw.show()

        logger.info("Cleaning data")
        df_clean = clean_data(df_raw)
        df_clean.printSchema()
        df_clean.show(10, truncate=False)

        # Requirement B: Rename columns to match the specified schema
        ## Rename all columns to Portuguese and add 'dataproc' column with the processing date
        logger.info("[Requirement B] Renaming columns and adding dataproc")
        df_final = rename_columns(df_clean).withColumn("dataproc", lit(PROCESS_DATE))
        df_final.printSchema()
        df_final.show(10, truncate=False)

        # Requirement A: Calculate aggregations
        ## Average closing price by sector and total trading volume by sector
        logger.info("[Requirement A] Calculating aggregations")
        df_final = calculate_aggregations(df_final)
        df_final.show(10, truncate=False)
        df_final.printSchema()

        # Requirement C: Calculate date differences
        ## Difference in closing price between consecutive days for each stock
        logger.info("[Requirement C] Calculating date differences")
        df_final = calculate_date_diff(df_final)
        df_final.show(10, truncate=False)
        df_final.printSchema()

        logger.info(f"Creating Glue table if not exists: {DATABASE}.{TABLE}")
        create_table_if_not_exists(DATABASE, TABLE, OUTPUT_PATH)

        logger.info(f"Saving final data to S3: {OUTPUT_PATH}")
        save_to_s3(df_final, OUTPUT_PATH)

        logger.info(f"Repairing table partitions: {DATABASE}.{TABLE}")
        repair_table_partitions(DATABASE, TABLE)

        logger.info("Stock transformation job completed successfully!")

    except Exception as e:
        logger.error(f"Error occurred: {e}", exc_info=True)
        raise

    finally:
        job.commit()
        spark.stop()

if __name__ == "__main__":
    main()
