import sys
from botocore.exceptions import ClientError
from logging import basicConfig, getLogger, INFO
from warnings import filterwarnings
from datetime import datetime
from yfinance import download as download_stock

from pyspark.sql.types import StringType, DoubleType, LongType
from pyspark.sql.functions import lit, col, when, isnan
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from boto3 import client as clientBoto3
from awsglue.context import GlueContext
from pyspark.sql import DataFrame
from awsglue.job import Job

filterwarnings("ignore")

basicConfig(
    level=INFO, format="%(asctime)s - %(levelname)s - %(funcName)s - %(message)s"
)
logger = getLogger(__name__)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

glue_client = clientBoto3("glue")


RAW_DATA_PATH = "s3://stockpy/raw/stocks/"
DATABASE_NAME = "stock_db"
TABLE_NAME = "stock_prices_best_row"
PROCESS_DATE = datetime.now().strftime("%Y%m%d")

STOCKS = {
    "Banks": {"ITUB4.SA": "Itaú Unibanco", "BBAS3.SA": "Banco do Brasil"},
    "Energy": {"ISAE4.SA": "ISA Energia", "CPFE3.SA": "CPFL Energia"},
    "Sanitation": {"SBSP3.SA": "Sabesp", "SAPR4.SA": "Sanepar"},
    "Insurance": {"PSSA3.SA": "Porto Seguro", "BBSE3.SA": "BB Seguridade"},
    "Telecommunications": {"VIVT3.SA": "Vivo", "INTB3.SA": "Intelbras"},
}


class StockDataFetcher:
    """
    Class to fetch stock data from Yahoo Finance using yfinance library.
    """

    def __init__(self, stocks: dict) -> None:
        """
        Innitializes the StockDataFetcher with a dictionary of stocks.
        Args:
            stocks (dict): A dictionary where keys are sectors and values are dictionaries
                           mapping stock tickers to company names.
        """
        logger.info("Innitializing StockDataFetcher Job Extractor")
        self.stocks = stocks

    def _create_stock_mapping(self) -> DataFrame:
        """
        Creates a Spark DataFrame mapping sectors, tickers, and company names.
        Returns:
            DataFrame: A Spark DataFrame with columns "Sector", "Ticker", and "Company
        """
        stock_map = []
        for sector, companies in self.stocks.items():
            for ticker, name in companies.items():
                stock_map.append((sector, ticker, name))
        return spark.createDataFrame(stock_map, ["Sector", "Ticker", "Company"])

    def extract_stock_data(self) -> DataFrame:
        """
        Extracts stock data from Yahoo Finance and returns a cleaned Spark DataFrame.
        Returns:
            DataFrame: A Spark DataFrame containing stock data with metadata.
        """
        df_map = self._create_stock_mapping()
        tickers = [row["Ticker"] for row in df_map.collect()]

        logger.info("Looking up stock data from Yahoo Finance...")
        data = download_stock(
            tickers,
            period="1d",
            interval="1m",
            prepost=True,
            progress=False,
            threads=True,
        )

        if data.empty:
            logger.warning("No data returned from Yahoo Finance.")
            return df_map.withColumn("dataproc", lit(PROCESS_DATE))

        data = data.stack(level=1).reset_index()
        data = data.rename(columns={"Datetime": "Date"})

        data["Date"] = data["Date"].dt.strftime("%Y-%m-%d")
        df_data = spark.createDataFrame(data)

        df_data = df_data.select(
            when(col("Date").isNotNull(), col("Date").cast(StringType()))
            .otherwise(None)
            .alias("Date"),
            when(col("Ticker").isNotNull(), col("Ticker").cast(StringType()))
            .otherwise(None)
            .alias("Ticker"),
            when(
                col("Close").isNotNull() & ~isnan(col("Close")),
                col("Close").cast(DoubleType()),
            )
            .otherwise(None)
            .alias("Close"),
            when(
                col("High").isNotNull() & ~isnan(col("High")),
                col("High").cast(DoubleType()),
            )
            .otherwise(None)
            .alias("High"),
            when(
                col("Low").isNotNull() & ~isnan(col("Low")),
                col("Low").cast(DoubleType()),
            )
            .otherwise(None)
            .alias("Low"),
            when(
                col("Open").isNotNull() & ~isnan(col("Open")),
                col("Open").cast(DoubleType()),
            )
            .otherwise(None)
            .alias("Open"),
            when(
                col("Volume").isNotNull() & ~isnan(col("Volume")),
                col("Volume").cast(LongType()),
            )
            .otherwise(None)
            .alias("Volume"),
        )

        results = df_map.join(df_data, on="Ticker", how="left")
        results = (
            results.withColumnRenamed("Sector", "sector")
            .withColumnRenamed("Ticker", "ticker")
            .withColumnRenamed("Company", "company")
            .withColumnRenamed("Date", "date")
            .withColumnRenamed("Close", "close")
            .withColumnRenamed("High", "high")
            .withColumnRenamed("Low", "low")
            .withColumnRenamed("Open", "open")
            .withColumnRenamed("Volume", "volume")
            .withColumnRenamed("Datetime", "date")
        )

        results = results.withColumn("dataproc", lit(PROCESS_DATE))

        logger.info("Final schema of the DataFrame:")
        results.printSchema()

        return results


@staticmethod
def create_glue_catalog(database: str, table: str, s3_path: str) -> None:
    """
    Creates or updates the Glue Data Catalog database and table with optimized schema.
    Args:
        database (str): The name of the Glue database.
        table (str): The name of the Glue table.
        s3_path (str): The S3 path where the data is stored.
    """

    try:
        glue_client.create_database(
            DatabaseInput={
                "Name": database,
                "Description": f"Database para dados de ações - Criado em {datetime.now().isoformat()}",
            }
        )
        logger.info(f"Database '{database}' created in Glue Catalog.")
    except ClientError as e:
        if e.response["Error"]["Code"] == "AlreadyExistsException":
            logger.info(f"Database '{database}' Already exists. Continuing...")
        else:
            logger.error(f"Error creating database: {e}")
            raise

    table_columns = [
        {"Name": "sector", "Type": "string", "Comment": "Setor da empresa"},
        {"Name": "ticker", "Type": "string", "Comment": "Código da ação"},
        {"Name": "company", "Type": "string", "Comment": "Nome da empresa"},
        {
            "Name": "date",
            "Type": "string",
            "Comment": "Data da cotação (formato YYYY-MM-DD)",
        },
        {"Name": "close", "Type": "double", "Comment": "Preço de fechamento"},
        {"Name": "high", "Type": "double", "Comment": "Preço máximo"},
        {"Name": "low", "Type": "double", "Comment": "Preço mínimo"},
        {"Name": "open", "Type": "double", "Comment": "Preço de abertura"},
        {"Name": "volume", "Type": "bigint", "Comment": "Volume negociado"},
    ]

    partition_keys = [
        {
            "Name": "dataproc",
            "Type": "string",
            "Comment": "Data de processamento (YYYYMMDD)",
        }
    ]

    table_input = {
        "Name": table,
        "Description": f"Tabela de preços de ações - Atualizada em {datetime.now().isoformat()}",
        "StorageDescriptor": {
            "Columns": table_columns,
            "Location": s3_path,
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {"serialization.format": "1"},
            },
            "Compressed": True,
            "Parameters": {
                "classification": "parquet",
                "compressionType": "snappy",
                "typeOfData": "file",
                "parquet.compress": "SNAPPY",
                "projection.enabled": "false",
                "parquet.column.index.access": "false",
                "parquet.enable.dictionary": "false",
            },
        },
        "PartitionKeys": partition_keys,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "EXTERNAL": "TRUE",
            "has_encrypted_data": "false",
            "classification": "parquet",
            "compressionType": "snappy",
            "typeOfData": "file",
            "parquet.column.index.access": "false",
            "parquet.enable.dictionary": "false",
            "skip.header.line.count": "0",
            "columnsOrdered": "true",
        },
    }

    try:
        try:
            glue_client.delete_table(DatabaseName=database, Name=table)
            logger.info(
                f"Table '{database}.{table}' removed to recreate with correct schema."
            )
        except ClientError:
            pass

        glue_client.create_table(DatabaseName=database, TableInput=table_input)
        logger.info(
            f"Table '{database}.{table}' created in Glue Catalog with compatible schema."
        )

    except ClientError as e:
        logger.error(f"Error creating table: {e}")
        raise

    logger.info(f"S3 Location: {s3_path}")


@staticmethod
def add_partition_if_not_exists(
    database: str, table: str, partition_value: str, partition_location: str
) -> None:
    """
    Added a partition to the Glue table if it does not already exist.
    Args:
        database (str): The name of the Glue database.
        table (str): The name of the Glue table.
        partition_value (str): The value of the partition to add (e.g., "20240915").
        partition_location (str): The S3 location of the partition data.
    """
    try:
        glue_client.create_partition(
            DatabaseName=database,
            TableName=table,
            PartitionInput={
                "Values": [partition_value],
                "StorageDescriptor": {
                    "Location": partition_location,
                    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                        "Parameters": {"serialization.format": "1"},
                    },
                    "Parameters": {
                        "classification": "parquet",
                        "compressionType": "snappy",
                        "parquet.enable.dictionary": "false",
                    },
                },
            },
        )
        logger.info(
            f"Partition dataproc={partition_value} added to table {database}.{table}"
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "AlreadyExistsException":
            logger.info(f"Partition dataproc={partition_value} already exists.")
        else:
            logger.error(f"Error creating partition: {e}")


@staticmethod
def repair_table_partitions(database: str, table: str) -> None:
    """
    Repair the partitions of a Glue table.
    Args:
        database (str): The name of the Glue database.
        table (str): The name of the Glue table.
    """
    try:
        spark.sql(f"MSCK REPAIR TABLE {database}.{table}")
        logger.info(f"MSCK REPAIR TABLE executado com sucesso para {database}.{table}")

        partitions_df = spark.sql(f"SHOW PARTITIONS {database}.{table}")
        partitions_count = partitions_df.count()
        logger.info(f"New partitions discovered: {partitions_count}")

        if partitions_count > 0:
            logger.info("New partitions found:")
            partitions_list = [row[0] for row in partitions_df.collect()]
            for partition in partitions_list:
                logger.info(f"  - {partition}")

    except Exception as e:
        logger.warning(f"MSCK REPAIR TABLE failed: {e}")

        try:
            partition_location = f"{RAW_DATA_PATH}dataproc={PROCESS_DATE}/"
            add_partition_if_not_exists(
                database, table, PROCESS_DATE, partition_location
            )
        except Exception as manual_error:
            logger.error(f"Error adding partition manually: {manual_error}")


def main():
    try:
        logger.info("Starting AWS Glue Job for Stock Data Extraction")

        logger.info("Extracting stock data...")
        fetcher = StockDataFetcher(STOCKS)
        df_result = fetcher.extract_stock_data()

        total_rows = df_result.count()
        logger.info(f"Total records extracted: {total_rows}")

        if total_rows == 0:
            logger.error("No data extracted. Aborting pipeline.")
            raise Exception("Data extraction failed - no records found")

        logger.info("Sample of extracted data:")
        sample_data = df_result.limit(3).collect()
        for i, row in enumerate(sample_data, 1):
            logger.info(
                f"  {i}. {row['sector']} | {row['ticker']} | {row['date']} | Close: ${row['close']}"
            )

        logger.info("Saving data to S3 in Parquet format...")
        s3_current_partition = f"{RAW_DATA_PATH}dataproc={PROCESS_DATE}/"
        logger.info(f"Cleaning up existing partition: {s3_current_partition}")

        try:
            existing_df = spark.read.parquet(s3_current_partition)
            logger.info(
                f"Found {existing_df.count()} records in existing partition. Replacing..."
            )
        except:
            logger.info("No existing partition found. Creating new one.")

        (
            df_result.write.mode("append")
            .option("compression", "snappy")
            .option("parquet.enable.dictionary", "false")
            .option("parquet.bloom.filter.enabled", "false")
            .partitionBy("dataproc")
            .parquet(RAW_DATA_PATH)
        )
        logger.info(f"Saving to {s3_current_partition} completed.")

        logger.info("Updating Glue Data Catalog...")
        create_glue_catalog(DATABASE_NAME, TABLE_NAME, RAW_DATA_PATH)

        logger.info("Discovering partitions...")
        repair_table_partitions(DATABASE_NAME, TABLE_NAME)

    except Exception as e:
        logger.error(f"CRITICAL ERROR in pipeline: {e}")
        logger.error("Check the logs above for error details")
        raise

    finally:
        logger.info("Finalizing job...")
        job.commit()


if __name__ == "__main__":
    main()
