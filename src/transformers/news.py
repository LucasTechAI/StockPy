from pyspark.sql.functions import col, to_date, regexp_replace, to_date
from pyspark.sql.types import StringType, StructType, StructField
from logging import getLogger, basicConfig, INFO
from pyspark.sql import DataFrame, SparkSession
#from awsglue.utils import getResolvedOptions
#from awsglue.context import GlueContext
from pyspark.context import SparkContext
#from pyspark.sql.functions import *
#from awsglue.transforms import *
from datetime import datetime
#from awsglue.job import Job
import sys

basicConfig(level=INFO, format="%(asctime)s - %(levelname)s - %(funcName)s - %(message)s")
logger = getLogger(__name__)

# Arguments for Glue job
""" args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'source_bucket',
    'target_bucket',
    'source_key',
    'target_key'
]) """

# Initialize Spark and Glue contexts
""" sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args) """


""" # Configurações padrão se não fornecidas
source_bucket = args.get('source_bucket', 'bucket-dados-b3')
target_bucket = args.get('target_bucket', 'bucket-dados-b3')
source_key = args.get('source_key', 'raw-data/acoes-b3.csv')
target_key = args.get('target_key', 'processed-data/acoes-agregado/')

print(f"Iniciando processamento do job: {args['JOB_NAME']}")
print(f"Fonte: s3://{source_bucket}/{source_key}")
print(f"Destino: s3://{target_bucket}/{target_key}") """


spark = SparkSession.builder \
    .appName("TransformacoesAcoesB3") \
    .config("spark.sql.ansi.enabled", "false") \
    .getOrCreate()


def get_raw_data(bucket: str = "bucket-dados-b3", key: str = "raw-data/acoes-b3.csv", schema: StructType = None, path: str = "/home/lucas/StockPy/data/raw/news_data.csv") -> DataFrame:
    """
    Get raw data from S3 bucket or local path for testing.
    Args:
        bucket (str): S3 bucket name.
        key (str): S3 object key.
        schema (StructType): Schema for the DataFrame.
        path (str): Local path for testing.
    Returns:
        DataFrame: Raw data as a Spark DataFrame.
    """
    dataframe = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .schema(schema) \
        .csv(path)
    logger.info(f"Registros lidos: {dataframe.count()}")
    logger.info("Show schema of raw data:")
    dataframe.printSchema()   
    logger.info(f"Count data read: {dataframe.count()}")
    return dataframe

def clean_data(dataframe: DataFrame) -> DataFrame:
    """
    Clean the raw data by applying necessary transformations.
    Filters news to keep only those where publication date equals extraction date.
    Args:
        dataframe (DataFrame): Raw data DataFrame.
    Returns:
        DataFrame: Cleaned data DataFrame.
    """
    
    logger.info("Starting data cleaning process...")
    
    dataframe = dataframe \
        .withColumn("published_date_str", 
                   regexp_replace(col("published_time"), "T.*", "")) \
        .withColumn("extracted_date_str", 
                   regexp_replace(col("extracted_at"), "T.*", ""))
    
    dataframe = dataframe \
        .withColumn("published_date", to_date(col("published_date_str"), "yyyy-MM-dd")) \
        .withColumn("extracted_date", to_date(col("extracted_date_str"), "yyyy-MM-dd"))
    
    count_before_filter = dataframe.count()
    logger.info(f"Records before date filtering: {count_before_filter}")

    dataframe = dataframe \
        .filter(
            col("published_date").isNotNull() & 
            col("extracted_date").isNotNull() & 
            (col("published_date") == col("extracted_date"))
        )
    
    count_after_filter = dataframe.count()
    logger.info(f"Records after date filtering (same day): {count_after_filter}")
    logger.info(f"Records filtered out: {count_before_filter - count_after_filter}")

    dataframe = dataframe \
        .drop("published_date_str", "extracted_date_str", "published_date", "extracted_date") \
        .dropDuplicates() \
        .na.drop(subset=["title", "link", "source", "published_time", "company", "ticker", "sector", "search_term", "extracted_at"])

    dataframe = dataframe.select(
        "title", 
        "link", 
        "source", 
        "published_time", 
        "company", 
        "ticker", 
        "sector", 
        "search_term", 
        "extracted_at"
    )
    
    count_final = dataframe.count()
    logger.info(f"Final record count after all cleaning: {count_final}")

    logger.info("Checking for duplicate links...")
    dataframe = dataframe.dropDuplicates(["link"])
                
    logger.info(f"Final record count after removing duplicate links: {dataframe.count()}")
    logger.info("Data cleaning process completed.")
    return dataframe

def save_to_s3(df: DataFrame) -> None:
    """
    Save the final DataFrame to S3 in Parquet format.
    Args:
        df (DataFrame): Final DataFrame to save.
    """
    output_path = f"data/refined/{datetime.now().strftime('%Y%m%d')}/news_parquet"
    df.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(output_path)
    logger.info(f"Data saved successfully to: {output_path}")

def main() -> None:
    try:
        logger.info("Starting job execution [TRANSFORMATIONS-NEWS]...")

        schema = StructType([
            StructField("title", StringType(), True),
            StructField("link", StringType(), True),
            StructField("source", StringType(), True),
            StructField("published_time", StringType(), True),
            StructField("company", StringType(), True),
            StructField("ticker", StringType(), True),
            StructField("sector", StringType(), True),
            StructField("search_term", StringType(), True),
            StructField("extracted_at", StringType(), True)
        ])

        path = "data/raw/news_data.csv"# temporary path for local testing

        # STEP 1: READING DATA (Data Source)
        logger.info(">>> STEP 1: Reading Data <<<")
        df_raw = get_raw_data(path=path, schema=schema)
             
        
        # STEP 2: DATA VALIDATION AND CLEANING (Change Schema)
        logger.info(">>> STEP 2: Data Validation and Cleaning <<<")
        df_clean = clean_data(df_raw)

    
        # STEP 3: SAVING PROCESSED DATA (Data Target)
        logger.info(">>> STEP 3: Saving processed data <<<")
        save_to_s3(df_clean)
        
        """ 
        print(f"Dados salvos com sucesso em: s3://{target_bucket}/{target_key}")
        
        # ETAPA 9: CRIANDO TABELA NO DATA CATALOG (opcional)
        print(">>> ETAPA 9: Registrando no Data Catalog <<<")

        # Convertendo DataFrame para DynamicFrame para integração com Catalog
        dynamic_frame_final = DynamicFrame.fromDF(df_final, glueContext, "df_final")
        
        # Escrevendo no Data Catalog
        glueContext.write_dynamic_frame.from_catalog(
            frame=dynamic_frame_final,
            database="database_acoes_b3",
            table_name="acoes_agregado_por_setor",
            transformation_ctx="write_to_catalog"
        ) 
        """

        logger.info("Job completed successfully!")

    except Exception as e:
        logger.error(f"Error during job execution: {str(e)}")
        logger.error("Stack trace:", exc_info=True)
        raise e

    finally:
        logger.info("End of job execution.")
        spark.stop()  

if __name__ == "__main__":
    main()