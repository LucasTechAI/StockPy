from pyspark.sql.functions import col, to_date, current_timestamp, current_date, datediff, year, month, count, sum, avg, max, min, first, round, date_format
from pyspark.sql.types import StringType, StructType, StructField, DoubleType, LongType
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
    .getOrCreate()


def get_raw_data(bucket: str = "bucket-dados-b3", key: str = "raw-data/acoes-b3.csv", schema: StructType = None, path: str = "/home/lucas/StockPy/data/raw/stocks.csv") -> DataFrame:
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
    return dataframe


def clean_data(df: DataFrame) -> DataFrame:
    """
    Clean the raw data by applying necessary transformations.
    Args:
        df (DataFrame): Raw data DataFrame.
    Returns:
        DataFrame: Cleaned data DataFrame.
    """
    dataframe = df \
            .withColumn("Date", to_date(col("Date"), "yyyy-MM-dd")) \
            .withColumn("Close", col("Close").cast(DoubleType())) \
            .withColumn("High", col("High").cast(DoubleType())) \
            .withColumn("Low", col("Low").cast(DoubleType())) \
            .withColumn("Open", col("Open").cast(DoubleType())) \
            .withColumn("Volume", col("Volume").cast(LongType())) \
            .filter(col("Sector").isNotNull()) \
            .filter(col("Close") > 0) \
            .filter(col("Volume") > 0)
    logger.info(f"Count data after cleaning: {dataframe.count()}")
    return dataframe


def rename_columns(df: DataFrame) -> DataFrame:
    """
    Rename columns of the DataFrame to Portuguese.
    Args:
        df (DataFrame): DataFrame to rename columns.
    Returns:
        DataFrame: DataFrame with renamed columns.
    """
    dataframe = df \
        .withColumnRenamed("Sector", "Setor") \
        .withColumnRenamed("Close", "PrecoFechamento") \
        .withColumnRenamed("Volume", "VolumeNegociacao") \
        .withColumnRenamed("Date", "Data") \
        .withColumnRenamed("High", "PrecoMaximo") \
        .withColumnRenamed("Low", "PrecoMinimo") \
        .withColumnRenamed("Open", "PrecoAbertura") \
        .withColumnRenamed("Ticker", "CodigoAcao") \
        .withColumnRenamed("Company", "NomeEmpresa")
    logger.info(f"Columns renamed: {dataframe.columns}")
    return dataframe

def add_date_fields(df: DataFrame) -> DataFrame:
    """
    Add date-related fields to the DataFrame.
    Args:
        df (DataFrame): DataFrame to add date fields.
    Returns:
        DataFrame: DataFrame with added date fields.
    """
    dataframe = df \
            .withColumn("DataProcessamento", current_timestamp()) \
            .withColumn("DiasDesdeReferencia", 
                    datediff(current_date(), col("Data"))) \
            .withColumn("AnoReferencia", year(col("Data"))) \
            .withColumn("MesReferencia", month(col("Data"))) \
            .withColumn("DataProcessamentoStr", 
                    date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
    logger.info("Date fields added.")
    return dataframe

def aggregate_by_sector(df: DataFrame) -> DataFrame:
    """
    Aggregate data by sector with various metrics.
    Args:
        df (DataFrame): DataFrame to aggregate.
    Returns:
        DataFrame: Aggregated DataFrame by sector.
    """
    dataframe = df \
            .groupBy("Setor") \
            .agg(
                count("*").alias("TotalEmpresas"),
                sum("VolumeNegociacao").alias("TotalVolume"),
                avg("PrecoFechamento").alias("PrecoMedio"),
                avg("PrecoMaximo").alias("PrecoMaximoMedio"),
                avg("PrecoMinimo").alias("PrecoMinimoMedio"),
                avg("PrecoAbertura").alias("PrecoAberturaMedio"),
                max("PrecoFechamento").alias("MaiorPreco"),
                min("PrecoFechamento").alias("MenorPreco"),
                sum(col("PrecoFechamento") * col("VolumeNegociacao") / 1000000).alias("ValorMercadoMilhoes"),
                first("DataProcessamento").alias("DataProcessamento"),
                first("DiasDesdeReferencia").alias("DiasDesdeReferencia"),
                first("AnoReferencia").alias("AnoReferencia"),
                first("MesReferencia").alias("MesReferencia")
            ) \
            .withColumn("PrecoMedio", round(col("PrecoMedio"), 2)) \
            .withColumn("PrecoMaximoMedio", round(col("PrecoMaximoMedio"), 2)) \
            .withColumn("PrecoMinimoMedio", round(col("PrecoMinimoMedio"), 2)) \
            .withColumn("PrecoAberturaMedio", round(col("PrecoAberturaMedio"), 2)) \
            .withColumn("ValorMercadoMilhoes", round(col("ValorMercadoMilhoes"), 2)) \
            .orderBy("TotalVolume", ascending=False)
    logger.info("Data aggregated by sector.")
    return dataframe

def add_calculated_metrics(df: DataFrame) -> DataFrame:
    """
    Add calculated metrics to the aggregated DataFrame.
    Args:
        df (DataFrame): Aggregated DataFrame.
    Returns:
        DataFrame: DataFrame with added calculated metrics.
    """
    volume_all_sectors = df.agg(sum("TotalVolume")).collect()[0][0]
    dataframe = df \
            .withColumn("PercentualVolume", 
                    round((col("TotalVolume") / volume_all_sectors) * 100, 2)) \
            .withColumn("VolumeMediaPorEmpresa", 
                    round(col("TotalVolume") / col("TotalEmpresas"), 0)) \
            .withColumn("RangePreco", 
                    round(col("MaiorPreco") - col("MenorPreco"), 2)) \
            .withColumn("VolatilityIndex", 
                    round(col("RangePreco") / col("PrecoMedio") * 100, 2))
    logger.info("Calculated metrics added.")
    return dataframe

def show_results(df: DataFrame) -> None:
    """
    Show the results of the final DataFrame.
    Args:
        df (DataFrame): Final DataFrame to show results.
    """
    logger.info(f"Total rows: {df.count()}")
    logger.info(f"Sample data:")
    logger.info(f"Schema:")
    df.printSchema()
    json_data = df.toJSON().collect()
    for record in json_data:
        logger.info(f"Record: {record}")

def save_to_s3(df: DataFrame) -> None:
    """
    Save the final DataFrame to S3 in Parquet format.
    Args:
        df (DataFrame): Final DataFrame to save.
    """
    output_path = f"data/refined/{datetime.now().strftime('%Y%m%d')}/stocks_parquet"
    df.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .partitionBy("Setor") \
        .parquet(output_path)
    logger.info(f"Data saved successfully to: {output_path}")

def main() -> None:
    try:
        logger.info("Starting job execution [TRANSFORMATIONS-STOCKS]...")
        schema = StructType([
            StructField("Sector", StringType(), True),
            StructField("Ticker", StringType(), True),
            StructField("Company", StringType(), True),
            StructField("Date", StringType(), True),  
            StructField("Close", StringType(), True), 
            StructField("High", StringType(), True),  
            StructField("Low", StringType(), True),   
            StructField("Open", StringType(), True),  
            StructField("Volume", StringType(), True) 
        ])
        path = "data/raw/stocks.csv"# temporary path for local testing

        # STEP 1: READING DATA (Data Source)
        logger.info(">>> STEP 1: Reading Data <<<")
        df_raw = get_raw_data(path=path, schema=schema)
             
        
        # STEP 2: DATA VALIDATION AND CLEANING (Change Schema)
        logger.info(">>> STEP 2: Data Validation and Cleaning <<<")
        df_clean = clean_data(df_raw)
        

        # STEP 3: RENAMING COLUMNS (Transformation B)
        logger.info(">>> STEP 3: Renaming columns [Transformation B] <<<")
        df_renamed = rename_columns(df_clean)


        # STEP 4: ADDING DATE FIELDS (Transformation C)
        logger.info(">>> STEP 4: Adding date fields [Transformation C] <<<")
        df_with_dates = add_date_fields(df_renamed)
        

        # STEP 5: GROUPING AND AGGREGATIONS (Transformation A)
        logger.info(">>> STEP 5: Grouping and aggregations [Transformation A] <<<")
        df_aggregated = aggregate_by_sector(df_with_dates)
        
        
        # STEP 6: ADDING CALCULATED METRICS
        logger.info(">>> STEP 6: Adding calculated metrics <<<")
        df_final = add_calculated_metrics(df_aggregated)


        # STEP 7: VALIDATING RESULTS
        logger.info(">>> STEP 7: Validating results <<<")
        show_results(df_final)

        # STEP 8: SAVING PROCESSED DATA (Data Target)
        logger.info(">>> STEP 8: Saving processed data to S3 <<<")
        save_to_s3(df_final)
        
        
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