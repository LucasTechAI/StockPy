import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, to_date, lit, avg, sum, lag
)
from pyspark.sql.types import StringType, StructType, StructField, DoubleType, LongType
from pyspark.sql.window import Window
from logging import getLogger, basicConfig, INFO
from datetime import datetime

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# ---------------------
# Setup Glue & Spark
# ---------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

basicConfig(level=INFO, format="%(asctime)s - %(levelname)s - %(funcName)s - %(message)s")
logger = getLogger(__name__)

# ---------------------
# Constantes
# ---------------------
DATABASE = "stock_db"
TABLE = "stocks_clean"
PROCESS_DATE = datetime.now().strftime("%Y%m%d")

RAW_PATH = f"s3://stockpy/raw/stocks/dataproc={PROCESS_DATE}/"
OUTPUT_PATH = "s3://stockpy/refined/stocks/"

# ---------------------
# Funções auxiliares
# ---------------------

def table_exists(database: str, table: str) -> bool:
    try:
        spark.sql(f"DESCRIBE TABLE {database}.{table}")
        logger.info(f"Tabela encontrada: {database}.{table}")
        print(f"[DEBUG] Tabela encontrada: {database}.{table}")
        return True
    except Exception:
        logger.warning(f"Tabela NÃO encontrada: {database}.{table}")
        print(f"[DEBUG] Tabela NÃO encontrada: {database}.{table}")
        return False

def create_table_if_not_exists(database: str, table: str, location: str) -> None:
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
        logger.info(f"Tabela {database}.{table} criada com sucesso!")
        print(f"[DEBUG] Tabela {database}.{table} criada com sucesso!")

def repair_table_partitions(database: str, table: str):
    try:
        spark.sql(f"MSCK REPAIR TABLE {database}.{table}")
        logger.info(f"MSCK REPAIR TABLE executado com sucesso para {database}.{table}")
        partitions_df = spark.sql(f"SHOW PARTITIONS {database}.{table}")
        total_part = partitions_df.count()
        logger.info(f"Total de partições descobertas: {total_part}")
        print(f"[DEBUG] Total de partições descobertas: {total_part}")
    except Exception as e:
        logger.error(f"Falha no REPAIR TABLE: {e}")
        print(f"[ERRO] Falha no REPAIR TABLE: {e}")

def get_raw_data(path: str, schema: StructType) -> DataFrame:
    print(f"[DEBUG] Lendo dados brutos de: {path}")
    df = spark.read.parquet(path)
    count = df.count()
    print(f"[DEBUG] Registros lidos: {count}")
    logger.info(f"Registros lidos: {count}")
    df.printSchema()
    return df

def clean_data(df: DataFrame) -> DataFrame:
    print("[DEBUG] Iniciando limpeza de dados")
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
    print(f"[DEBUG] Registros após limpeza: {df_clean.count()}")
    return df_clean

def rename_columns(df: DataFrame) -> DataFrame:
    print("[DEBUG] Renomeando colunas")
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
    print(f"[DEBUG] Registros após renomear colunas: {df.count()}")
    return df

def calculate_aggregations(df: DataFrame) -> DataFrame:
    print("[DEBUG] Calculando agregações")
    df_agg = df.groupBy("setor").agg(
        avg("precoFechamento").alias("mediaFechamento"),
        sum("volumeNegociacao").alias("totalVolume")
    )
    print(f"[DEBUG] Linhas agregadas: {df_agg.count()}")
    return df.join(df_agg, on="setor", how="left")

def calculate_date_diff(df: DataFrame) -> DataFrame:
    print("[DEBUG] Calculando variação de fechamento")
    windowSpec = Window.partitionBy("codigoAcao").orderBy("data")
    df = df.withColumn("fechamentoAnterior", lag("precoFechamento").over(windowSpec))
    df = df.withColumn("variacaoFechamento", col("precoFechamento") - col("fechamentoAnterior"))
    df = df.drop("fechamentoAnterior")
    print(f"[DEBUG] Registros após cálculo de variação: {df.count()}")
    return df

def save_to_s3(df: DataFrame, output_path: str) -> None:
    print("[DEBUG] Salvando DataFrame no S3")
    df.show(10, truncate=False)
    df.write \
        .mode("append") \
        .option("compression", "snappy") \
        .partitionBy("dataproc", "setor") \
        .parquet(output_path)
    print(f"[DEBUG] Dados salvos em {output_path}")
    logger.info(f"Dados salvos em: {output_path}")

# ---------------------
# Main
# ---------------------
def main():
    try:
        logger.info(">>> Iniciando processamento STOCKS <<<")
        print(">>> Iniciando processamento STOCKS <<<")

        schema = StructType([
            StructField("sector", StringType(), True),
            StructField("ticker", StringType(), True),
            StructField("company", StringType(), True),
            StructField("date", StringType(), True),
            StructField("close", StringType(), True),
            StructField("high", StringType(), True),
            StructField("low", StringType(), True),
            StructField("open", StringType(), True),
            StructField("volume", StringType(), True),
        ])

        # Ler dados crus
        df_raw = get_raw_data(RAW_PATH, schema)
        print("[DEBUG] Dados crus carregados")
        df_raw.show()

        # Limpar dados
        df_clean = clean_data(df_raw)
        print("[DEBUG] Dados limpos")
        df_clean.printSchema()
        df_clean.show(10, truncate=False)

        # Renomear colunas e adicionar dataproc
        df_final = rename_columns(df_clean).withColumn("dataproc", lit(PROCESS_DATE))
        print("[DEBUG] Colunas renomeadas e coluna dataproc adicionada")
        df_final.printSchema()
        df_final.show(10, truncate=False)

        # Agregações obrigatórias
        df_final = calculate_aggregations(df_final)
        print("[DEBUG] Agregações concluídas")
        df_final.show(10, truncate=False)
        df_final.printSchema()

        # Cálculos baseados em datas
        df_final = calculate_date_diff(df_final)
        print("[DEBUG] Diferença de datas calculada")
        df_final.show(10, truncate=False)
        df_final.printSchema()

        # Criar tabela Glue caso não exista
        create_table_if_not_exists(DATABASE, TABLE, OUTPUT_PATH)
        print("[DEBUG] Tabela verificada/criada")

        # Salvar em Parquet particionado por SETOR
        save_to_s3(df_final, OUTPUT_PATH)

        # Atualizar partições no Glue Catalog
        repair_table_partitions(DATABASE, TABLE)
        print("[DEBUG] Partições atualizadas")

        logger.info(">>> Processamento concluído com sucesso <<<")
        print(">>> Processamento concluído com sucesso <<<")

    except Exception as e:
        logger.error(f"Erro na execução: {e}", exc_info=True)
        print(f"[ERRO] {e}")
        raise

    finally:
        print("[DEBUG] Encerrando Spark e Job")
        job.commit()
        spark.stop()

if __name__ == "__main__":
    main()
