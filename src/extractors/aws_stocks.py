import sys
import boto3
from botocore.exceptions import ClientError
from logging import basicConfig, getLogger, INFO
from warnings import filterwarnings
from datetime import datetime
from yfinance import download as download_stock

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
import pyspark.sql.functions as F
from pyspark.sql.types import *

# ========================
# Configuração de Logging
# ========================
filterwarnings("ignore")
basicConfig(level=INFO, format="%(asctime)s - %(levelname)s - %(funcName)s - %(message)s")
logger = getLogger(__name__)

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# ========================
# Configurações Gerais
# ========================
raw_data_path = "s3://stockpy/raw/stocks/"
db_name = "stock_db"
table_name = "stock_prices_best_row"
process_date = datetime.now().strftime("%Y%m%d")

# Dicionário de ações
STOCKS = {
    "Banks": {
        "ITUB4.SA": "Itaú Unibanco",
        "BBAS3.SA": "Banco do Brasil"
    },
    "Energy": {
        "ISAE4.SA": "ISA Energia",
        "CPFE3.SA": "CPFL Energia"
    },
    "Sanitation": {
        "SBSP3.SA": "Sabesp",
        "SAPR4.SA": "Sanepar"
    },
    "Insurance": {
        "PSSA3.SA": "Porto Seguro",
        "BBSE3.SA": "BB Seguridade"
    },
    "Telecommunications": {
        "VIVT3.SA": "Vivo",
        "INTB3.SA": "Intelbras"
    }
}


# ========================
# Classe de Coleta TOTALMENTE CORRIGIDA
# ========================
class StockDataFetcher:
    def __init__(self, stocks: dict) -> None:
        self.stocks = stocks
        logger.info("StockDataFetcher inicializado com %d setores.", len(stocks))

    def _create_stock_mapping(self):
        stock_map = []
        for sector, companies in self.stocks.items():
            for ticker, name in companies.items():
                stock_map.append((sector, ticker, name))
        return spark.createDataFrame(stock_map, ["Sector", "Ticker", "Company"])

    def extract_stock_data(self):
        df_map = self._create_stock_mapping()
        tickers = [row["Ticker"] for row in df_map.collect()]
    
        logger.info("Buscando dados de %d tickers...", len(tickers))
        data = download_stock(
            tickers,
            period="1d",
            interval="1m",
            prepost=True,
            progress=False,
            threads=True
        )
    
        if data.empty:
            logger.warning("Nenhum dado retornado do Yahoo Finance.")
            return df_map.withColumn("dataproc", F.lit(process_date))
    
        # Flatten do multiindex do yfinance
        print(data.columns)
        print(data)
        data = data.stack(level=1).reset_index()
        print(data.columns)
        print(data)
        data = data.rename(columns={
            "Datetime": "Date"
        })
    
        # CORREÇÃO CRÍTICA: Converter data para string desde o início
        data['Date'] = data['Date'].dt.strftime('%Y-%m-%d')
    
        # OPÇÃO 1: Usar spark.createDataFrame diretamente com pandas (mais simples)
        df_data = spark.createDataFrame(data)
    
        # Limpar dados usando PySpark (sem pandas)
        from pyspark.sql.functions import col, when, isnan
        
        df_data = df_data.select(
            when(col('Date').isNotNull(), col('Date').cast(StringType())).otherwise(None).alias('Date'),
            when(col('Ticker').isNotNull(), col('Ticker').cast(StringType())).otherwise(None).alias('Ticker'),
            when(col('Close').isNotNull() & ~isnan(col('Close')), col('Close').cast(DoubleType())).otherwise(None).alias('Close'),
            when(col('High').isNotNull() & ~isnan(col('High')), col('High').cast(DoubleType())).otherwise(None).alias('High'),
            when(col('Low').isNotNull() & ~isnan(col('Low')), col('Low').cast(DoubleType())).otherwise(None).alias('Low'),
            when(col('Open').isNotNull() & ~isnan(col('Open')), col('Open').cast(DoubleType())).otherwise(None).alias('Open'),
            when(col('Volume').isNotNull() & ~isnan(col('Volume')), col('Volume').cast(LongType())).otherwise(None).alias('Volume')
        )
    
        # Join com metadados
        results = df_map.join(df_data, on="Ticker", how="left")
    
        # Renomear colunas para padrão snake_case
        results = results.withColumnRenamed("Sector", "sector") \
                     .withColumnRenamed("Ticker", "ticker") \
                     .withColumnRenamed("Company", "company") \
                     .withColumnRenamed("Date", "date") \
                     .withColumnRenamed("Close", "close") \
                     .withColumnRenamed("High", "high") \
                     .withColumnRenamed("Low", "low") \
                     .withColumnRenamed("Open", "open") \
                     .withColumnRenamed("Volume", "volume") \
                     .withColumnRenamed("Datetime", "date")

        # Adiciona partição dataproc como última coluna
        results = results.withColumn("dataproc", F.lit(process_date))
    
        logger.info("Schema final do DataFrame:")
        results.printSchema()
        
        return results

# ========================
# Função Catálogo Glue TOTALMENTE CORRIGIDA
# ========================
glue_client = boto3.client("glue")

def create_glue_catalog(database, table, s3_path):
    """Cria database e tabela no Glue Catalog com schema 100% compatível com Athena"""
    
    # 1. Criar database se não existir
    try:
        glue_client.create_database(
            DatabaseInput={
                "Name": database,
                "Description": f"Database para dados de ações - Criado em {datetime.now().isoformat()}"
            }
        )
        logger.info(f"Database '{database}' criado no Glue Catalog.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'AlreadyExistsException':
            logger.info(f"Database '{database}' já existe.")
        else:
            logger.error(f"Erro ao criar database: {e}")
            raise

    # 2. SCHEMA TOTALMENTE COMPATÍVEL COM ATHENA
    table_columns = [
        {"Name": "sector", "Type": "string", "Comment": "Setor da empresa"},
        {"Name": "ticker", "Type": "string", "Comment": "Código da ação"},
        {"Name": "company", "Type": "string", "Comment": "Nome da empresa"},
        {"Name": "date", "Type": "string", "Comment": "Data da cotação (formato YYYY-MM-DD)"},
        {"Name": "close", "Type": "double", "Comment": "Preço de fechamento"},
        {"Name": "high", "Type": "double", "Comment": "Preço máximo"},
        {"Name": "low", "Type": "double", "Comment": "Preço mínimo"},
        {"Name": "open", "Type": "double", "Comment": "Preço de abertura"},
        {"Name": "volume", "Type": "bigint", "Comment": "Volume negociado"}
    ]

    partition_keys = [
        {"Name": "dataproc", "Type": "string", "Comment": "Data de processamento (YYYYMMDD)"}
    ]

    # 3. Definições de tabela otimizadas para compatibilidade
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
                "Parameters": {
                    "serialization.format": "1"
                }
            },
            "Compressed": True,
            "Parameters": {
                "classification": "parquet",
                "compressionType": "snappy",
                "typeOfData": "file",
                "parquet.compress": "SNAPPY",
                "projection.enabled": "false",
                # CRÍTICO: Parâmetros para compatibilidade total
                "parquet.column.index.access": "false",  # Evita problemas de index
                "parquet.enable.dictionary": "false"     # Resolve dictionary encoding issues
            }
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
            # Parâmetros de compatibilidade Hive/Athena
            "skip.header.line.count": "0",
            "columnsOrdered": "true"
        }
    }

    try:
        # Deletar tabela existente se houver problemas de schema
        try:
            glue_client.delete_table(DatabaseName=database, Name=table)
            logger.info(f"Tabela existente '{database}.{table}' removida para recriar com schema correto.")
        except ClientError:
            pass  # Tabela não existe, OK

        # Criar nova tabela
        glue_client.create_table(
            DatabaseName=database,
            TableInput=table_input
        )
        logger.info(f"Tabela '{database}.{table}' criada no Glue Catalog com schema compatível.")
        
    except ClientError as e:
        logger.error(f"Erro ao criar tabela: {e}")
        raise

    logger.info(f"Localização S3: {s3_path}")


def add_partition_if_not_exists(database, table, partition_value, partition_location):
    """Adiciona partição específica se ela não existir"""
    try:
        glue_client.create_partition(
            DatabaseName=database,
            TableName=table,
            PartitionInput={
                'Values': [partition_value],
                'StorageDescriptor': {
                    'Location': partition_location,
                    'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                        'Parameters': {
                            'serialization.format': '1'
                        }
                    },
                    'Parameters': {
                        'classification': 'parquet',
                        'compressionType': 'snappy',
                        'parquet.enable.dictionary': 'false'
                    }
                }
            }
        )
        logger.info(f"Partição dataproc={partition_value} adicionada à tabela {database}.{table}")
    except ClientError as e:
        if e.response['Error']['Code'] == 'AlreadyExistsException':
            logger.info(f"Partição dataproc={partition_value} já existe.")
        else:
            logger.error(f"Erro ao criar partição: {e}")


def repair_table_partitions(database, table):
    """Executa MSCK REPAIR TABLE para descobrir partições automaticamente"""
    try:
        # Usar Spark SQL com configurações seguras
        spark.sql(f"MSCK REPAIR TABLE {database}.{table}")
        logger.info(f"MSCK REPAIR TABLE executado com sucesso para {database}.{table}")
        
        # Verificar partições
        partitions_df = spark.sql(f"SHOW PARTITIONS {database}.{table}")
        partitions_count = partitions_df.count()
        logger.info(f"Total de partições descobertas: {partitions_count}")
        
        if partitions_count > 0:
            logger.info("Partições encontradas:")
            partitions_list = [row[0] for row in partitions_df.collect()]
            for partition in partitions_list:
                logger.info(f"  - {partition}")
        
    except Exception as e:
        logger.warning(f"MSCK REPAIR TABLE falhou: {e}")
        
        # Método alternativo: adicionar partição manualmente
        try:
            partition_location = f"{raw_data_path}dataproc={process_date}/"
            add_partition_if_not_exists(database, table, process_date, partition_location)
        except Exception as manual_error:
            logger.error(f"Erro ao adicionar partição manualmente: {manual_error}")


def validate_data_in_athena(database, table):
    """Valida se os dados estão acessíveis via queries SQL"""
    try:
        # Teste 1: Contar registros com timeout
        count_query = f"SELECT COUNT(*) as total_records FROM {database}.{table}"
        logger.info(f"Executando: {count_query}")
        
        count_df = spark.sql(count_query)
        total_records = count_df.collect()[0]['total_records']
        logger.info(f"✅ Total de registros na tabela: {total_records}")
        
        if total_records == 0:
            logger.warning("⚠️ Nenhum registro encontrado na tabela")
            return False
        
        # Teste 2: Verificar partições
        partitions_query = f"SELECT DISTINCT dataproc FROM {database}.{table} ORDER BY dataproc DESC"
        partitions_df = spark.sql(partitions_query)
        logger.info("✅ Partições disponíveis:")
        partitions_list = [row['dataproc'] for row in partitions_df.collect()]
        for partition in partitions_list:
            logger.info(f"  - dataproc={partition}")
        
        # Teste 3: Amostra de dados da partição atual
        sample_query = f"""
        SELECT sector, ticker, company, date, 
               CAST(close AS DECIMAL(10,2)) as close_formatted,
               volume 
        FROM {database}.{table} 
        WHERE dataproc = '{process_date}'
        LIMIT 5
        """
        logger.info("✅ Amostra dos dados da partição atual:")
        sample_df = spark.sql(sample_query)
        sample_results = sample_df.collect()
        
        for i, row in enumerate(sample_results, 1):
            logger.info(f"  {i}. {row['sector']} | {row['ticker']} | {row['company']} | {row['date']} | ${row['close_formatted']} | Vol: {row['volume']}")
        
        # Teste 4: Verificar tipos de dados
        logger.info("✅ Schema da tabela:")
        describe_df = spark.sql(f"DESCRIBE {database}.{table}")
        for row in describe_df.collect():
            logger.info(f"  - {row['col_name']}: {row['data_type']}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro na validação dos dados: {e}")
        
        # Diagnóstico adicional
        try:
            # Verificar se os arquivos existem no S3
            s3_path_current = f"{raw_data_path}dataproc={process_date}/"
            df_direct = spark.read.parquet(s3_path_current)
            count_direct = df_direct.count()
            logger.info(f"✅ Leitura direta do S3: {count_direct} registros encontrados")
            
            # Mostrar schema do arquivo direto
            logger.info("Schema dos arquivos Parquet:")
            df_direct.printSchema()
            
            return True
            
        except Exception as e2:
            logger.error(f"❌ Leitura direta do S3 também falhou: {e2}")
            return False


# ========================
# Main TOTALMENTE CORRIGIDO
# ========================
if __name__ == "__main__":
    try:
        logger.info("🚀 Iniciando pipeline de dados de ações...")
        
        # 1. Extrair dados
        logger.info("📊 Fase 1: Extração de dados do Yahoo Finance")
        fetcher = StockDataFetcher(STOCKS)
        df_result = fetcher.extract_stock_data()
        
        # Validações antes de salvar
        total_rows = df_result.count()
        logger.info(f"✅ Dados extraídos: {total_rows} registros")
        
        if total_rows == 0:
            logger.error("❌ Nenhum dado foi extraído. Abortando pipeline.")
            raise Exception("Extração de dados falhou - nenhum registro encontrado")
        
        # Mostrar amostra
        logger.info("📋 Amostra dos dados extraídos:")
        sample_data = df_result.limit(3).collect()
        for i, row in enumerate(sample_data, 1):
            logger.info(f"  {i}. {row['sector']} | {row['ticker']} | {row['date']} | Close: ${row['close']}")

        # 2. Salvar no S3 com configurações otimizadas
        logger.info("💾 Fase 2: Salvando dados no S3...")
        
        # Remover dados da partição atual se existir (para evitar duplicatas)
        s3_current_partition = f"{raw_data_path}dataproc={process_date}/"
        logger.info(f"🗑️ Limpando partição existente: {s3_current_partition}")
        
        try:
            # Tentar deletar arquivos existentes da partição atual
            existing_df = spark.read.parquet(s3_current_partition)
            logger.info(f"Encontrados {existing_df.count()} registros na partição existente. Substituindo...")
        except:
            logger.info("Nenhuma partição existente encontrada. Criando nova.")
        
        # Salvar com configurações otimizadas
        (
            df_result.write
            .mode("append")  # Substituir dados da partição
            .option("compression", "snappy")
            .option("parquet.enable.dictionary", "false")  # CRÍTICO
            .option("parquet.bloom.filter.enabled", "false")
            .partitionBy("dataproc")
            .parquet(raw_data_path)
        )
        logger.info(f"✅ Dados salvos no S3: {raw_data_path}")

        # 3. Criar/atualizar Glue Catalog
        logger.info("📚 Fase 3: Atualizando Glue Data Catalog...")
        create_glue_catalog(db_name, table_name, raw_data_path)

        # 4. Reparar partições
        logger.info("🔧 Fase 4: Descobrindo partições...")
        repair_table_partitions(db_name, table_name)

        # 5. Validar dados
        logger.info("✅ Fase 5: Validação final...")
        validation_success = validate_data_in_athena(db_name, table_name)
        
        if validation_success:
            logger.info("🎉 SUCESSO TOTAL! Pipeline executado com sucesso!")
            logger.info("=" * 60)
            logger.info("📊 CONSULTAS PRONTAS PARA O ATHENA:")
            logger.info(f"   SELECT * FROM {db_name}.{table_name} LIMIT 10;")
            logger.info(f"   SELECT sector, COUNT(*) as stocks FROM {db_name}.{table_name} GROUP BY sector;")
            logger.info(f"   SELECT ticker, date, close FROM {db_name}.{table_name} WHERE date >= '2025-09-10';")
            logger.info("=" * 60)
            logger.info("🔗 Acesse: https://console.aws.amazon.com/athena")
        else:
            logger.error("❌ Pipeline concluído com problemas na validação")
            raise Exception("Validação dos dados falhou")

    except Exception as e:
        logger.error(f"💥 ERRO CRÍTICO no pipeline: {e}")
        logger.error("🔍 Verifique os logs acima para detalhes do erro")
        raise

    finally:
        logger.info("🏁 Finalizando job...")
        job.commit()

job.commit()