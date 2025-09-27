import sys
import traceback
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.types import StructType, StructField, StringType
from logging import getLogger, basicConfig, INFO
from boto3 import client as boto3_client
from pyspark.sql.functions import lit
from urllib.parse import quote
from bs4 import BeautifulSoup
from datetime import datetime
from requests import Session
from time import sleep

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

RAW_DATA_PATH = "s3://stockpy/raw/news/"
DB_NAME = "news_db"
TABLE_NAME = "news_raw"

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

basicConfig(level=INFO, format="%(asctime)s - %(levelname)s - %(funcName)s - %(message)s")
logger = getLogger(__name__)

class GlueGoogleNewsExtractor:
    """
    Extracts news articles from Google News for AWS Glue jobs.
    """
    def __init__(self, stocks_dict: dict) -> None:
        """
        Initializes the extractor with stock configurations and HTTP session.
        Args:
            stocks_dict (dict): Dictionary with stock tickers and company names.
        """
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        self.stocks = stocks_dict
        self.session = Session()
        self.session.headers.update(self.headers)
        self.s3_client = boto3_client('s3')


    def __get_title(self, article: BeautifulSoup) -> str:
        """
        Extracts the title from a news article element.
        Args:
            article (bs4.element.Tag): BeautifulSoup Tag object representing the article.
        Returns:
            str: The extracted title text.
        """
        title_elem = (article.find('h3') or
                      article.find('h4') or
                      article.find('a', attrs={'data-n-tid': True}))
        return title_elem.get_text(strip=True) if title_elem else ""


    def __get_link(self, article: BeautifulSoup) -> str:
        """
        Extracts the link from a news article element.
        Args:
            article (bs4.element.Tag): BeautifulSoup Tag object representing the article.
        Returns:
            str: The extracted link URL.
        """
        link_elem = article.find('a')
        link = ''
        if link_elem and link_elem.get('href'):
            link = link_elem.get('href')
            if link.startswith('./'):
                link = f"https://news.google.com{link[1:]}"
            elif not link.startswith('http'):
                link = f"https://news.google.com{link}"
        return link


    def __get_source(self, article: BeautifulSoup) -> str:
        """
        Extracts the source from a news article element.
        Args:
            article (bs4.element.Tag): BeautifulSoup Tag object representing the article.
        Returns:
            str: The extracted source text.
        """
        source_elem = (article.find('div', attrs={'data-n-tid': True}) or
                       article.find('span', attrs={'data-n-tid': True}) or
                       article.find(attrs={'data-n-tid': True}))
        return source_elem.get_text(strip=True) if source_elem else 'Google News'


    def __get_date_published(self, article: BeautifulSoup) -> str:
        """
        Extracts the publication date from a news article element.
        Args:
            article (bs4.element.Tag): BeautifulSoup Tag object representing the article.
        Returns:
            str: The extracted publication date as a string.
        """
        time_elem = article.find('time')
        if time_elem:
            return time_elem.get('datetime') or time_elem.get_text(strip=True) or ""
        return ""


    def __get_sector(self, ticker: str) -> str:
        """
        Extracts the sector from a stock ticker.
        Args:
            ticker (str): The stock ticker symbol.
        Returns:
            str: The sector associated with the stock ticker.
        """
        for sector, companies in self.stocks.items():
            if ticker in companies:
                return sector
        return "Unknown"
    
    
    def __sanitize_text(self, text: str) -> str:
        """
        Sanitizes text to ensure UTF-8 encoding and removes unwanted characters.
        Args:
            text (str): The text to sanitize.
        Returns:
            str: The sanitized text.
        """
        if not text:
            return ""
        try:
            return text.encode("utf-8", "ignore").decode("utf-8").strip()
        except Exception:
            return str(text).strip()
    
    
    def __process_news(self, search_term: str, company_name: str, ticker: str) -> list:
        """
        Processes news articles for a given search term.
        Args:
            search_term (str): The search term to query Google News.
            company_name (str): The name of the company.
            ticker (str): The stock ticker symbol.
        Returns:
            list: A list of dictionaries containing news article details.
        """
        news = []
        try:
            encoded_query = quote(search_term)
            url = f"https://news.google.com/search?q={encoded_query}&hl=pt-BR&gl=BR&ceid=BR:pt-419"
            logger.info(f"Searching: {search_term} -> {url}")
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            articles = soup.find_all('article') or soup.find_all('div', attrs={'data-n-tid': True}) or soup.find_all('div', class_='xrnccd')

            logger.info(f"Found {len(articles)} articles for '{search_term}'")

            for article in articles:
                try:
                    title = self.__get_title(article)
                    link = self.__get_link(article)
                    source = self.__get_source(article)
                    published_time = self.__get_date_published(article)
                    sector = self.__get_sector(ticker)
                    extracted_at = datetime.utcnow().isoformat()

                    news_item = {
                        'ticker': self.__sanitize_text(ticker),
                        'company': self.__sanitize_text(company_name),
                        'sector': self.__sanitize_text(sector),
                        'title': self.__sanitize_text(title),
                        'source': self.__sanitize_text(source),
                        'link': self.__sanitize_text(link),
                        'published_time': self.__sanitize_text(published_time),
                        'search_term': self.__sanitize_text(search_term),
                        'extracted_at': self.__sanitize_text(extracted_at)
                    }
                    

                    news.append(news_item)
                except Exception as ex_item:
                    logger.debug(f"Error processing individual article: {ex_item}")
                    continue

        except Exception as e:
            logger.warning(f"Error fetching news for '{search_term}': {e}")
        return news


    def _extract_google_news(self, company_name: str, ticker: str) -> list:
        """
        Extracts news articles from Google News for a given company.
        Args:
            company_name (str): The name of the company.
            ticker (str): The stock ticker symbol.
        Returns:
            list: A list of dictionaries containing news article details.
        """
        try:
            search_terms = [
                f"Empresa {company_name}",
                ticker.replace('.SA', ''),
                f"{company_name} resultados",
                f"{company_name} prejuízo"
            ]
            news = []
            for search_term in search_terms:
                try:
                    results = self.__process_news(search_term, company_name, ticker)
                    if results:
                        news.extend(list(results))
                    sleep(2)
                except Exception as e:
                    logger.warning(f"Error fetching news for '{search_term}': {e}")
                    continue
            return news
        except Exception as e:
            logger.error(f"General error extracting news for {company_name}: {e}")
            return []


    def extract_and_save_to_s3(self) -> bool:
        """
        Extracts news articles and saves them to S3 in Parquet format.
        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            logger.info("Initiating Google News extraction...")
            all_news = []

            total_companies = sum(len(companies) for companies in self.stocks.values())
            current_company = 0
            for sector, companies in self.stocks.items():
                logger.info(f"Processing sector: {sector}")
                for ticker, company_name in companies.items():
                    current_company += 1
                    logger.info(f"[{current_company}/{total_companies}] {company_name} ({ticker})")
                    company_news = self._extract_google_news(company_name, ticker)
                    if company_news:
                        all_news.extend(list(company_news))
                    sleep(3)

            if not all_news:
                logger.warning("No news articles extracted")
                return False

            logger.info(f"Total news articles collected: {len(all_news)}")

            news_schema = StructType([
                StructField("ticker", StringType(), True),
                StructField("company", StringType(), True),
                StructField("sector", StringType(), True),
                StructField("title", StringType(), True),
                StructField("source", StringType(), True),
                StructField("link", StringType(), True),
                StructField("published_time", StringType(), True),
                StructField("search_term", StringType(), True),
                StructField("extracted_at", StringType(), True)
            ])

            news_data = list(all_news)
            df = spark.createDataFrame(news_data, schema=news_schema)

            # Current date for partition
            now = datetime.utcnow()
            dataproc = now.strftime("%Y%m%d")
            df = df.withColumn("dataproc", lit(dataproc).cast(StringType()))

            output_path = f"{RAW_DATA_PATH}dataproc={dataproc}/"

            logger.info(f"Saving DataFrame to S3: {output_path}")
            df.write \
              .mode("append") \
              .option("compression", "snappy") \
              .partitionBy("dataproc") \
              .parquet(output_path)

            logger.info(f"Successfully saved {len(news_data)} news articles to {output_path}")

            self._create_glue_catalog_table()
            self._repair_table_partitions()
            logger.info(f"Data written to Glue Catalog: {DB_NAME}.{TABLE_NAME}")
            return True

        except Exception as e:
            logger.error(f"Error in extract_and_save_to_s3: {e}")
            logger.error(traceback.format_exc())
            return False


    def _create_glue_catalog_table(self) -> None:
        """
        Creates or updates the Glue Catalog table for the news articles.
        """
        try:
            logger.info(f"Creating/updating Glue Catalog table {DB_NAME}.{TABLE_NAME}")
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
            try:
                spark.sql(f"DROP TABLE IF EXISTS {DB_NAME}.{TABLE_NAME}")
            except Exception as e:
                logger.warning(f"Could not drop table (might not exist): {e}")

            create_table_sql = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {DB_NAME}.{TABLE_NAME} (
                ticker STRING,
                company STRING,
                sector STRING,
                title STRING,
                source STRING,
                link STRING,
                published_time STRING,
                search_term STRING,
                extracted_at STRING
            )
            PARTITIONED BY (
                dataproc STRING
            )
            STORED AS PARQUET
            LOCATION '{RAW_DATA_PATH}'
            TBLPROPERTIES (
                'has_encrypted_data'='false',
                'parquet.compression'='SNAPPY'
            )
            """
            spark.sql(create_table_sql)
            logger.info(f"Table {DB_NAME}.{TABLE_NAME} created successfully")
        except Exception as e:
            logger.error(f"Error creating Glue Catalog table: {e}")
            logger.error(traceback.format_exc())
            raise


    def _repair_table_partitions(self) -> None:
        """
        Repairs the table partitions in the Glue Catalog.
        """
        try:
            logger.info(f"Repairing table partitions for {DB_NAME}.{TABLE_NAME}")
            spark.sql(f"MSCK REPAIR TABLE {DB_NAME}.{TABLE_NAME}")
        except Exception as e:
            logger.error(f"Error executing MSCK REPAIR TABLE: {e}")
            logger.error(traceback.format_exc())
    


def main():
    try:
        logger.info("Starting Google News extraction Glue job...")
        extractor = GlueGoogleNewsExtractor(STOCKS)
        success = extractor.extract_and_save_to_s3()
        if success:
            logger.info("News extraction job completed successfully")
        else:
            logger.error("News extraction job failed")
            raise Exception("News extraction failed")
    except Exception as e:
        logger.error(f"Job failed with error: {e}")
        logger.error(traceback.format_exc())
        raise
    finally:
        job.commit()

if __name__ == "__main__":
    main()
