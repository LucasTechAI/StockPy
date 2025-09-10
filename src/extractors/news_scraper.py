from logging import getLogger, basicConfig, INFO
from urllib.parse import quote
from bs4 import BeautifulSoup
from datetime import datetime
from requests import Session
from pandas import DataFrame
from time import sleep


basicConfig(level=INFO, format="%(asctime)s - %(levelname)s - %(funcName)s - %(message)s")
logger = getLogger(__name__)

class GoogleNewsExtractor:
    """
    Extracts news articles from Google News.
    """
    def __init__(self, stocks: dict) -> None:
        """
        Initializes the GoogleNewsExtractor class.

        Sets up the stock dictionary with sectors and companies, configures HTTP headers for requests,
        and creates a persistent requests session with custom headers for scraping Google News.

        Args:
            stocks (dict): Dictionary mapping sectors to company tickers and names.

        Attributes:
            stocks (dict): Dictionary mapping sectors to company tickers and names.
            headers (dict): HTTP headers for web requests to mimic a browser.
            session (Session): Persistent session for making HTTP requests.
        """
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        self.stocks = stocks
        self.session = Session()
        self.session.headers.update(self.headers)


    def __get_title(self, article) -> str:
        """
        Extracts the title from a news article element.
        Args:
            article: BeautifulSoup element representing a news article.
        Returns:
            str: The extracted title text or an empty string if not found.
        """
        title_elem = (article.find('h3') or 
                      article.find('h4') or 
                      article.find('a', {'data-n-tid': True}))
        if title_elem:
            return title_elem.get_text(strip=True)
        return ""


    def __get_link(self, article) -> str:
        """
        Extracts the link from a news article element.
        Args:
            article: BeautifulSoup element representing a news article.
        Returns:
            str: The extracted link URL or an empty string if not found.
        """
        link_elem = article.find('a')
        link = ''
        if link_elem and link_elem.get('href'):
            link = link_elem.get('href')
            print(link)
            if link.startswith('./'):
                link = f"https://news.google.com{link[1:]}"
            elif not link.startswith('http'):
                link = f"https://news.google.com{link}"
        return link


    def __get_source(self, article) -> str:
        """
        Extracts the source from a news article element.
        Args:
            article: BeautifulSoup element representing a news article.
        Returns:
            str: The extracted source text or 'Google News' if not found.
        """
        source_elem = (article.find('div', {'data-n-tid': True}) or
                       article.find('span', {'data-n-tid': True}) or
                       article.find('[data-n-tid]'))
        return source_elem.get_text(strip=True) if source_elem else 'Google News'


    def __get_date_published(self, article) -> str:
        """
        Extracts the publication date from a news article element.
        Args:
            article: BeautifulSoup element representing a news article.
        Returns:
            str: The extracted publication date in ISO format or an empty string if not found.
        """
        time_elem = article.find('time')
        if time_elem:
            pub_time = time_elem.get('datetime') or time_elem.get_text(strip=True)
            return pub_time
        return ""
       

    def __process_news(self, search_term: str, company_name: str, ticker: str) -> list:
        """
        Private method to fetch and parse news articles from Google News for a given search term.
        
        Args:
            search_term (str): The term to search for.
            company_name (str): The company name.
            ticker (str): The company ticker.
        
        Returns:
            list: List of news items (dict).
        """
        news = []
        try:
            encoded_query = quote(search_term)
            
            url = f"https://news.google.com/search?q={encoded_query}&hl=pt-BR&gl=BR&ceid=BR:pt-419"

            logger.info(f"Searching: {search_term}")

            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            articles = (soup.find_all('article') or 
                      soup.find_all('div', {'data-n-tid': True}) or
                      soup.find_all('div', class_='xrnccd'))

            logger.info(f"Found {len(articles)} articles for '{search_term}'")

            for article in articles:
                try:
                    title = self.__get_title(article)
                    link = self.__get_link(article)
                    source = self.__get_source(article)
                    published_time = self.__get_date_published(article)
                    sector = self.__get_sector(ticker)
                    extracted_at = datetime.now().isoformat()

                    new = {
                        'ticker': ticker,
                        'company': company_name,
                        'sector': sector,
                        'title': title,
                        'source': source,
                        'link': link,
                        'published_time': published_time,
                        'search_term': search_term,
                        'extracted_at': extracted_at
                    }

                    news.append(new)

                except Exception as e:
                    logger.debug(f"Error processing individual article: {e}")
                    continue
        except Exception as e:
            logger.warning(f"Error fetching news for '{search_term}': {e}")

        return news


    def __get_sector(self, ticker: str) -> str:
        """
        Return the sector of the company by its ticker
        Args:
            ticker (str): The company ticker.
        Returns:
            str: The sector name or "Unknown" if not found.
        """
        for sector, companies in self.stocks.items():
            if ticker in companies:
                return sector
        return "Unknown"
    

    def _extract_google_news(self, company_name: str, ticker: str) -> list:
        """
        Extracts news articles for a specific company using various search terms.
        Args:
            company_name (str): The name of the company.
            ticker (str): The ticker symbol of the company.
        Returns:
            list: List of news items (dict).
        """
        try:
            search_terms = {
                f"Empresa {company_name}",
                ticker.replace('.SA', ''),
                f"{company_name} resultados",
                f"{company_name} prejuízo"
            }
            
            news = []

            for search_term in search_terms:
                try:
                    __args = {
                        "search_term": search_term,
                        "company_name": company_name,
                        "ticker": ticker
                    }
                    results = self.__process_news(**__args)
                    news.extend(results)
                    sleep(2)
                except Exception as e:
                    logger.warning(f"Error fetching news for '{search_term}': {e}")
                    continue
            return news
            
        except Exception as e:
            logger.error(f"General error extracting news for {company_name}: {e}")
            return


    def extract_news_data(self) -> DataFrame:
        """        
        Extracts news for all companies in the stocks dictionary.
        Returns:
            DataFrame: DataFrame containing all extracted news articles."""
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
                    all_news.extend(company_news)
                    sleep(3)
            news = DataFrame(all_news)
            return news
        except Exception as e:
            logger.error(f"Error extracting news for all companies: {e}")
            return DataFrame()
