from logging import basicConfig, getLogger, INFO
from yfinance import download as download_stock
from warnings import filterwarnings
from pandas import DataFrame

filterwarnings("ignore")
basicConfig(level=INFO, format="%(asctime)s - %(levelname)s - %(funcName)s - %(message)s")
logger = getLogger(__name__)


class StockDataFetcher:
    """
    A class to fetch stock data from Yahoo Finance and return it as a pandas DataFrame.
    
    Attributes:
        stocks (dict): A dictionary mapping sectors to company tickers and names.
    """

    def __init__(self, stocks: dict) -> None:
        """
        Initialize the StockDataFetcher with a dictionary of stocks.
        
        Args:
            stocks (dict): Dictionary of sectors and their companies with tickers.
        """
        self.stocks = stocks
        logger.info("StockDataFetcher initialized with %d sectors.", len(stocks))


    def _create_stock_mapping(self) -> DataFrame:
        """
        Create a DataFrame mapping tickers to sectors and company names.
        
        Returns:
            DataFrame: DataFrame with columns ['Sector', 'Ticker', 'Company'].
        """
        stock_map = []
        for sector, companies in self.stocks.items():
            for ticker, name in companies.items():
                __args = {
                    "Sector": sector,
                    "Ticker": ticker,
                    "Company": name
                }
                stock_map.append(__args)
        df_map = DataFrame(stock_map)
        logger.info("Stock mapping DataFrame created with %d entries.", len(df_map))
        return df_map


    def extract_stock_data(self) -> DataFrame:
        """
        Fetch daily stock data from Yahoo Finance for the provided tickers.
        
        Returns:
            DataFrame: Merged DataFrame containing sector, ticker, company, and stock data.
        """
        df_map = self._create_stock_mapping()
        tickers = df_map["Ticker"].tolist()
        
        logger.info("Fetching stock data for %d tickers...", len(tickers))
        data = download_stock(tickers, period="1d", interval="1d")

        if data.empty:
            logger.warning("No stock data fetched. Please check tickers or network connection.")
            return df_map
        
        data = data.stack(level=1).reset_index()
        data = data.rename(columns={"Price": "Type", 0: "Value"})
        
        results = df_map.merge(data, on="Ticker", how="left")
        logger.info(results)
        logger.info("Stock data fetched and merged successfully.")
        return results

