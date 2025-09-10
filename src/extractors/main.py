from stocks_yfinance import StockDataFetcher
from news_scraper import GoogleNewsExtractor

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

def main():
    stocks = StockDataFetcher(STOCKS).extract_stock_data()
    #news = GoogleNewsExtractor(STOCKS).extract_news_data()
    #print(news)
    print(stocks)


    stocks.to_csv("stocks.csv", index=False)    
if __name__ == "__main__":
    main()