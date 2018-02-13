from bs4 import BeautifulSoup
from urllib import request, parse
import ssl

class CoinMarketParser:
    
    def __init__(self, app_sql):
        
        ssl._create_default_https_context = ssl._create_unverified_context
        self.absolute_start = "20130428"
        self.base_url_one = "https://coinmarketcap.com/currencies/"
        self.base_url_two = "/historical-data/?start="
        self.base_url_three = "&end="
        self.app_sql = app_sql
        
    def __historical_url_builder(self, currency, end, start=None):
        
        return self.base_url_one + str(currency) + self.base_url_two + str(start) + self.base_url_three + str(end)
    
    def get_historical(self, ticker, end):
        return
    
    def get_custom_range(self, ticker, start, end):
        return
    
    def parse_historical(self, ticker):
        return
    
    def parse_historical_range(self, name, start, end):
        
        url_str = self.__historical_url_builder(name, end, start)
        page = request.urlopen(url_str).read()
        soup = BeautifulSoup(page, 'html.parser')
        table = soup.find("table", {"class": "table"})
        
        for row in table.findAll("tr"):
            cells = row.findAll("td")
            print(cells)
    
    def add_historical(self):
        return
    
    
    
    
    