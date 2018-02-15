import datetime
import json
import ssl
from urllib import request

from bs4 import BeautifulSoup


class CoinMarketParser:
    
    def __init__(self, app_sql, logger):
        
        ssl._create_default_https_context = ssl._create_unverified_context
        self.absolute_start = "20130428"
        self.base_url_one = "https://coinmarketcap.com/currencies/"
        self.base_url_two = "/historical-data/?start="
        self.base_url_three = "&end="
        self.api_url = "https://api.coinmarketcap.com/v1/ticker/"
        self.app_sql = app_sql
        self.logger = logger
        
    def __historical_url_builder(self, currency, end, start=None):
        
        return self.base_url_one + str(currency) + self.base_url_two + str(start) + self.base_url_three + str(end)
    
    def __ticker_api_url(self, ticker_id):
        
        return self.api_url + str(ticker_id)

    def __process_numeric(self, string, type_numeric=True):
    
        stripped = string.strip().replace(",", "").strip()
        try:
            if type_numeric:
                value = float(stripped)
            else:
                value = int(stripped)
        except ValueError:
            value = -1
    
        return value
    
    def quick_search(self):
        
        return
    
    def check_latest_date(self, ticker, table):
        
        return self.app_sql.check_latest_date(ticker, table)
    
    def parse_historical(self, ticker, end):
        
        return self.parse_historical_range(ticker, self.absolute_start, end)
    
    def parse_historical_range(self, slug, ticker, start, end):
        
        url_str = self.__historical_url_builder(slug, end, start)
        page = request.urlopen(url_str).read()
        soup = BeautifulSoup(page, "html.parser")
        table = soup.find("table", {"class": "table"})
        data_rows = []
    
        for row in table.findAll("tr")[1:]:
            row_dict = {}
            cells = row.findAll("td")
            row_dict["ticker"] = ticker
            row_dict["close_date"] = datetime.datetime.strptime(cells[0].find(text=True), "%b %d, %Y").strftime("%Y-%m-%d")
            row_dict["day_open"] = self.__process_numeric(cells[1].find(text=True))
            row_dict["daily_high"] = self.__process_numeric(cells[2].find(text=True))
            row_dict["daily_low"] = self.__process_numeric(cells[3].find(text=True))
            row_dict["day_close"] = self.__process_numeric(cells[4].find(text=True))
            row_dict["volume_trade"] = self.__process_numeric(cells[5].find(text=True), False)
            data_rows.append(row_dict)
                
        return data_rows
    
    def parse_ticker_data(self, ticker_id):
        
        url_str = self.__ticker_api_url(ticker_id)
        page = request.urlopen(url_str).read()
        soup = BeautifulSoup(page, "html.parser")
        
        return json.loads(str(soup))
    
    def add_historical(self, data_row, temp_file, table, header):
        
        self.app_sql.insert_historical_data(data_row, temp_file, table, header)
        
    def delete_historical(self, ticker, table):
        
        self.app_sql.delete_from_historical(ticker, table)
        
    def create_status(self, status_dict):
        
        self.app_sql.insert_currency_status(status_dict)
        
    def get_currency_status(self, ticker):
        
        return self.app_sql.get_currency_status(ticker)
        
    def update_status(self, ticker, timestamp):
        
        self.app_sql.update_currency_status(ticker, timestamp)
        
    def delete_status(self, ticker):
        
        self.app_sql.delete_currency_status(ticker)
        
    def get_all_historical(self, ticker):
        
        return self.app_sql.get_all_historical(ticker)
