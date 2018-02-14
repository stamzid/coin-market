import json
import unittest
import datetime

from coinmarket.application_sql import ApplicationSql
from coinmarket.coin_market_parser import CoinMarketParser
from coinmarket.models import *


class TestCoinMarketParser(unittest.TestCase):
    
    def test_custom_parser_range(self):
        
        with open("config.json") as config_file:
            config = json.load(config_file)
    
        conf = config["dbconf"]
        db_config = DBConfig(conf["host"], conf["db"], conf["user"], conf["password"], conf["port"])
        logger = CoinLogger()
        app_sql = ApplicationSql(db_config, logger)
        coin_market_parser = CoinMarketParser(app_sql, logger)
        currency = "Bitcoin"
        start = "20180201"
        end = "20180208"
        current_date = datetime.datetime.now()

        test_ticker = "BTC"

        status_dict = {
            "ticker": test_ticker,
            "name": "Bitcoin",
            "market_cap": 1234567889,
            "last_update": current_date,
            "slug": "bitcoin",
            "rank": 1
        }

        coin_market_parser.create_status(status_dict)
        
        hist_data = coin_market_parser.parse_historical_range(currency, test_ticker, start, end)
        ticker_data = coin_market_parser.parse_ticker_data("bitcoin")
        coin_market_parser.add_historical(hist_data)
        
        self.assertEqual(hist_data[0]["day_open"], 7637.86, "Open price for february 8th")
        self.assertEqual(ticker_data[0]["id"], "bitcoin", "Ticker from api should be bitcoin")
        
        new_end = datetime.datetime.strptime("20180212", "%Y%m%d")
        db_end = coin_market_parser.check_latest_date(test_ticker, "historical_trade_data")[0]["close"]
    
        self.assertGreater(new_end, db_end, "new date is later")
        new_start = db_end + datetime.timedelta(days=1)
        
        new_start = new_start.strftime("%Y%m%d")
        new_end = new_end.strftime("%Y%m%d")
        
        new_hist = coin_market_parser.parse_historical_range("bitcoin", test_ticker, new_start, new_end)
        coin_market_parser.add_historical(new_hist)
        
        new_date = coin_market_parser.check_latest_date(test_ticker, "historical_trade_data")[0]["close"].strftime("%Y%m%d")
        self.assertEqual(new_end, new_date, "latest entry in the db should be udpated")
        
        coin_market_parser.delete_historical(test_ticker)
        coin_market_parser.delete_status(test_ticker)
        app_sql.close()
        
    
if __name__ == "__main__":
    unittest.main(verbosity=2)
