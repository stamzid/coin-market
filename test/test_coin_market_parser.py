import unittest
import json
from coinmarket.models import *
from coinmarket.application_sql import ApplicationSql
from coinmarket.coin_market_parser import CoinMarketParser


class TestCoinMarketParser(unittest.TestCase):
    
    def test_custom_parser_range(self):
        
        with open("config.json") as config_file:
            config = json.load(config_file)
    
        conf = config["dbconf"]
        db_config = DBConfig(conf["host"], conf["db"], conf["user"], conf["password"], conf["port"])
        logger = CoinLogger()
        app_sql = ApplicationSql(db_config, logger)
        coin_market_parser = CoinMarketParser(app_sql)
        currency = "ethereum"
        start = "20180201"
        end = "20180208"
        
        coin_market_parser.parse_historical_range(currency, start, end)
    
    
if __name__ == "__main__":
    unittest.main(verbosity=2)
