import unittest
import json
import datetime
from coinmarket.application_sql import *
from coinmarket.models import CoinLogger, DBConfig


class TestApplicationSql(unittest.TestCase):
    
    def test_currency_status(self):
        
        with open("config.json") as config_file:
            config = json.load(config_file)

        conf = config["dbconf"]
        db_config = DBConfig(conf["host"], conf["db"], conf["user"], conf["password"], conf["port"])
        logger = CoinLogger()
        app_sql = ApplicationSql(db_config, logger)
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
        
        app_sql.insert_currency_status(status_dict)
        status = app_sql.get_currency_status(test_ticker)
        self.assertEqual(status["ticker"], test_ticker, "Ticker should be BTC")
        self.assertEqual(status["last_update"], current_date, "last update should be today")
        
        updated_time = datetime.datetime.now()
        status_dict["last_update"] = updated_time
        app_sql.update_currency_status(test_ticker, updated_time, 2)
        status = app_sql.get_currency_status(test_ticker)
        self.assertEqual(status["last_update"], updated_time, "Status should be updated")
        self.assertEqual(status["rank"], 2, "rank should be updated")
        app_sql.delete_currency_status(test_ticker)
        status = app_sql.get_currency_status(test_ticker)
        self.assertEqual(status, {}, "It should return empty dictionary after deletion")
        app_sql.close()
        
        
if __name__ == "__main__":
    unittest.main(verbosity=1)