import logging
import sys


class DBConfig:
    
    def __init__(self, host, db_name, user, password, port=5432):
        self.host = host
        self.db = db_name
        self.user = user
        self.password = password
        self.port = port
        

class CoinLogger:
    
    def __init__(self):
    
        self.logger = logging.getLogger('coinmarket')
        console_handler = logging.StreamHandler(stream=sys.stdout)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.DEBUG)
        self.logger.addHandler(console_handler)
        self.logger.setLevel(logging.DEBUG)
        
    def info(self, msg):
        
        self.logger.info(msg)
        
    def debug(self, msg):
        
        self.logger.debug(msg)
        
    def warn(self, msg):
        
        self.logger.warning(msg)
        
    def error(self, msg):
        
        self.logger.error(msg)
        
    def critical(self, msg):
        
        self.logger.critical(msg)

        
class CurrencyStatus:
    
    def __init__(self, ticker, name, market_cap=None):
        
        self.ticker = ticker
        self.name = name
        self.market_cap = market_cap
