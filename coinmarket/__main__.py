from coinmarket.models import *
import json


def main():
    
    logger = CoinLogger()
    logger.info("Initializing ... ")
    logger.info("Getting DB Configuration")
    
    with open("config.json") as config_file:
        config = json.load(config_file)
        
    logger.info("DB Configuration Loaded")
    logger.info(str(config))
    
    db_config = DBConfig(config.host, config.db, config.user, config.password, config.port, config.timeout)
    
    
if __name__ == "__main__":
    main()
