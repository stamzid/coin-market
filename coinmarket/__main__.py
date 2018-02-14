from coinmarket.models import *
from coinmarket.application_sql import ApplicationSql
from coinmarket.coin_market_parser import CoinMarketParser
import json
import ssl
from urllib import request
from bs4 import BeautifulSoup
import pandas as pd
import datetime


GLOBAL_START = "20130428"
GLOBAL_END = datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d")
RAW_TABLE = "historical_trade_data"
STATUS_TABLE = "currency_status"
ROLLING_TABLE = "historical_rolling_stats"
EXTREME_TABLE = "extreme_performer"
PERCENTILE_TABLE = "coin_relative_percentile"


def url_data_provider(page_url):
    
    ssl._create_default_https_context = ssl._create_unverified_context
    page = request.urlopen(page_url).read()
    soup = BeautifulSoup(page, "html.parser")
    
    return json.loads(str(soup))


def quick_search():
    
    all_coin_url = "https://files.coinmarketcap.com/generated/search/quick_search.json"
    return url_data_provider(all_coin_url)


def coin_selector(all_coins, required_coins):
    
    coin_select = []
    
    for coin in all_coins:
        if coin["symbol"] in required_coins:
            coin_select.append(coin)
            
    return coin_select


def trade_data_processor(coin_parser, coin_dict, logger):
    
    logger.info("Processing Raw Historical Data")
    try:
        logger.info("Checking if ticker already existed in the db")
        status_dict = coin_parser.get_currency_status(coin_dict["symbol"])
        status_dict["name"]
        latest_date = coin_parser.check_latest_date(coin_dict["symbol"], RAW_TABLE)
        try:
            start = latest_date[0]["close"].strftime("%Y%m%d")
        except IndexError:
            logger.error("Unknown error, ticker will be skipped, manually check db for ticker: " + str(coin_dict["symbol"]))
            return
    except TypeError:
        logger.info("Ticker does not exist in db, will load since beginning of time")
        start = GLOBAL_START
        
    hist_trade_data = coin_parser.parse_historical_range(coin_dict["slug"], coin_dict["symbol"], start, GLOBAL_END)
    coin_parser.add_historical(hist_trade_data)
    data_frame = pd.DataFrame(hist_trade_data)


def module_runner(config, coin_list, logger):
    
    summary_url = "https://api.coinmarketcap.com/v1/ticker/"
    
    db_host = config["dbconf"]["host"]
    db_name = config["dbconf"]["db"]
    db_user = config["dbconf"]["user"]
    db_pass = config["dbconf"]["password"]
    db_port = config["dbconf"]["port"]
    
    db_config = DBConfig(db_host, db_name, db_user, db_pass, db_port)
    main_sql = ApplicationSql(db_config, logger)
    coin_parser = CoinMarketParser(main_sql, logger)
    
    for coin_dict in coin_list:
        
        try:
            ticker_summary = url_data_provider(summary_url + str(coin_dict["slug"]))[0]
        except IndexError as ie:
            logger.error(str(ie))
            logger.warn("skipping ticker: " + coin_dict["symbol"])
            continue
        
        last_update = datetime.datetime.now()
        status_dict = {
            "ticker": coin_dict["symbol"],
            "slug": coin_dict["slug"],
            "name": coin_dict["name"],
            "rank": coin_dict["rank"],
            "market_cap": ticker_summary["market_cap_usd"],
            "last_update": last_update
        }
        coin_parser.create_status(status_dict)
        trade_data_processor(coin_parser,  coin_dict, logger)
    
    main_sql.close()

    
def main():
    
    logger = CoinLogger()
    logger.info("Initializing ... ")
    logger.info("Getting DB Configuration")
    
    with open("config.json") as config_file:
        config = json.load(config_file)
        
    logger.info("DB Configuration Loaded")
    logger.info(str(config))
    
    data_parse_options = config["data_parse"]
    logger.info("Loading all tickers")
    all_coins = quick_search()
    
    if data_parse_options["type"] == "select":
        coin_select = coin_selector(all_coins, data_parse_options["tickers"])
        module_runner(config, coin_select, logger)
    else:
        module_runner(config, all_coins, logger)
    
    
if __name__ == "__main__":
    main()
