from coinmarket.models import *
from coinmarket.application_sql import ApplicationSql
from coinmarket.coin_market_parser import CoinMarketParser
from coinmarket.coin_statistics import CoinStatistics
import json
import ssl
from urllib import request
from bs4 import BeautifulSoup
import pandas as pd
import datetime

GLOBAL_START = "20170101"
GLOBAL_END = datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d")
RAW_TABLE = "historical_trade_data"
STATUS_TABLE = "currency_status"
ROLLING_TABLE = "historical_rolling"
TOP_TABLE = "top_ranking"
BOTTOM_TABLE = "bottom_ranking"
RETURN_TABLE = "period_return"

historical_header = ["ticker", "close_date", "day_open", "daily_high", "daily_low", "day_close", "volume_trade"]
rolling_header = ["ticker", "close_date", "rsi", "std_dev"]
period_header = ["ticker", "close_date", "period_return"]
top_header = ["close_date", "gainers", "returns"]
bottom_header = ["close_date", "losers", "returns"]
top_n = 5


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


def dict_row_to_list(dict_row):
    
    data_rows = []

    for row in dict_row:
        
        row_dict = dict()
        row_dict["ticker"] = row["ticker"]
        row_dict["close_date"] = row["close_date"]
        # datetime.datetime.strptime(row["close_date"], "%b %d, %Y").strftime("%Y-%m-%d")
        row_dict["day_open"] = float(row["day_open"])
        row_dict["daily_high"] = float(row["daily_high"])
        row_dict["daily_low"] = float(row["daily_low"])
        row_dict["day_close"] = float(row["day_close"])
        row_dict["volume_trade"] = int(row["volume_trade"])
        data_rows.append(row_dict)
    
    return data_rows


def trade_data_processor(coin_parser, coin_dict, logger):
    
    logger.info("Processing Raw Historical Data")
    try:
        logger.info("Checking if ticker already existed in the db")
        latest_date = coin_parser.check_latest_date(coin_dict["symbol"], RAW_TABLE)
        logger.info("Latest date list: " + str(latest_date))
        start = latest_date[0]["close"] + datetime.timedelta(days=1)
        start_str = start.strftime("%Y%m%d")
        try:
            logger.info("Adding data starting from date: "+str(start))
            hist_trade_data = coin_parser.parse_historical_range(coin_dict["slug"], coin_dict["symbol"], start_str,
                                                                 GLOBAL_END)
            coin_parser.add_historical(hist_trade_data, "temp_csv_hist.csv", RAW_TABLE, historical_header)
            all_history_rows = coin_parser.get_all_historical(coin_dict["symbol"])
            all_history = dict_row_to_list(all_history_rows)
            
            data_frame = pd.DataFrame(all_history)

            coin_parser.delete_historical(coin_dict["symbol"], ROLLING_TABLE)
            coin_parser.delete_historical(coin_dict["symbol"], RETURN_TABLE)

            logger.info("Calculating Statistic")
            coin_statistics = CoinStatistics(data_frame, coin_dict["symbol"], logger)
            rolling_frame = coin_statistics.historical_rolling_stats()
            returns = coin_statistics.rolling_returns()

            coin_parser.add_historical(rolling_frame.T.to_dict().values(), "temp_rolling.csv", ROLLING_TABLE,
                                       rolling_header)
            coin_parser.add_historical(returns.T.to_dict().values(), "temp_returns.csv", RETURN_TABLE, period_header)
        except ValueError:
            logger.info("Already latest data in the db")
    except IndexError:
        logger.info("Ticker does not exist in db, will load since beginning of time")
        
        hist_trade_data = coin_parser.parse_historical_range(coin_dict["slug"], coin_dict["symbol"], GLOBAL_START, GLOBAL_END)
        coin_parser.add_historical(hist_trade_data, "temp_csv_hist.csv", RAW_TABLE, historical_header)
        data_frame = pd.DataFrame(hist_trade_data)
        
        coin_parser.delete_historical(coin_dict["symbol"], ROLLING_TABLE)
        coin_parser.delete_historical(coin_dict["symbol"], RETURN_TABLE)

        logger.info("Calculating Statistic")
        coin_statistics = CoinStatistics(data_frame, coin_dict["symbol"], logger)
        rolling_frame = coin_statistics.historical_rolling_stats()
        returns = coin_statistics.rolling_returns()
        
        coin_parser.add_historical(rolling_frame.T.to_dict().values(), "temp_rolling.csv", ROLLING_TABLE, rolling_header)
        coin_parser.add_historical(returns.T.to_dict().values(), "temp_returns.csv", RETURN_TABLE, period_header)


def top_list_builder(cursor_rows):
    
    rank_list = []
    for row in cursor_rows:
        arg = {
            "close_date": row["date"],
            "gainers": row["ticker"],
            "returns": row["return"]
        }
        
        rank_list.append(arg)
        
    return rank_list


def bottom_list_builder(cursor_rows):
    rank_list = []
    for row in cursor_rows:
        arg = {
            "close_date": row["date"],
            "losers": row["ticker"],
            "returns": row["return"]
        }
        
        rank_list.append(arg)
    
    return rank_list


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

    logger.info("Calculating Rankings")
    main_sql.delete_from_rankings(TOP_TABLE)
    main_sql.delete_from_rankings(BOTTOM_TABLE)
    toprank = main_sql.get_top_ranking()
    top_frame = pd.DataFrame(top_list_builder(toprank)).groupby('close_date').head(top_n)
    bottomrank = main_sql.get_bottom_ranking()
    bot_frame = pd.DataFrame(bottom_list_builder(bottomrank)).groupby('close_date').head(top_n)

    main_sql.insert_historical_data(top_frame.T.to_dict().values(), "topranks.csv", TOP_TABLE, top_header)
    main_sql.insert_historical_data(bot_frame.T.to_dict().values(), "botranks.csv", BOTTOM_TABLE, bottom_header)
    
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
