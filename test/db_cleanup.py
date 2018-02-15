from coinmarket.application_sql import ApplicationSql
from coinmarket.models import CoinLogger, DBConfig
import json

if __name__ == '__main__':
    
    with open("config.json") as config_file:
        config = json.load(config_file)
    
    conf = config["dbconf"]
    db_config = DBConfig(conf["host"], conf["db"], conf["user"], conf["password"], conf["port"])
    logger = CoinLogger()
    app_sql = ApplicationSql(db_config, logger)
    
    logger.info("Initiating deletion of all data")
    app_sql.cursor.execute("""DELETE FROM top_ranking;""")
    app_sql.cursor.execute("""DELETE FROM bottom_ranking;""")
    app_sql.cursor.execute("""DELETE FROM period_return;""")
    app_sql.cursor.execute("""DELETE FROM historical_rolling;""")
    app_sql.cursor.execute("""DELETE FROM historical_trade_data;""")
    app_sql.cursor.execute("""DELETE FROM currency_status;""")