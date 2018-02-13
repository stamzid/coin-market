import psycopg2
from psycopg2.extras import DictCursor
from psycopg2.extensions import AsIs
import datetime


class ApplicationSql:
    
    def __init__(self, db_config, log):
        
        host = db_config.host
        name = db_config.db
        user = db_config.user
        password = db_config.password
        port = db_config.port
        
        self.logger = log
        
        self.__currency_status = AsIs("currency_status")
        self.__historical_trade_data = AsIs("historical_trade_data")
        
        psydsn = "host={} dbname={} user={} password={} port={}".format(host, name, user, password, port)
        
        try:
            self.conn = psycopg2.connect(psydsn, cursor_factory=DictCursor)
            self.logger.info("Connected to database")
            self.cursor = self.conn.cursor()
        except psycopg2.DatabaseError as e:
            self.logger.error(str(e))
            
    def close(self):
        
        self.conn.close()
        
    def get_currency_status(self, ticker):
        
        get_dict = {
            "coin_ticker": ticker,
            "status_table": self.__currency_status
        }
        self.logger.info("Getting status for ticker: " + str(ticker))
        try:
            self.cursor.execute("""
                SELECT "coin_ticker" as ticker, "coin_name" as name, "last_update" as update, "market_cap" as cap
                FROM %(status_table)s WHERE "coin_ticker"=%(coin_ticker)s;""", get_dict)
            
            row = self.cursor.fetchall()[0]
            ret_dict = {
                "ticker": row["ticker"],
                "name": row["name"],
                "last_update": row["update"],
                "market_cap": row["cap"]
            }
        except IndexError:
            self.logger.error("No Entry found for the ticker")
            ret_dict = {}
        except psycopg2.Error as e:
            self.logger.info(str(e))
            ret_dict = {}
            
        return ret_dict
    
    def insert_currency_status(self, status_obj):
        
        status_dict = {
            "coin_ticker": status_obj["ticker"],
            "coin_name": status_obj["name"],
            "market_cap": status_obj["market_cap"],
            "last_update": status_obj["last_update"],
            "status_table": self.__currency_status
        }
        
        self.logger.info("Creating status for ticker: " + status_obj["ticker"])
        try:
            self.cursor.execute("""INSERT INTO %(status_table)s ("coin_ticker", "coin_name", "last_update", "market_cap")
                SELECT
                    %(coin_ticker)s,
                    %(coin_name)s,
                    %(last_update)s,
                    %(market_cap)s
                ;""", status_dict)
            self.conn.commit()
            self.logger.info("Status created")
        except psycopg2.Error as e:
            self.conn.rollback()
            self.logger.error(str(e))
    
    def update_currency_status(self, ticker, time_stamp):
        
        update_dict = {
            "ticker": ticker,
            "last_update": time_stamp,
            "status_table": self.__currency_status
        }
        
        self.logger.info("Updating status for ticker: " + str(ticker))
        try:
            self.cursor.execute("""UPDATE %(status_table)s
            SET "last_update"=%(last_update)s WHERE "coin_ticker"=%(ticker)s;""", update_dict)
            self.conn.commit()
            self.logger.info("Status Updated")
        except psycopg2.Error as e:
            self.conn.rollback()
            self.logger.error(str(e))
    
    def delete_currency_status(self, ticker):
        
        ticker_dict = {
            "coin_ticker": ticker,
            "status_table": self.__currency_status
        }
        
        self.logger.info("Deleting status for ticker: " + str(ticker))
        try:
            self.cursor.execute("""DELETE FROM %(status_table)s WHERE "coin_ticker"=%(coin_ticker)s;""", ticker_dict)
            self.conn.commit()
            self.logger.info("Status Deleted")
        except psycopg2.Error as e:
            self.conn.rollback()
            self.logger.error(str(e))