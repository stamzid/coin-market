import psycopg2
from psycopg2.extensions import AsIs
from psycopg2.extras import DictCursor
import csv
import os


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
        self.__historical_header = ["ticker", "close_date", "day_open", "daily_high", "daily_low", "day_close", "volume_trade"]
        self.__hist_table_name = "historical_trade_data"
        
        psydsn = "host={} dbname={} user={} password={} port={}".format(host, name, user, password, port)
        
        try:
            self.conn = psycopg2.connect(psydsn, cursor_factory=DictCursor)
            self.logger.info("Connected to database")
            self.cursor = self.conn.cursor()
        except psycopg2.DatabaseError as e:
            self.logger.error(str(e))
            
    def close(self):
        
        self.conn.close()
        self.logger.info("Closed DB Connection")
        
    def get_currency_status(self, ticker):
        
        get_dict = {
            "coin_ticker": ticker,
            "status_table": self.__currency_status
        }
        self.logger.info("Getting status for ticker: " + str(ticker))
        try:
            self.cursor.execute("""
                SELECT "coin_ticker" as ticker, "coin_name" as name, "last_update" as update, "market_cap" as cap,
                    "slug" as slug, "rank" as rank
                FROM %(status_table)s WHERE "coin_ticker"=%(coin_ticker)s;""", get_dict)
            
            row = self.cursor.fetchall()[0]
            ret_dict = {
                "ticker": row["ticker"],
                "name": row["name"],
                "last_update": row["update"],
                "market_cap": row["cap"],
                "slug": row["slug"],
                "rank": row["rank"]
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
            "slug": status_obj["slug"],
            "rank": status_obj["rank"],
            "status_table": self.__currency_status
        }
        
        self.logger.info("Creating status for ticker: " + status_obj["ticker"])
        try:
            self.cursor.execute("""INSERT INTO %(status_table)s ("coin_ticker", "coin_name", "last_update",
                "market_cap", "slug", "rank")
                SELECT
                    %(coin_ticker)s,
                    %(coin_name)s,
                    %(last_update)s,
                    %(market_cap)s,
                    %(slug)s,
                    %(rank)s
                ;""", status_dict)
            self.conn.commit()
            self.logger.info("Status created")
        except psycopg2.Error as e:
            self.conn.rollback()
            self.logger.error(str(e))
    
    def update_currency_status(self, ticker, time_stamp, rank):
        
        update_dict = {
            "ticker": ticker,
            "last_update": time_stamp,
            "rank": rank,
            "status_table": self.__currency_status
        }
        
        self.logger.info("Updating status for ticker: " + str(ticker))
        try:
            self.cursor.execute("""UPDATE %(status_table)s
            SET "last_update"=%(last_update)s, "rank"=%(rank)s WHERE "coin_ticker"=%(ticker)s;""", update_dict)
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
            
    def insert_historical_data(self, data_row):
    
        file_path = os.path.join(os.getcwd(), "temp_hist.csv")
        w = open(file_path, "w")
        out_writer = csv.DictWriter(w, fieldnames=self.__historical_header, delimiter='~', escapechar='\\', quoting=csv.QUOTE_NONE)
        
        for row in data_row:
            out_writer.writerow(row)

        w.close()

        f = open(file_path, "rb")
        try:
            self.cursor.copy_from(f, self.__hist_table_name, sep="~", columns=self.__historical_header)
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            self.logger.error(str(e))
            
        f.close()
        os.remove(file_path)
        
    def delete_from_historical(self, ticker):
        
        tab_arg = {"table": self.__historical_trade_data, "ticker": ticker}
        try:
            self.cursor.execute("""DELETE FROM %(table)s where ticker=%(ticker)s;""", tab_arg)
            self.conn.commit()
        except psycopg2.Error as e:
            self.conn.rollback()
            self.logger.error(str(e))
            
    def check_latest_date(self, ticker, table):
    
        tab_arg = {"table": AsIs(table), "ticker": ticker}
        try:
            self.cursor.execute("""SELECT DISTINCT close_date as close FROM %(table)s WHERE ticker=%(ticker)s
                ORDER BY close_date desc;""", tab_arg)
            return self.cursor.fetchall()[:1]
        except psycopg2.Error as e:
            self.conn.rollback()
            self.logger.error(str(e))
            return []
