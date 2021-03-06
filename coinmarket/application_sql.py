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
        
        psydsn = "host={} dbname={} user={} password={} port={}".format(host, name, user, password, port)
        
        try:
            self.conn = psycopg2.connect(psydsn, cursor_factory=DictCursor)
            self.logger.info("Connected to database")
            self.cursor = self.conn.cursor()
        except psycopg2.DatabaseError as e:
            self.logger.error(str(e))
     
    def get_connection(self):
        
        return self.conn
        
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
            self.logger.info("Trying update")
            self.update_currency_status(status_dict["coin_ticker"], status_dict["last_update"], status_dict["rank"])
            
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
            
    def get_all_historical(self, ticker):
        
        ticker_dict = {"ticker": ticker}
        try:
            self.cursor.execute("""SELECT ticker as ticker, close_date as close_date, day_open as day_open,
                day_close as day_close, daily_high as daily_high, daily_low as daily_low, volume_trade as volume_trade
                FROM historical_trade_data
                ;""", ticker_dict)
            return self.cursor.fetchall()
        except psycopg2.Error:
            self.conn.rollback()
            self.logger.error("failed to get historical data for ticker: "+str(ticker))
            
    def insert_historical_data(self, data_row, temp_file, table, header):
    
        file_path = os.path.join(os.getcwd(), temp_file)
        w = open(file_path, "w")
        out_writer = csv.DictWriter(w, fieldnames=header, delimiter='~', escapechar='\\', quoting=csv.QUOTE_NONE)
        
        for row in data_row:
            out_writer.writerow(row)

        w.close()

        f = open(file_path, "rb")
        try:
            self.cursor.copy_from(f, table, sep="~", columns=header)
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            self.logger.error(str(e))
            
        f.close()
        os.remove(file_path)
        
    def delete_from_historical(self, ticker, table):
        
        tab_arg = {"table": AsIs(table), "ticker": ticker}
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
        
    def get_top_ranking(self):
        
        self.logger.info("Fetching Ranking")
        try:
            self.cursor.execute(""" SELECT close_date as date, ticker as ticker, period_return as return
                FROM period_return
                GROUP BY close_date, ticker
                ORDER BY close_date, period_return desc;""")
            return self.cursor.fetchall()
        except psycopg2.Error as e:
            self.conn.rollback()
            self.logger.error("Error Fetching Top gainers")
            return None

    def get_bottom_ranking(self):
    
        self.logger.info("Fetching Ranking")
        try:
            self.cursor.execute(""" SELECT close_date as date, ticker as ticker, period_return as return
                FROM period_return
                GROUP BY close_date, ticker
                ORDER BY close_date desc, period_return asc;""")
            return self.cursor.fetchall()
        except psycopg2.Error as e:
            self.conn.rollback()
            self.logger.error("Error Fetching Top gainers")
            return None

    def delete_from_rankings(self, table):
        
        self.logger.info("Deleting Ranking Table entries")
        try:
            self.cursor.execute("""DELETE FROM top_ranking;""")
            self.conn.commit()
            self.cursor.execute("""DELETE FROM bottom_ranking;""")
            self.conn.commit()
            self.logger.info("Successfully deleted")
        except psycopg2.Error:
            self.conn.rollback()
            self.logger.error("Deletion of ranking failed")
