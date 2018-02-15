import pandas as pd
import numpy as np


class CoinStatistics:
    
    def __init__(self, data_frame, ticker, logger):
        
        self.data_frame = data_frame
        self.ticker = ticker
        self.logger = logger
        self.size = len(data_frame)

    def calculate_rsi(self, lookback):
    
        series = self.data_frame["day_close"]
        if self.size < lookback:
            self.logger.warn("Not enough data for RSI")
            return
    
        rsi_index = []
        for ind in range(self.size - 1, lookback - 1, -1):
        
            index = ind - lookback
            dataset = series[index:ind]
            up_values = []
            down_values = []
        
            diff = np.diff(dataset)
            n = len(diff)
            if n > 0:
                for j in range(0, n):
                    if diff[j] >= 0:
                        up_values.append(diff[j])
                        down_values.append(0)
                    else:
                        down_values.append(abs(diff[j]))
                        up_values.append(0)
            
                avg_up_days = sum(up_values) / len(up_values)
                avg_down_days = sum(down_values) / len(down_values)
                rs = avg_up_days / avg_down_days
            
                rsi_index.append((100 - (100 / (1 + rs))))
            else:
                rsi_index.append(0)
    
        return rsi_index

    def rolling_std_dev(self, lookback):
    
        series = self.data_frame["day_close"]
        if self.size < lookback:
            self.logger.warn("Not enough data for STD DEV")
            return
    
        std_dev = []
    
        for ind in range(self.size - 1, lookback - 1, -1):
            index = ind - lookback
            dataset = series[index:ind]
            std_dev.append(np.std(dataset))
    
        return std_dev

    def historical_rolling_stats(self, n=14):
    
        self.size = len(self.data_frame)
        self.data_frame["returns"] = (self.data_frame["day_close"] / self.data_frame["day_close"].shift(-1))
        daily_std = self.rolling_std_dev(n)
        rsi_index = self.calculate_rsi(n)
    
        return_frame = pd.DataFrame()
    
        return_frame["close_date"] = self.data_frame["close_date"][:self.size-n]
        return_frame["rsi"] = rsi_index
        return_frame["std_dev"] = daily_std
        return_frame["ticker"] = [self.ticker] * (self.size - n)
        
        return return_frame
    
    def rolling_returns(self, n=5):
    
        if self.size < n:
            self.logger.warn("Not enough data for Returns")
            return
    
        series = self.data_frame["day_close"]
        returns = []
    
        for ind in range(self.size - 1, n, -1):
            index = ind - n
            period_return = (series[ind] - series[index])/series[index]
            returns.append(period_return)
            
        return_frame = pd.DataFrame({
            "close_date": self.data_frame["close_date"][:self.size - n -1],
            "period_return": returns,
            "ticker": [self.ticker] * (self.size - n - 1)
        })
        
        return return_frame
