/*
  Schemas for data and analysis storage
*/

create table currency_status (
  id bigserial,
  coin_ticker text primary key,
  coin_name text not null,
  last_update timestamp not null,
  market_cap text,
  slug text not null,
  rank int
);

create unique index currency_status_idx on currency_status(coin_ticker);

create table historical_trade_data (
  ticker text references currency_status(coin_ticker),
  close_date timestamp not null,
  day_open numeric,
  daily_high numeric,
  daily_low numeric,
  day_close numeric,
  volume_trade bigint,
  primary key (ticker, close_date)
);

create unique index historical_trade_idx on historical_trade_data(ticker, close_date);

create table historical_rolling_stats(
  ticker text references currency_status(coin_ticker),
  close_date timestamp not null,
  mean_intraday_returns numeric,
  rsi numeric,
  std_dev numeric,
  primary key (ticker, close_date)
);

create unique index historical_rolling_idx on historical_trade_data(ticker, close_date);

create table extreme_performers(
  ticker text references currency_status(coin_ticker),
  close_date timestamp not null,
  gainers text not null,
  losers text not null,
  primary key (ticker, close_date)
);

create unique index extreme_performers_idx on historical_trade_data(ticker, close_date);

create table coin_relative_percentile(
  ticker text references currency_status(coin_ticker),
  close_date timestamp not null,
  five_day_return numeric,
  percentile int,
  primary key (ticker, close_date)
);

create unique index relative_percentile_idx on historical_trade_data(ticker, close_date);