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

create table historical_rolling(
  ticker text references currency_status(coin_ticker),
  close_date timestamp not null,
  rsi numeric,
  std_dev numeric,
  primary key (ticker, close_date)
);

create unique index historical_rolling_idx on historical_rolling(ticker, close_date);

create table period_return(
  ticker text references currency_status(coin_ticker),
  close_date timestamp not null,
  period_return numeric,
  primary key (ticker, close_date)
);

create unique index period_return_idx on historical_trade_data(ticker, close_date);

create table top_ranking(
  close_date timestamp not null,
  gainers text references currency_status(coin_ticker),
  returns numeric,
  primary key (close_date, gainers)
);

create unique index top_rank_idx on top_ranking(close_date, gainers);

create table bottom_ranking(
  close_date timestamp not null,
  losers text references currency_status(coin_ticker),
  returns numeric,
  primary key (close_date, losers)
);

create unique index bottom_rank_idx on bottom_ranking(close_date, losers);