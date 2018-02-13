/*
  Schemas for data and analysis storage
*/

create table currency_status (
  id bigserial,
  coin_ticker text primary key,
  coin_name text,
  last_update timestamp,
  market_cap text
);

create unique index currency_status_idx on currency_status(coin_ticker);

create table historical_trade_data (
  ticker text references currency_status(coin_ticker),
  close_date timestamp,
  day_open numeric,
  daily_high numeric,
  day_close numeric,
  volume_trade bigint,
  primary key (ticker, close_date)
);

create unique index historical_trade_idx on historical_trade_data(ticker, close_date);