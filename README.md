# coin-market
ETL pipeline for coin market crypto daily data.

This python program fetches and stores crypto currency OHLCV data
and stores some basic statistic and the raw data.

# How to run

After unzipping the file, go to the root of the directory and install `virtualenv`. Make sure python virtualenv is installed.
The following operations are run on command line.

Step 1: Run `virtualenv venv`.
Step 2: Activate python from the root by running `source venv/bin/activate`.
Step 3: Run `pip install -r requirements.txt`
Step 4: Run `python setup.py install`.

At this point, the DB needs to be running which requires Docker and Flyway. If they are not present in the environment,
please install. Alternatively, if one has a host somewhere they can use the schema under `sql/V0001_coin-market.sql`
and credentials in `sql/conf/localhost.conf` to create a DB environment.

For docker and flyway users, first run the following:
```docker run -e POSTGRES_DB=coinmarket -e POSTGRES_USER=coinmarket -e POSTGRES_PASSWORD=coinmarket -p 5432:5432 postgres:9.5```

Once the docker container is up and running, run flyway to create the schema:

`flyway migrate -configFile=sql/conf/localhost.conf -locations=filesystem:sql`

Once flyway migration is successful, the program can now be run to fetch and store data. 

`python -mcoinmarket`

There are few configuration options available for the user by manually changing the `conf.json` file. If the user willing to get all available data, then before running set `data_parse.type` field to `all`. For selective coins, set the field to `select` and populate `data_parse.tickers` with desired symbols from `coinmarketcap.com`.

Upon completion of the run the data are stored in the following tables:

`currency_status` -> contains coin metadata
`historical_trade_data` -> raw OHLCV data.
`historical_rolling` -> contains rolling RSI and standard deviation. (daily close values only)
`period_returns` -> contains n period rolling return on close values. Right now n is defaulted to 5 which can be changed by       chaning the variable `top_n` in `coinmarket/__main__.py`.
`top_ranking` -> Contains top 5 tickers/ symbols evaluated period_returns.
`bottom_ranking` -> Contains bottom 5 symbols for the aforementioned metric.
