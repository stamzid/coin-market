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

# Improvements

- There are rooms for improvment in this design. Currently, this pipeline is only scraping daily data from a website. If the data is coming from a static source then this can be easily scaled by deploying this to multiple server instances. The underlying assumption here is that the source is a webpage like coinmarket. For dynamic data sources / streams, there needs to be a layer added on top of the existing architecture. Python libraries like spark streaming and/ or asynchronous request handling libraries will be required. The storage needs to be improved to deal with big data. Currently, this is designed to use a postgreql host. For larger storages the same schemas can be used on AWS services such as Athena for faster query and transaction.

- Assuming this will be deployed on a cloud instance, the program needs to be modified to be run as a service and will need to be triggered by using REST requests / scheduled by using scheduling services such as chron. Alternatively, for event driven pipeline, AWS lambda could possibly come in handy. The service needs to have the capability to reboot in the events of failure and proper logging of the pre-failure and failure state needs to be incorporated. There are services and tools such Kibana or slack integration/ email notification could be useful for this.

- For very large data systems, I would potentially divide the pipeline in two environments. For initial fetching and distributing workload I would prefer to use Scala for it's concurrent features such as Actor Model. Once the raw data in the system, then we can use event driven python stats/ numpy libraries for modelling and calculating statistic.
