# HPI Information integration project ST2022

## Task 2: Setup and getting data

The most important components of the project are Apache Kafka, Kafka Connect and the custom producers [rb_crawler](./rb_crawler/) and [csv_crawler](./csv_producer/). More detail can be found in [INITAL_README.md](./INITIAL_README.md).

1. Setup Kafka, Kafka Connect and some other services:

    ```bash
    docker compose up -d
    ```

2. Depending on your wishes setup the [Elasticsearch connector](./connect/elastic-sink.json) and/or [SQLite connector](./connect/sqlite-sink.json). All events of the important topics will be exported to these databases.

    ```bash
    cd connect/
    bash push-config.sh localhost 8083 elastic-sink.json
    bash push-config.sh localhost 8083 sqlite-sink.json
    ```

    The SQLite connector produces a dump in the [data/](./data/) directory.

3. Produce events (see next section)

### Producing events

You probably need to setup all python dependencies first:

```bash
bash generate-proto.sh
poetry install
```

Note: At the point of writing, `confluent-kafka` appears to be incompatible with Python3.10.  
You can use [pyenv](https://github.com/pyenv/pyenv) to install an alternative python version (such as 3.9) on your system.  
Note that you have to run `poetry env use python3.9` in a context where pyenv is active (see [here](https://github.com/python-poetry/poetry/issues/5252)) for poetry to actually use the python version specified by pyenv.

#### RB crawler

The [rb_crawler](./rb_crawler/) can be used to get announcements from <https://www.handelsregisterbekanntmachungen.de/> (see [INITIAL_README.md](./INITIAL_README.md).

You can also use the [main_multi.py](./rb_crawler/main_multi.py) script to crawl the website more efficiently (faster). There you can also adjust the starting IDs.

After the retrieved information have been dumped into a SQLite database, they can then be parsed and transformed into a more structured data format.
To achieve this, first perform the [schema transformation](./rb_crawler/rb_schema_transform.sql) and run [rb_parser](./rb_crawler/rb_parser.py) afterwards.

```bash
cd rb_crawler
poetry run python main_multy.py
sqlite3 --echo ../data/some-database-file.sqlite < rb_schema_transform.sql
poetry run python rb_parser.py
```

#### LEI data

We also ingest data from the [Global Legal Entity Identifier Foundation](https://www.gleif.org/) (LEI=Legal Entity Identifier).

Their complete dataset can be downloaded [here](https://www.gleif.org/en/lei-data/gleif-golden-copy/download-the-golden-copy#/). You should choose the CSV version.

The CSV files can be converted to Kafka events using the [csv_producer](./csv_producer/).

```bash
cd csv_producer
poetry run python csv_producer.py path/to/yyyymmdd-0000-gleif-goldencopy-lei2-golden-copy.csv ../build/gen/lei/v1/leidata_pb2:LeiData lei-data
poetry run python csv_producer.py path/to/yyyymmdd-0000-gleif-goldencopy-rr-golden-copy.csv ../build/gen/lei/v1/leirelationshipdata_pb2:LeiRelationshipData lei-relationship-data
```

## Task 3: Extracting information, schema transformation and integration

These steps are performed on a SQLite database of the data. This file is exported by the sqlite Kafka connect sink.

1. Schema transformation and some intgration

    ```bash
    sqlite3 path/to/corporate.sqlite <(cat transformations/*.sql)
    ```

2. Extract information from RB texts

    ```bash
    poetry run python rb_crawler/rb_parser.py --database data/corporate.sqlite
    ```
