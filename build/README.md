## To start the docker container:

Note: If you're using a Mac with an M1/M2/M3 chip (ARM architecture), 
replace the 'postgis/postgis:latest' image in the docker-compose.yml with 'imresamu/postgis', 
since the default image does not support ARM.

To run in macOS with M chips, use the following command: `docker compose -f docker-compose.dev.yml --profile arm64 up`
For other architectures, use: `docker compose -f docker-compose.dev.yml --profile default up`

docker-compose up

### Postgres + PostGIS

The password and username are specified in the `docker-compose.dev/prod.yml` file.

To populate the Postgres tables, use one of these query formats:

1. INSERT INTO postgres.berlinmod.<TABLE_NAME> SELECT * FROM csv.berlinmod.<TABLE_NAME>;
2. INSERT OR REPLACE INTO postgres.berlinmod.<TABLE_NAME> SELECT * FROM csv.berlinmod.<TABLE_NAME>;
3. INSERT OVERWRITE INTO postgres.berlinmod.<TABLE_NAME> SELECT * FROM csv.berlinmod.<TABLE_NAME>;

Example query to populate the Postgres `trips` table:

INSERT INTO postgres.berlinmod.trips SELECT * FROM csv.berlinmod.trips;

### InfluxDB3

Step one: Create a token

docker exec -it <CONTAINER> influxdb3 create token --admin

Step two: Create a database

docker exec -it influxdb influxdb3 create database [OPTIONS] <DATABASE_NAME>

To populate the Influx tables, use the following query format:

INSERT INTO influx_catalog.influx_schema.<TABLE_NAME> SELECT * FROM csv.berlinmod.<TABLE_NAME>;

Example query to populate the Influx `trips` table:

INSERT INTO influx.berlinmod.trips SELECT * FROM csv.berlinmod.trips;
