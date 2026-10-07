-- Follows the documentation of MobilityDB for loading the vehicles data
-- https://docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03.html#idp8

-- However this script is slightly different to facilitate the benchmarking process:
-- * It is neccessary to pass a flag to this script to define
--   how the loading of the vehicles data shall happen
-- * The actual vehicle fleet data ships as datamcar.csv, not vehicles.csv.
-- * The id column is named Moid (not VehId as in the original documentation),
--   matching this project's convention used everywhere else (Trips.Moid,
--   Regions/Periods/Points ids) and the datamcar.csv header itself
--   (Moid,Licence,Type,Model). trips/base.sql and trips/optimized.sql's
--   FOREIGN KEY (Moid) REFERENCES Vehicles(Moid) depends on this name.

-- indicates whether to explicitly run ANALYZE after data loading
\if :{?analyze_table}
\else
  \echo '\n[ERROR] Missing flag: -v analyze_table=true|false\n'
  SELECT 'ERROR: analyze_table is required'::integer; \q
\endif

-- path (inside the container) to the datamcar.csv of the scale factor to load
\if :{?vehicles_csv}
\else
  \echo '\n[ERROR] Missing flag: -v vehicles_csv=/home/mobilitydb/BerlinMOD/<scale>/datamcar.csv\n'
  SELECT 'ERROR: vehicles_csv is required'::integer; \q
\endif

-- Boolean Type Validation (Server-side)
SELECT
  :analyze_table::boolean AS analyze_table_valid
WHERE false;

CREATE TABLE Vehicles (
  Moid integer PRIMARY KEY,
  Licence varchar(32),
  Type varchar(32),
  Model varchar(32)
);

COPY Vehicles(Moid, Licence, Type, Model)
FROM :'vehicles_csv' DELIMITER ',' CSV HEADER;

\if :analyze_table
  ANALYZE Vehicles;
\endif
