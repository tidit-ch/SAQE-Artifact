-- Builds the *1/*2 sample views the official MobilityDB-BerlinMOD docs
-- query set (Q3, Q5, Q7, Q8, Q10-Q16) run against, rather than the full
-- Query* tables. Docs use SAMPLESIZE=100 -> 10-row samples; mirrors SAQE's
-- own sample views (benches/csv_benchmarks/mod.rs), which use the same
-- LIMIT 10 / LIMIT 10 OFFSET 10 pattern, so both sides sample identically.
--
-- Assumes Regions/Periods/Points/Instants/Licences are already loaded.
-- Pure view creation only - no data loading, no flags (nothing optional
-- here to gate behind a mandatory flag, unlike the load_data/* scripts).

DROP VIEW IF EXISTS Licences1;
DROP VIEW IF EXISTS Licences2;
DROP VIEW IF EXISTS Points1;
DROP VIEW IF EXISTS Regions1;
DROP VIEW IF EXISTS Instants1;
DROP VIEW IF EXISTS Periods1;

CREATE VIEW Licences1 AS SELECT * FROM Licences LIMIT 10;
CREATE VIEW Licences2 AS SELECT * FROM Licences LIMIT 10 OFFSET 10;
CREATE VIEW Points1 AS SELECT * FROM Points LIMIT 10;
CREATE VIEW Regions1 AS SELECT * FROM Regions LIMIT 10;
CREATE VIEW Instants1 AS SELECT * FROM Instants LIMIT 10;
CREATE VIEW Periods1 AS SELECT * FROM Periods LIMIT 10;
