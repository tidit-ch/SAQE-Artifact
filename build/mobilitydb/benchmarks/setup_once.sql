-- Run ONCE per database, before any benchmark trial. Not timed —
-- extension creation is a one-time cost amortized across every dataset
-- ever loaded, not part of "time to insight" for a single dataset.
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS mobilitydb CASCADE;
