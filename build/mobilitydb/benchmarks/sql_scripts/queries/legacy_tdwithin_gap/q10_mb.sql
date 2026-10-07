-- Official BerlinMOD-MobilityDB Query 10 (docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html)
-- Records interactions between vehicle pairs within 3m proximity.
-- Adapted:
--  - VehId -> Moid. Joins Trips.Moid directly to Licences1/2.VehId, exactly
--    as documented (see q3_mb.sql for the VehId column note).
--  - atPeriodSet -> attime, atValue -> atvalues (renamed in 1.3.0).
--  - The docs' own published SQL for this query is broken as printed: a
--    missing AND before the final `dwithin(...)` line, and `dwithin` isn't
--    an actual MobilityDB function for two tgeompoint arguments (only
--    `tdwithin` is) - both fixed here using the same
--    `tdwithin(...) ?= true` pattern Query 6 already uses.
WITH Values AS (
  SELECT DISTINCT L1.Licence AS QueryLicence, L2.Licence AS OtherLicence,
    attime(T1.Trip, gettime(atvalues(tdwithin(T1.Trip, T2.Trip, 3.0), TRUE))) AS Pos
  FROM Trips T1, Licences1 L1, Trips T2, Licences2 L2
  WHERE T1.Moid = L1.VehId AND T2.Moid = L2.VehId AND T1.Moid < T2.Moid AND
    expandspace(T1.Trip, 3) && expandspace(T2.Trip, 3) AND
    tdwithin(T1.Trip, T2.Trip, 3.0) ?= true )
SELECT QueryLicence, OtherLicence, array_agg(Pos ORDER BY startTimestamp(Pos)) AS Pos
FROM Values
GROUP BY QueryLicence, OtherLicence
ORDER BY QueryLicence, OtherLicence;
