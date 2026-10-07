-- Official BerlinMOD-MobilityDB Query 16 (docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html)
-- Vehicle pairs sharing region presence within a period without ever
-- being simultaneously present (no temporal overlap).
-- Adapted: VehId -> Moid; atPeriod -> attime. Joins Trips.Moid directly to
-- Licences1/2.VehId, exactly as documented (see q3_mb.sql for the VehId
-- column note).
SELECT P.PeriodId, P.Period, R.RegionId, L1.Licence AS Licence1, L2.Licence AS Licence2
FROM Trips T1, Licences1 L1, Trips T2, Licences2 L2, Periods1 P, Regions1 R
WHERE T1.Moid = L1.VehId AND T2.Moid = L2.VehId AND L1.Licence < L2.Licence AND
  T1.Trip && STBOX(R.Geom, P.Period) AND T2.Trip && STBOX(R.Geom, P.Period) AND
  ST_Intersects(trajectory(attime(T1.Trip, P.Period)), R.Geom) AND
  ST_Intersects(trajectory(attime(T2.Trip, P.Period)), R.Geom) AND
  tintersects(attime(T1.Trip, P.Period), attime(T2.Trip, P.Period)) %= FALSE
ORDER BY PeriodId, RegionId, Licence1, Licence2;
