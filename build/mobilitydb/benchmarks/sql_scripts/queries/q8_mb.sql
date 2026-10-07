-- Official BerlinMOD-MobilityDB Query 8 (docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html)
-- Travelled distances by vehicle and period.
-- Adapted: VehId -> Moid; atPeriod -> attime (renamed in 1.3.0). Joins
-- Trips.Moid directly to Licences1.VehId, exactly as documented (see
-- q3_mb.sql for the VehId column note).
SELECT L.Licence, P.PeriodId, P.Period, SUM(length(attime(T.Trip, P.Period))) AS Dist
FROM Trips T, Licences1 L, Periods1 P
WHERE T.Moid = L.VehId AND T.Trip && P.Period
GROUP BY L.Licence, P.PeriodId, P.Period
ORDER BY L.Licence, P.PeriodId;
