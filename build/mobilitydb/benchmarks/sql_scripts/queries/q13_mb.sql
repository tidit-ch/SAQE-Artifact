-- Official BerlinMOD-MobilityDB Query 13 (docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html)
-- Vehicles in regions during periods. Matches the paper's Query 13, same
-- as this project's older custom q4.sql - kept alongside it as the
-- docs-numbered version. Adapted: VehId -> Moid; atPeriod -> attime.
SELECT DISTINCT R.RegionId, P.PeriodId, P.Period, C.Licence
FROM Trips T, Vehicles C, Regions1 R, Periods1 P
WHERE T.Moid = C.Moid AND T.trip && STBOX(R.Geom, P.Period) AND
  ST_Intersects(trajectory(attime(T.Trip, P.Period)), R.Geom)
ORDER BY R.RegionId, P.PeriodId, C.Licence;
