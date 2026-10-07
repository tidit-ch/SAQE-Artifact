-- Official BerlinMOD-MobilityDB Query 9 (docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html)
-- Maximum distance travelled per period, over all vehicles.
-- Adapted: VehId -> Moid; atPeriod -> attime (renamed in 1.3.0).
WITH Distances AS (
  SELECT P.PeriodId, P.Period, T.Moid, SUM(length(attime(T.Trip, P.Period))) AS Dist
  FROM Trips T, Periods P
  WHERE T.Trip && P.Period
  GROUP BY P.PeriodId, P.Period, T.Moid )
SELECT PeriodId, Period, MAX(Dist) AS MaxDist
FROM Distances
GROUP BY PeriodId, Period
ORDER BY PeriodId;
