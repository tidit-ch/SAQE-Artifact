-- Official BerlinMOD-MobilityDB Query 15 (docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html)
-- Vehicles passing points during periods. Adapted: VehId -> Moid; atPeriod -> attime.
SELECT DISTINCT PO.PointId, PO.Geom, PR.PeriodId, PR.Period, C.Licence
FROM Trips T, Vehicles C, Points1 PO, Periods1 PR
WHERE T.Moid = C.Moid AND T.Trip && STBOX(PO.Geom, PR.Period) AND
  ST_Intersects(trajectory(attime(T.Trip, PR.Period)), PO.Geom)
ORDER BY PO.PointId, PR.PeriodId, C.Licence;
