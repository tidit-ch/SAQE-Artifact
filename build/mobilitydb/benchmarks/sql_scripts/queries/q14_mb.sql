-- Official BerlinMOD-MobilityDB Query 14 (docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html)
-- Vehicles in regions at instants. Adapted: VehId -> Moid.
SELECT DISTINCT R.RegionId, I.InstantId, I.Instant, C.Licence
FROM Trips T, Vehicles C, Regions1 R, Instants1 I
WHERE T.Moid = C.Moid AND T.Trip && STBOX(R.Geom, I.Instant) AND
  ST_Contains(R.Geom, valueAtTimestamp(T.Trip, I.Instant))
ORDER BY R.RegionId, I.InstantId, C.Licence;
