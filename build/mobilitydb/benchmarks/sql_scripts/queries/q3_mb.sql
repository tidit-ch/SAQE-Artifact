-- Official BerlinMOD-MobilityDB Query 3 (docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html)
-- Vehicle positions at specific times.
-- Adapted: VehId -> Moid. Joins Trips.Moid directly to Licences1.VehId,
-- exactly as documented (Licences.VehId is populated at load time - see
-- load_all_documented.sql/load_all_partitioned.sql's Licences section).
SELECT DISTINCT L.Licence, I.InstantId, I.Instant AS Instant,
  valueAtTimestamp(T.Trip, I.Instant) AS Pos
FROM Trips T, Licences1 L, Instants1 I
WHERE T.Moid = L.VehId AND valueAtTimestamp(T.Trip, I.Instant) IS NOT NULL
ORDER BY L.Licence, I.InstantId;
