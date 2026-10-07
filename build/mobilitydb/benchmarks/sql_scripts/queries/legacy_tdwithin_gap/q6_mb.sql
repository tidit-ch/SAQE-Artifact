-- Official BerlinMOD-MobilityDB Query 6 (docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html)
-- Closely spaced truck pairs (within 10m at the same time).
-- Adapted: VehId -> Moid; expandSpatial -> expandspace (renamed in 1.3.0).
SELECT DISTINCT C1.Licence AS Licence1, C2.Licence AS Licence2
FROM Trips T1, Vehicles C1, Trips T2, Vehicles C2
WHERE T1.Moid = C1.Moid AND T2.Moid = C2.Moid AND
  T1.Moid < T2.Moid AND C1.Type = 'truck' AND C2.Type = 'truck' AND
  T1.Trip && expandspace(T2.Trip, 10) AND
  tdwithin(T1.Trip, T2.Trip, 10.0) ?= true
ORDER BY C1.Licence, C2.Licence;
