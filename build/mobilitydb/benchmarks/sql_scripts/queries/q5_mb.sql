-- Official BerlinMOD-MobilityDB Query 5 (docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html)
-- Minimum distance between vehicle pairs.
-- Adapted: VehId -> Moid. Joins Trips.Moid directly to Licences1/2.VehId,
-- exactly as documented (see q3_mb.sql for the VehId column note).
SELECT L1.Licence AS Licence1, L2.Licence AS Licence2,
  MIN(ST_Distance(trajectory(T1.Trip), trajectory(T2.Trip))) AS MinDist
FROM Trips T1, Licences1 L1, Trips T2, Licences2 L2
WHERE T1.Moid = L1.VehId AND T2.Moid = L2.VehId AND T1.Moid < T2.Moid
GROUP BY L1.Licence, L2.Licence
ORDER BY L1.Licence, L2.Licence;
