-- Official BerlinMOD-MobilityDB Query 7 (docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html)
-- First passenger cars reaching each point.
-- Adapted: VehId -> Moid; atValue -> atvalues (renamed in 1.3.0);
-- `tgeompoint && geometry` no longer exists in 1.3.0, so P.Geom is wrapped
-- in stbox() for the bbox pre-filter (see q4_mb.sql).
WITH Timestamps AS (
  SELECT DISTINCT C.Licence, P.PointId, P.Geom,
    MIN(startTimestamp(atvalues(T.Trip,P.Geom))) AS Instant
  FROM Trips T, Vehicles C, Points1 P
  WHERE T.Moid = C.Moid AND C.Type = 'passenger' AND
    T.Trip && stbox(P.Geom) AND ST_Intersects(trajectory(T.Trip), P.Geom)
  GROUP BY C.Licence, P.PointId, P.Geom )
SELECT T1.Licence, T1.PointId, T1.Geom, T1.Instant
FROM Timestamps T1
WHERE T1.Instant <= ALL (
  SELECT T2.Instant
  FROM Timestamps T2
  WHERE T1.PointId = T2.PointId )
ORDER BY T1.PointId, T1.Licence;
