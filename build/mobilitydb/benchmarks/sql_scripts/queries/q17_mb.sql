-- Official BerlinMOD-MobilityDB Query 17 (docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html)
-- Most-visited points. Adapted: VehId -> Moid.
WITH PointCount AS (
  SELECT P.PointId, COUNT(DISTINCT T.Moid) AS Hits
  FROM Trips T, Points P
  WHERE ST_Intersects(trajectory(T.Trip), P.Geom)
  GROUP BY P.PointId )
SELECT PointId, Hits
FROM PointCount AS P
WHERE P.Hits = ( SELECT MAX(Hits) FROM PointCount );
