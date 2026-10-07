-- Official BerlinMOD-MobilityDB Query 4 (docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html)
-- Vehicles passing points. Adapted: VehId -> Moid; `tgeompoint && geometry`
-- no longer exists in 1.3.0 (only tgeompoint && stbox/tstzspan/tgeompoint
-- do), so the point geometry is wrapped in stbox() for the bbox pre-filter.
SELECT DISTINCT P.PointId, P.Geom, C.Licence
FROM Trips T, Vehicles C, Points P
WHERE T.Moid = C.Moid AND T.Trip && stbox(P.Geom) AND
  ST_Intersects(trajectory(T.Trip), P.Geom)
ORDER BY P.PointId, C.Licence;
