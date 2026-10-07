-- Official BerlinMOD-MobilityDB Query 11 (docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html)
-- Vehicles at point-instant combinations. Adapted: VehId -> Moid.
SELECT P.PointId, P.Geom, I.InstantId, I.Instant, C.Licence
FROM Trips T, Vehicles C, Points1 P, Instants1 I
WHERE T.Moid = C.Moid AND T.Trip @> STBOX(P.Geom, I.Instant) AND
  valueAtTimestamp(T.Trip, I.Instant) = P.Geom
ORDER BY P.PointId, I.InstantId, C.Licence;
