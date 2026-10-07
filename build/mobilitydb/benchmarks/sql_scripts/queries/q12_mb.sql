-- Official BerlinMOD-MobilityDB Query 12 (docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html)
-- Vehicle pairs meeting at exact point-instant. Adapted: VehId -> Moid.
SELECT DISTINCT P.PointId, P.Geom, I.InstantId, I.Instant,
  C1.Licence AS Licence1, C2.Licence AS Licence2
FROM Trips T1, Vehicles C1, Trips T2, Vehicles C2, Points1 P, Instants1 I
WHERE T1.Moid = C1.Moid AND T2.Moid = C2.Moid AND T1.Moid < T2.Moid AND
  T1.Trip @> STBOX(P.Geom, I.Instant) AND T2.Trip @> STBOX(P.Geom, I.Instant) AND
  valueAtTimestamp(T1.Trip, I.Instant) = P.Geom AND
  valueAtTimestamp(T2.Trip, I.Instant) = P.Geom
ORDER BY P.PointId, I.InstantId, C1.Licence, C2.Licence;
