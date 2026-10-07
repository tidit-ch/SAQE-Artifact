-- Official BerlinMOD-MobilityDB Query 1 (docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html)
-- Vehicle models and licence plates.
SELECT DISTINCT L.Licence, C.Model AS Model
FROM Vehicles C, Licences L
WHERE C.Licence = L.Licence;
