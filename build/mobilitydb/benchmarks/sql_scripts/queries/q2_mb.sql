-- Official BerlinMOD-MobilityDB Query 2 (docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch03s03.html)
-- Passenger car count.
SELECT COUNT (Licence)
FROM Vehicles C
WHERE Type = 'passenger';
