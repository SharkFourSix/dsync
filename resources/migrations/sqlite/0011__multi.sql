CREATE TABLE if NOT EXISTS animals(id integer primary key AUTOINCREMENT, someCode int, animalName TEXT not null);

CREATE INDEX IF NOT EXISTS animalCode ON animals( someCode );

INSERT INTO animals(someCode, animalName)
VALUES 
 (20, "Sparrow") 
,(21, "Bald Eagle")
,(22, "Red Kite")
,(23, "Vulture")
;