CREATE TABLE if NOT EXISTS animals(id int primary key, someCode int, animalName varchar(100) not null);

CREATE INDEX IF NOT EXISTS animalCode ON animals( someCode );