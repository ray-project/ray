-- SQL used to create the test table in v6_trips_8i1u.zip
CREATE TABLE v6_trips_8i1u
(
    ts     BIGINT,
    uuid   STRING,
    rider  STRING,
    driver STRING,
    fare DOUBLE,
    city   STRING
) USING HUDI
PARTITIONED BY (city)
TBLPROPERTIES (
    type = 'cow',
    primaryKey = 'uuid',
    preCombineField = 'ts',
    'hoodie.metadata.enable' = 'false',
    'hoodie.parquet.small.file.limit' = '0'
);

INSERT INTO v6_trips_8i1u
VALUES (1695159649087, '334e26e9-8355-45cc-97c6-c31daf0df330', 'rider-A', 'driver-K', 19.10, 'san_francisco'),
       (1695091554788, 'e96c4396-3fad-413a-a942-4cb36106d721', 'rider-C', 'driver-M', 27.70, 'san_francisco'),
       (1695046462179, '9909a8b1-2d15-4d3d-8ec9-efc48c536a00', 'rider-D', 'driver-L', 33.90, 'san_francisco'),
       (1695332066204, '1dced545-862b-4ceb-8b43-d2a568f6616b', 'rider-E', 'driver-O', 93.50, 'san_francisco'),
       (1695516137016, 'e3cf430c-889d-4015-bc98-59bdce1e530c', 'rider-F', 'driver-P', 34.15, 'sao_paulo'),
       (1695376420876, '7a84095f-737f-40bc-b62f-6b69664712d2', 'rider-G', 'driver-Q', 43.40, 'sao_paulo'),
       (1695173887231, '3eeb61f7-c2b0-4636-99bd-5d7a5a1d2c04', 'rider-I', 'driver-S', 41.06, 'chennai'),
       (1695115999911, 'c8abbe79-8d89-47ea-b4ce-4d224bae5bfa', 'rider-J', 'driver-T', 17.85, 'chennai');

UPDATE v6_trips_8i1u
SET fare = 25.0
WHERE rider = 'rider-D';
