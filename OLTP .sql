use `IA_FINAL_PROJECT_DB`;
DROP TABLE IF EXISTS `traffic_tickets_source1`;
CREATE TABLE `traffic_tickets_source1` (
  `Violation Charged Code` varchar(100),
  `Violation Description` varchar(500),
  `Violation Year` int,
  `Violation Month` int,
  `Violation Day of Week` varchar(100),
  `Age at Violation` float,
  `Gender` varchar(100),
  `State of License` varchar(100),
  `Police Agency` varchar(100),
  `Court` varchar(100),
  `Source` varchar(100)
  );
  -- testing 1st data source:
  select * from `traffic_tickets_source1`;
  
  
-- creating 2nd table for data source #2:

DROP TABLE IF EXISTS `vehicle_crashes_source2`;
CREATE TABLE `vehicle_crashes_source2`(
  `Year` int,
  `Violation Description` varchar(500),
  `Violation Code` varchar(100),
  `Case Individual ID` int
  );
  
-- testing 2nd data source:
  select * from `vehicle_crashes_source2`;
  
LOAD DATA INFILE '/Users/sweetysaha/Downloads'
INTO TABLE mytable
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

  
  
  -- creating 3rd table for data source #3:
DROP TABLE IF EXISTS `vehicle_info_source3`;
CREATE TABLE `vehicle_info_source3`
(
year int,
`case vehicle id` int,
`vehicle body type` VARCHAR(200),
`registration class` VARCHAR(200),
`action prior to accident` VARCHAR(200),
`type / axles of truck or bus`VARCHAR(200), 
`direction of travel` VARCHAR(200),
`fuel type` VARCHAR(200),
`vehicle year` VARCHAR(200),
`state of registration` VARCHAR(200),
`number of occupants` VARCHAR(200),
`engine cylinders` VARCHAR(200),
`vehicle make` VARCHAR(200),
`contributing factor 1` VARCHAR(200),
`contributing factor 1 description` VARCHAR(200),
`contributing factor 2` VARCHAR(200),
`contributing factor 2 description` VARCHAR(200),
`event type` VARCHAR(200),
`partial vin` VARCHAR(200)
);

-- testing 3rd data source:
  select * from `vehicle_info_source3`;
  

-- creating 4th table for data source #4:
DROP TABLE IF EXISTS `individual_info_source4`;
CREATE TABLE `individual_info_source4`
(
year int,
`case individual id` int,
`case vehicle id` int,
`victim status` VARCHAR(200),
`role type` VARCHAR(200),
`seating position`VARCHAR(200), 
`ejection` VARCHAR(200),
`license state code` VARCHAR(200),
`sex` VARCHAR(200),
`transported by` VARCHAR(200),
`safety equipment` VARCHAR(200),
`injury descriptor` VARCHAR(200),
`injury location` VARCHAR(200),
`injury severity` VARCHAR(200),
`age` int
);

-- testing 4th data source:
select * from `individual_info_source4`;