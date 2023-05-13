CREATE DATABASE  IF NOT EXISTS `dmv_dw1`;
USE `dmv_dw1`;

-- dim table#1 for Ticket violation:
DROP TABLE IF EXISTS `dim_ticket_violation`;

CREATE TABLE `dim_ticket_violation` (
  `ticket_key` int NOT NULL AUTO_INCREMENT,
  `Violation Charged Code` varchar(100),
  `Violation Description` varchar(500),
  `Violation Day of Week` varchar(100),
  `Violation Month` int(11),
  `Age at Violation` float,
  `Gender` varchar(100),
  `State of License` varchar(100),
  `Police Agency` varchar(100),
  `Court` varchar(100),
  PRIMARY KEY( `ticket_key`)
);


-- checking the query for table#1:
select * from dmv_dw.dim_ticket_violation;


-- dim table#2 for Motor Vehicle Crash violation information:
DROP TABLE IF EXISTS `dim_mtr_crash_violation`;

CREATE TABLE `dim_mtr_crash_violation` (
  `violation_key` int NOT NULL AUTO_INCREMENT,
  `Case Individual ID` int(11),
  `Crash Violation Description` varchar(500),
  `Violation Code` varchar(100),
  PRIMARY KEY(`violation_key`)
);





-- checking the query for table#2:
select * from dim_mtr_crash_violation


-- dim table#3 for Motor Vehicle Crash individual information:
DROP TABLE IF EXISTS `dim_mtr_crash_individual`;

CREATE TABLE `dim_mtr_crash_individual` (
  `individual_key` int NOT NULL AUTO_INCREMENT,
  `case individual id` int(11),
  `case vehicle id` int(11),
  `victim status` varchar(200),
  `role type` varchar(200),
  `seating position` varchar(200),
  `ejection` varchar(200),
  `sex` varchar(200),
  `transported by` varchar(200),
  `safety equipment` varchar(200),
  `injury descriptor` varchar(200),
  `injury location` varchar(200),
  `injury severity` varchar(200),
  PRIMARY KEY(`individual_key`)
);





-- checking the query for table#3:
select * from dmv_dw.dim_mtr_crash_individual;



-- dim table#4 for Motor Vehicle Crash vehicle information:
DROP TABLE IF EXISTS `dim_mtr_crash_vehicle`;

CREATE TABLE `dim_mtr_crash_vehicle` (
  `vehicle_key` int NOT NULL AUTO_INCREMENT,
  `case vehicle id` int(11),
  `vehicle body type` varchar(200),
  `registration class` varchar(200),
  `action prior to accident` varchar(200),
  `type / axles of truck or bus` varchar(200),
  `direction of travel` varchar(200),
  `fuel type` varchar(200),
  `vehicle year` varchar(200),
  `number of occupants` varchar(200),
  `engine cylinders` varchar(200),
  `vehicle make` varchar(200),
  `contributing factor 1` varchar(200),
  `contributing factor 1 description` varchar(200),
  `contributing factor 2` varchar(200),
  `contributing factor 2 description` varchar(200),
  `event type` varchar(200),
  PRIMARY KEY(`vehicle_key`)
);





-- checking the query for table#4:
select * from dmv_dw.dim_mtr_crash_vehicle;


-- creating 5th (fact table):

DROP TABLE IF EXISTS `fact_dmv`;

CREATE TABLE `fact_dmv`(
`ticket_key` int,
`violation_key` int, 
`individual_key` int,
`vehicle_key` int,
`count_ticket_violations` int,
`count_motor_violations` int,
FOREIGN KEY (`ticket_key`) REFERENCES `dim_ticket_violation`(`ticket_key`),
FOREIGN KEY (`violation_key`) REFERENCES `dim_mtr_crash_violation`(`violation_key`),
FOREIGN KEY (`individual_key`) REFERENCES `dim_mtr_crash_individual`(`individual_key`),
FOREIGN KEY (`vehicle_key`) REFERENCES `dim_mtr_crash_vehicle`(`vehicle_key`)
);

DELIMITER //
CREATE PROCEDURE etl_oltp_olap_prc()
BEGIN
   -- ETL for table#1:

insert into dmv_dw.dim_ticket_violation(`Violation Charged Code`,`Violation Description`,`Violation Day of Week`,
`Violation Month`,`Age at Violation`,`Gender`,`State of License`,`Police Agency`,`Court`)
(SELECT DISTINCT `Violation Charged Code`,`Violation Description`,`Violation Day of Week`,
`Violation Month`,`Age at Violation`,`Gender`,`State of License`,`Police Agency`,`Court` FROM IA_FINAL_PROJECT_DB.traffic_tickets_source1
);
-- ETL for table#2:

insert into dmv_dw.dim_mtr_crash_violation(`Case Individual ID`,`Crash Violation Description`,`Violation Code`)
(SELECT DISTINCT `Case Individual ID`,`Violation Description`,`Violation Code` FROM IA_FINAL_PROJECT_DB.vehicle_crashes_source2
);
-- ETL for table#3:

insert into dmv_dw.dim_mtr_crash_individual(`case individual id`,`case vehicle id`,`victim status`,`role type`,`seating position`,
`ejection`,`sex`,`transported by`,`safety equipment`,`injury descriptor`,`injury location`,
`injury severity`)
(SELECT DISTINCT `case individual id`,`case vehicle id`,`victim status`,`role type`,`seating position`,
`ejection`,`sex`,`transported by`,`safety equipment`,`injury descriptor`,`injury location`,
`injury severity` FROM IA_FINAL_PROJECT_DB.individual_info_source4
);

-- ETL for table#4:

insert into dmv_dw.dim_mtr_crash_vehicle(`case vehicle id`,`vehicle body type`,`registration class`,`action prior to accident`,
`type / axles of truck or bus`,`direction of travel`,`fuel type`,`vehicle year`,`number of occupants`,
`engine cylinders`,`vehicle make`,`contributing factor 1`,`contributing factor 1 description`,
`contributing factor 2`,`contributing factor 2 description`,`event type`)
(SELECT DISTINCT `case vehicle id`,`vehicle body type`,`registration class`,`action prior to accident`,
`type / axles of truck or bus`,`direction of travel`,`fuel type`,`vehicle year`,`number of occupants`,
`engine cylinders`,`vehicle make`,`contributing factor 1`,`contributing factor 1 description`,
`contributing factor 2`,`contributing factor 2 description`,`event type`
FROM IA_FINAL_PROJECT_DB.vehicle_info_source3
);


insert into dmv_dw.fact_dmv(ticket_key,violation_key,individual_key,vehicle_key,count_ticket_violations,count_motor_violations)
(select 
distinct d1.ticket_key, 
d2.violation_key, 
d3.individual_key, 
d4.vehicle_key,
count(d1.`Violation Charged Code`) as count_ticket_violations,
count(d2.`Violation Code`) as count_motor_violations
from dmv_dw.dim_ticket_violation as d1,
dmv_dw.dim_mtr_crash_violation as d2,
dmv_dw.dim_mtr_crash_individual as d3,
dmv_dw.dim_mtr_crash_vehicle as d4
where d1.`Violation Charged Code`= d2.`Violation Code`
and d2.`Case Individual ID` = d3.`case individual id`
and d3.`case vehicle id` = d4.`case vehicle id`
group by d1.ticket_key, 
d2.violation_key, 
d3.individual_key, 
d4.vehicle_key);
END //
DELIMITER ;
call etl_oltp_olap_prc();