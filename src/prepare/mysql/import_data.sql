LOAD DATA LOCAL INFILE '/nfs/home/navid/data//FEC-prepare/data/indiv00.txt' INTO TABLE individual_contributions_v2 FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 LOAD DATA LOCAL INFILE '/nfs/home/navid/data//FEC-prepare/data/indiv02.txt' INTO TABLE individual_contributions_v2 FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 LOAD DATA LOCAL INFILE '/nfs/home/navid/data//FEC-prepare/data/indiv04.txt' INTO TABLE individual_contributions_v2 FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 LOAD DATA LOCAL INFILE '/nfs/home/navid/data//FEC-prepare/data/indiv06.txt' INTO TABLE individual_contributions_v2 FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 LOAD DATA LOCAL INFILE '/nfs/home/navid/data//FEC-prepare/data/indiv08.txt' INTO TABLE individual_contributions_v2 FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 LOAD DATA LOCAL INFILE '/nfs/home/navid/data//FEC-prepare/data/indiv10.txt' INTO TABLE individual_contributions_v2 FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 LOAD DATA LOCAL INFILE '/nfs/home/navid/data//FEC-prepare/data/indiv12.txt' INTO TABLE individual_contributions_v2 FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 LOAD DATA LOCAL INFILE '/nfs/home/navid/data//FEC-prepare/data/indiv14.txt' INTO TABLE individual_contributions_v2 FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 LOAD DATA LOCAL INFILE '/nfs/home/navid/data//FEC-prepare/data/indiv16.txt' INTO TABLE individual_contributions_v2 FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 LOAD DATA LOCAL INFILE '/nfs/home/navid/data//FEC-prepare/data/indiv80.txt' INTO TABLE individual_contributions_v2 FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 LOAD DATA LOCAL INFILE '/nfs/home/navid/data//FEC-prepare/data/indiv82.txt' INTO TABLE individual_contributions_v2 FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 LOAD DATA LOCAL INFILE '/nfs/home/navid/data//FEC-prepare/data/indiv84.txt' INTO TABLE individual_contributions_v2 FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 LOAD DATA LOCAL INFILE '/nfs/home/navid/data//FEC-prepare/data/indiv86.txt' INTO TABLE individual_contributions_v2 FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 LOAD DATA LOCAL INFILE '/nfs/home/navid/data//FEC-prepare/data/indiv88.txt' INTO TABLE individual_contributions_v2 FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 LOAD DATA LOCAL INFILE '/nfs/home/navid/data//FEC-prepare/data/indiv90.txt' INTO TABLE individual_contributions_v2 FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 LOAD DATA LOCAL INFILE '/nfs/home/navid/data//FEC-prepare/data/indiv92.txt' INTO TABLE individual_contributions_v2 FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 LOAD DATA LOCAL INFILE '/nfs/home/navid/data//FEC-prepare/data/indiv94.txt' INTO TABLE individual_contributions_v2 FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 LOAD DATA LOCAL INFILE '/nfs/home/navid/data//FEC-prepare/data/indiv96.txt' INTO TABLE individual_contributions_v2 FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 LOAD DATA LOCAL INFILE '/nfs/home/navid/data//FEC-prepare/data/indiv98.txt' INTO TABLE individual_contributions_v2 FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 UPDATE individual_contributions_v2 set TRANSACTION_DT = str_to_date(TRANSACTION_DT,'%m%d%Y');
 ALTER TABLE individual_contributions_v2 MODIFY TRANSACTION_DT DATE;

 -- Add id column to individual_contributions_v2 
 ALTER TABLE individual_contributions_v2 add column id INT NOT NULL AUTO_INCREMENT FIRST, ADD primary KEY ID_INDEX(id);
