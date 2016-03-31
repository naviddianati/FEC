LOAD DATA LOCAL INFILE 'cn04.txt' INTO TABLE candidate_master FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 LOAD DATA LOCAL INFILE 'cn06.txt' INTO TABLE candidate_master FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 LOAD DATA LOCAL INFILE 'cn08.txt' INTO TABLE candidate_master FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 LOAD DATA LOCAL INFILE 'cn10.txt' INTO TABLE candidate_master FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 LOAD DATA LOCAL INFILE 'cn12.txt' INTO TABLE candidate_master FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 LOAD DATA LOCAL INFILE 'cn14.txt' INTO TABLE candidate_master FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 -- Add id column to candidate_master
 ALTER TABLE candidate_master add column id INT NOT NULL AUTO_INCREMENT FIRST, ADD primary KEY ID_INDEX(id);
LOAD DATA LOCAL INFILE 'ccl04.txt' INTO TABLE candidate_to_committee_linkage FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 LOAD DATA LOCAL INFILE 'ccl06.txt' INTO TABLE candidate_to_committee_linkage FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 LOAD DATA LOCAL INFILE 'ccl08.txt' INTO TABLE candidate_to_committee_linkage FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 LOAD DATA LOCAL INFILE 'ccl10.txt' INTO TABLE candidate_to_committee_linkage FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 LOAD DATA LOCAL INFILE 'ccl12.txt' INTO TABLE candidate_to_committee_linkage FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 LOAD DATA LOCAL INFILE 'ccl14.txt' INTO TABLE candidate_to_committee_linkage FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 -- Add id column to candidate_to_committee_linkage
 ALTER TABLE candidate_to_committee_linkage add column id INT NOT NULL AUTO_INCREMENT FIRST, ADD primary KEY ID_INDEX(id);
LOAD DATA LOCAL INFILE 'cm04.txt' INTO TABLE committee_master FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 LOAD DATA LOCAL INFILE 'cm06.txt' INTO TABLE committee_master FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 LOAD DATA LOCAL INFILE 'cm08.txt' INTO TABLE committee_master FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 LOAD DATA LOCAL INFILE 'cm10.txt' INTO TABLE committee_master FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 LOAD DATA LOCAL INFILE 'cm12.txt' INTO TABLE committee_master FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 LOAD DATA LOCAL INFILE 'cm14.txt' INTO TABLE committee_master FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 -- Add id column to committee_master
 ALTER TABLE committee_master add column id INT NOT NULL AUTO_INCREMENT FIRST, ADD primary KEY ID_INDEX(id);
LOAD DATA LOCAL INFILE 'pas204.txt' INTO TABLE contributions_to_candidates FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 LOAD DATA LOCAL INFILE 'pas206.txt' INTO TABLE contributions_to_candidates FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 LOAD DATA LOCAL INFILE 'pas208.txt' INTO TABLE contributions_to_candidates FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 LOAD DATA LOCAL INFILE 'pas210.txt' INTO TABLE contributions_to_candidates FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 LOAD DATA LOCAL INFILE 'pas212.txt' INTO TABLE contributions_to_candidates FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 LOAD DATA LOCAL INFILE 'pas214.txt' INTO TABLE contributions_to_candidates FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
 UPDATE contributions_to_candidates set TRANSACTION_DT = str_to_date(TRANSACTION_DT,'%m%d%Y');
 ALTER TABLE contributions_to_candidates MODIFY TRANSACTION_DT DATE;

 -- Add id column to contributions_to_candidates
 ALTER TABLE contributions_to_candidates add column id INT NOT NULL AUTO_INCREMENT FIRST, ADD primary KEY ID_INDEX(id);
