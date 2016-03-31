-- This script truncates the individual_addresses tables down to only those records whose TRAN_ID,CMTE_ID pair also appears in individual_contributions_v2
 
 DROP TABLE IF EXISTS tmp;
 DROP TABLE IF EXISTS contributor_addresses_truncated;
 CREATE TABLE contributor_addresses_truncated like contributor_addresses;
 
 
 -- This makes it work!!!
 SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED ;
 
 
 INSERT INTO contributor_addresses_truncated 
 SELECT contributor_addresses.*
 FROM contributor_addresses
 JOIN individual_contributions_v2
 USING (TRAN_ID,CMTE_ID);
 
 COMMIT;
 
 
 -- Added later. Now I want to just delete the old contributor_addresses and replace it with the truncated version
 DROP TABLE contributor_addresses;
 RENAME TABLE contributor_addresses_truncated to contributor_addresses;

