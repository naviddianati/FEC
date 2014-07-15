-- Tables:
--     A) individual_contributions
--     B) contributor_addresses

-- A1 <-- Get those in A which have identifiable identifiers (individual_contributions_identifiable)
-- B1 <-- Get those in B which have identifiable identifiers (contributor_addresses_identifiable) (Done in another script: create_contributor_addresses_unique_and_identifiable.sql)

-- C  <-- Join A1,B1 on (TRAN_ID,CMTE_ID)
--     C is all the rows that have an identifiable address, together with their addresses. (country_addresses)

-- D  <-- Left Join A, C on (TRAN_ID,CMTE_ID)
--     All rows of A; those that are also in C, have their addresses as well. (country_combined)






USE FEC;


-- Create dummy table and insert (TRAN_ID,CMTE_ID) of all entries in individual contributions with unique key
-- This gives a list of record keys in individual_contributions that we want to find addresses for.
-- TODO: Modify to include non-unique ones with identical data fields as well.






/*  non-unique TRAN_ID,CMTE_ID pairs shared by multiple records of individual_contributions that are identical in important fields.
    Note that the same (TRAN_ID,CMTE_ID) can be repeated for the same person X, and repeated for several different X's. These must
    also be discarded. Hence "HAVING c1 = 1 "
*/

DROP TABLE IF EXISTS identical_duplicates_deleteme;
CREATE TABLE identical_duplicates_deleteme
    (UNIQUE KEY TRAN_INDEX (TRAN_ID,CMTE_ID))
    SELECT TRAN_ID,CMTE_ID
        FROM (
            -- Filter out duplicate id pairs that are shared by more than one person.
            SELECT TRAN_ID,CMTE_ID,count(*) as c1
                FROM(
                    SELECT *,count(*) AS c 
                        FROM individual_contributions 
                        WHERE (TRAN_ID !='' AND CMTE_ID != '') 
                        GROUP BY CMTE_ID,TRAN_ID,NAME,CITY,STATE,ZIP_CODE,EMPLOYER,OCCUPATION,ENTITY_TP 
                        HAVING c>1
                    ) as t
                GROUP BY TRAN_ID,CMTE_ID
                HAVING c1=1
            ) as t1


/*Set of all TRAN_ID,CMTE_ID pairs in individual contributors that are "identifiable", 
  that is, they either have a unique (TRAN_ID,CMTE_ID) pair, or else, all rows sharing the
  same pair have identical other fields and therefore clearly belong to the same person.
*/

DROP TABLE IF EXISTS identifiables_deleteme; 
CREATE TABLE identifiables_deleteme 
    (UNIQUE KEY TRAN_INDEX(TRAN_ID,CMTE_ID)) 
    SELECT TRAN_ID,CMTE_ID 
        FROM (
            -- unique TRAN_ID,CMTE_ID pairs
            SELECT TRAN_ID,CMTE_ID,count(*) AS c  
                FROM individual_contributions  
                WHERE (TRAN_ID !='' AND CMTE_ID != '') 
                GROUP BY TRAN_ID,CMTE_ID  
                HAVING c=1) as t

    UNION
        -- identical duplicate pairs
        identical_duplicates_deleteme;



-- TODO: join this with individual_contributions to get individual_contriutions_identifiable



-- create table which is a subset of the individual_contributions table such that it only contains entries that have a unique key.
-- This is the national version of the <state> tables. 
-- Note that the <state> tables are "cleaned up": They have non-empty and unique TRAN_INDEX.

DROP TABLE IF EXISTS individual_contributions_unique;
CREATE TABLE individual_contributions_unique 
    (UNIQUE KEY TRAN_INDEX(TRAN_ID,CMTE_ID)) 
    SELECT * FROM individual_contributions 
        JOIN uniques_deleteme
        USING (TRAN_ID,CMTE_ID);



-- I believe this table can be left joined with contributor_addresses_identifiable( another script) to give the final answer
DROP TABLE IF EXISTS individual_contributions_identifiable;
CREATE TABLE individual_contributions_identifiable 
    (UNIQUE KEY TRAN_INDEX(TRAN_ID,CMTE_ID)) 
    SELECT * FROM individual_contributions 
        JOIN identifiables_deleteme
        USING (TRAN_ID,CMTE_ID);


-- Different in logic from <state>_addresses only in that it contains not only unique id paris, but all "identifiable" id pairs
-- This table can be used for extracting affiliation networks in the same way as <state>_addresses tables.
DROP TABLE IF EXISTS country_addresses;
CREATE TABLE country_addresses 
    (UNIQUE KEY TRAN_INDEX(TRAN_ID,CMTE_ID)) 

    SELECT * FROM individual_contributions_unique AS t1  
        JOIN contributor_addresses_unique AS t2 
        USING (CMTE_ID,TRAN_ID);


--
DROP TABLE IF EXISTS country_combined;
CREATE TABLE country_combined
    (UNIQUE KEY TRAN_INDEX (TRAN_ID,CMTE_ID))
    SELECT * FROM individual_contributions
        LEFT JOIN country_addresses;


DROP TABLE uniques_deleteme;


*/
