DROP TABLE IF EXISTS washington_combined;
-- ---------------------------------------
CREATE TABLE washington_combined LIKE washington_addresses;
INSERT INTO washington_combined
        SELECT *
            FROM washington as t1
                LEFT JOIN 
                       (select TRAN_ID,CMTE_ID,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
                         from washington_addresses ) as t2
                USING (CMTE_ID,TRAN_ID);

