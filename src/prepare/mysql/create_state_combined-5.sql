DROP TABLE IF EXISTS montana_combined_v2;
CREATE TABLE montana_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM montana_full_v2 AS t2
                LEFT JOIN montana_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON montana_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON montana_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS nebraska_combined_v2;
CREATE TABLE nebraska_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM nebraska_full_v2 AS t2
                LEFT JOIN nebraska_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON nebraska_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON nebraska_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS nevada_combined_v2;
CREATE TABLE nevada_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM nevada_full_v2 AS t2
                LEFT JOIN nevada_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON nevada_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON nevada_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS newhampshire_combined_v2;
CREATE TABLE newhampshire_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM newhampshire_full_v2 AS t2
                LEFT JOIN newhampshire_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON newhampshire_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON newhampshire_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS newjersey_combined_v2;
CREATE TABLE newjersey_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM newjersey_full_v2 AS t2
                LEFT JOIN newjersey_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON newjersey_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON newjersey_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS newmexico_combined_v2;
CREATE TABLE newmexico_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM newmexico_full_v2 AS t2
                LEFT JOIN newmexico_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON newmexico_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON newmexico_combined_v2 (id);
-- -------------------------------------
