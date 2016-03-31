DROP TABLE IF EXISTS maryland_combined_v2;
CREATE TABLE maryland_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM maryland_full_v2 AS t2
                LEFT JOIN maryland_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON maryland_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON maryland_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS massachusetts_combined_v2;
CREATE TABLE massachusetts_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM massachusetts_full_v2 AS t2
                LEFT JOIN massachusetts_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON massachusetts_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON massachusetts_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS michigan_combined_v2;
CREATE TABLE michigan_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM michigan_full_v2 AS t2
                LEFT JOIN michigan_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON michigan_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON michigan_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS minnesota_combined_v2;
CREATE TABLE minnesota_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM minnesota_full_v2 AS t2
                LEFT JOIN minnesota_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON minnesota_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON minnesota_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS mississippi_combined_v2;
CREATE TABLE mississippi_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM mississippi_full_v2 AS t2
                LEFT JOIN mississippi_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON mississippi_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON mississippi_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS missouri_combined_v2;
CREATE TABLE missouri_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM missouri_full_v2 AS t2
                LEFT JOIN missouri_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON missouri_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON missouri_combined_v2 (id);
-- -------------------------------------
