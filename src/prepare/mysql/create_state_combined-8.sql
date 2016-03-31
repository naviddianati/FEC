DROP TABLE IF EXISTS southdakota_combined_v2;
CREATE TABLE southdakota_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM southdakota_full_v2 AS t2
                LEFT JOIN southdakota_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON southdakota_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON southdakota_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS tennessee_combined_v2;
CREATE TABLE tennessee_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM tennessee_full_v2 AS t2
                LEFT JOIN tennessee_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON tennessee_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON tennessee_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS texas_combined_v2;
CREATE TABLE texas_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM texas_full_v2 AS t2
                LEFT JOIN texas_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON texas_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON texas_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS utah_combined_v2;
CREATE TABLE utah_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM utah_full_v2 AS t2
                LEFT JOIN utah_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON utah_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON utah_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS vermont_combined_v2;
CREATE TABLE vermont_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM vermont_full_v2 AS t2
                LEFT JOIN vermont_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON vermont_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON vermont_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS virginia_combined_v2;
CREATE TABLE virginia_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM virginia_full_v2 AS t2
                LEFT JOIN virginia_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON virginia_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON virginia_combined_v2 (id);
-- -------------------------------------
