DROP TABLE IF EXISTS georgia_combined_v2;
CREATE TABLE georgia_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM georgia_full_v2 AS t2
                LEFT JOIN georgia_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON georgia_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON georgia_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS guam_combined_v2;
CREATE TABLE guam_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM guam_full_v2 AS t2
                LEFT JOIN guam_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON guam_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON guam_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS hawaii_combined_v2;
CREATE TABLE hawaii_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM hawaii_full_v2 AS t2
                LEFT JOIN hawaii_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON hawaii_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON hawaii_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS idaho_combined_v2;
CREATE TABLE idaho_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM idaho_full_v2 AS t2
                LEFT JOIN idaho_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON idaho_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON idaho_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS illinois_combined_v2;
CREATE TABLE illinois_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM illinois_full_v2 AS t2
                LEFT JOIN illinois_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON illinois_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON illinois_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS indiana_combined_v2;
CREATE TABLE indiana_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM indiana_full_v2 AS t2
                LEFT JOIN indiana_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON indiana_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON indiana_combined_v2 (id);
-- -------------------------------------
