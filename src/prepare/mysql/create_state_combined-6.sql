DROP TABLE IF EXISTS newyork_combined_v2;
CREATE TABLE newyork_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM newyork_full_v2 AS t2
                LEFT JOIN newyork_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON newyork_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON newyork_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS northcarolina_combined_v2;
CREATE TABLE northcarolina_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM northcarolina_full_v2 AS t2
                LEFT JOIN northcarolina_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON northcarolina_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON northcarolina_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS northdakota_combined_v2;
CREATE TABLE northdakota_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM northdakota_full_v2 AS t2
                LEFT JOIN northdakota_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON northdakota_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON northdakota_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS northernmarianaislands_combined_v2;
CREATE TABLE northernmarianaislands_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM northernmarianaislands_full_v2 AS t2
                LEFT JOIN northernmarianaislands_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON northernmarianaislands_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON northernmarianaislands_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS ohio_combined_v2;
CREATE TABLE ohio_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM ohio_full_v2 AS t2
                LEFT JOIN ohio_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON ohio_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON ohio_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS oklahoma_combined_v2;
CREATE TABLE oklahoma_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM oklahoma_full_v2 AS t2
                LEFT JOIN oklahoma_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON oklahoma_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON oklahoma_combined_v2 (id);
-- -------------------------------------
