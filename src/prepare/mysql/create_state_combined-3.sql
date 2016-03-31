DROP TABLE IF EXISTS iowa_combined_v2;
CREATE TABLE iowa_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM iowa_full_v2 AS t2
                LEFT JOIN iowa_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON iowa_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON iowa_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS kansas_combined_v2;
CREATE TABLE kansas_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM kansas_full_v2 AS t2
                LEFT JOIN kansas_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON kansas_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON kansas_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS kentucky_combined_v2;
CREATE TABLE kentucky_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM kentucky_full_v2 AS t2
                LEFT JOIN kentucky_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON kentucky_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON kentucky_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS louisiana_combined_v2;
CREATE TABLE louisiana_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM louisiana_full_v2 AS t2
                LEFT JOIN louisiana_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON louisiana_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON louisiana_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS maine_combined_v2;
CREATE TABLE maine_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM maine_full_v2 AS t2
                LEFT JOIN maine_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON maine_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON maine_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS marshallislands_combined_v2;
CREATE TABLE marshallislands_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM marshallislands_full_v2 AS t2
                LEFT JOIN marshallislands_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON marshallislands_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON marshallislands_combined_v2 (id);
-- -------------------------------------
