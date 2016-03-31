DROP TABLE IF EXISTS virginislands_combined_v2;
CREATE TABLE virginislands_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM virginislands_full_v2 AS t2
                LEFT JOIN virginislands_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON virginislands_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON virginislands_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS washington_combined_v2;
CREATE TABLE washington_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM washington_full_v2 AS t2
                LEFT JOIN washington_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON washington_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON washington_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS westvirginia_combined_v2;
CREATE TABLE westvirginia_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM westvirginia_full_v2 AS t2
                LEFT JOIN westvirginia_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON westvirginia_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON westvirginia_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS wisconsin_combined_v2;
CREATE TABLE wisconsin_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM wisconsin_full_v2 AS t2
                LEFT JOIN wisconsin_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON wisconsin_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON wisconsin_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS wyoming_combined_v2;
CREATE TABLE wyoming_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM wyoming_full_v2 AS t2
                LEFT JOIN wyoming_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON wyoming_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON wyoming_combined_v2 (id);
-- -------------------------------------
