DROP TABLE IF EXISTS alabama_combined_v2;
CREATE TABLE alabama_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM alabama_full_v2 AS t2
                LEFT JOIN alabama_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON alabama_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON alabama_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS alaska_combined_v2;
CREATE TABLE alaska_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM alaska_full_v2 AS t2
                LEFT JOIN alaska_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON alaska_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON alaska_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS americansamoa_combined_v2;
CREATE TABLE americansamoa_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM americansamoa_full_v2 AS t2
                LEFT JOIN americansamoa_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON americansamoa_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON americansamoa_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS arizona_combined_v2;
CREATE TABLE arizona_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM arizona_full_v2 AS t2
                LEFT JOIN arizona_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON arizona_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON arizona_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS arkansas_combined_v2;
CREATE TABLE arkansas_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM arkansas_full_v2 AS t2
                LEFT JOIN arkansas_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON arkansas_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON arkansas_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS california_combined_v2;
CREATE TABLE california_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM california_full_v2 AS t2
                LEFT JOIN california_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON california_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON california_combined_v2 (id);
-- -------------------------------------
