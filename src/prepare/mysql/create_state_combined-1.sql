DROP TABLE IF EXISTS colorado_combined_v2;
CREATE TABLE colorado_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM colorado_full_v2 AS t2
                LEFT JOIN colorado_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON colorado_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON colorado_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS connecticut_combined_v2;
CREATE TABLE connecticut_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM connecticut_full_v2 AS t2
                LEFT JOIN connecticut_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON connecticut_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON connecticut_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS delaware_combined_v2;
CREATE TABLE delaware_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM delaware_full_v2 AS t2
                LEFT JOIN delaware_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON delaware_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON delaware_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS districtofcolumbia_combined_v2;
CREATE TABLE districtofcolumbia_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM districtofcolumbia_full_v2 AS t2
                LEFT JOIN districtofcolumbia_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON districtofcolumbia_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON districtofcolumbia_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS federatedstatesofmicronesia_combined_v2;
CREATE TABLE federatedstatesofmicronesia_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM federatedstatesofmicronesia_full_v2 AS t2
                LEFT JOIN federatedstatesofmicronesia_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON federatedstatesofmicronesia_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON federatedstatesofmicronesia_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS florida_combined_v2;
CREATE TABLE florida_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM florida_full_v2 AS t2
                LEFT JOIN florida_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON florida_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON florida_combined_v2 (id);
-- -------------------------------------
