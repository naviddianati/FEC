DROP TABLE IF EXISTS oregon_combined_v2;
CREATE TABLE oregon_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM oregon_full_v2 AS t2
                LEFT JOIN oregon_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON oregon_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON oregon_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS palau_combined_v2;
CREATE TABLE palau_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM palau_full_v2 AS t2
                LEFT JOIN palau_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON palau_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON palau_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS pennsylvania_combined_v2;
CREATE TABLE pennsylvania_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM pennsylvania_full_v2 AS t2
                LEFT JOIN pennsylvania_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON pennsylvania_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON pennsylvania_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS puertorico_combined_v2;
CREATE TABLE puertorico_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM puertorico_full_v2 AS t2
                LEFT JOIN puertorico_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON puertorico_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON puertorico_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS rhodeisland_combined_v2;
CREATE TABLE rhodeisland_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM rhodeisland_full_v2 AS t2
                LEFT JOIN rhodeisland_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON rhodeisland_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON rhodeisland_combined_v2 (id);
-- -------------------------------------
DROP TABLE IF EXISTS southcarolina_combined_v2;
CREATE TABLE southcarolina_combined_v2
        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2
            FROM southcarolina_full_v2 AS t2
                LEFT JOIN southcarolina_addresses_v2 AS t3
                USING (TRAN_ID,CMTE_ID)
            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   
CREATE INDEX  TRAN_INDEX ON southcarolina_combined_v2 (TRAN_ID,CMTE_ID);
CREATE INDEX  ID_INDEX ON southcarolina_combined_v2 (id);
-- -------------------------------------
