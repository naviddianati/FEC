CREATE DATABASE IF NOT EXISTS FEC;

DROP TABLE IF EXISTS candidate_master ; 
CREATE TABLE candidate_master (
    CAND_ID VARCHAR(9) COMMENT "Candidate Identification",
    CAND_NAME VARCHAR(200) COMMENT "Candidate Name",
    CAND_PTY_AFFILIATION VARCHAR(3) COMMENT "Party Affiliation",
    CAND_ELECTION_YR VARCHAR(4) COMMENT "Year of Election",
    CAND_OFFICE_ST VARCHAR(2) COMMENT "Candidate State",
    CAND_OFFICE VARCHAR(1) COMMENT "Candidate Office",
    CAND_OFFICE_DISTRICT VARCHAR(2) COMMENT "Candidate District",
    CAND_ICI VARCHAR(1) COMMENT "Incumbent Challenger Status",
    CAND_STATUS VARCHAR(1) COMMENT "Candidate Status",
    CAND_PCC VARCHAR(9) COMMENT "Principal Campaign Committee",
    CAND_ST1 VARCHAR(34) COMMENT "Mailing Address - Street",
    CAND_ST2 VARCHAR(34) COMMENT "Mailing Address - Street2",
    CAND_CITY VARCHAR(30) COMMENT "Mailing Address - City",
    CAND_ST VARCHAR(2) COMMENT "Mailing Address - State",
    CAND_ZIP VARCHAR(9) COMMENT "Mailing Address - Zip Code");
DROP TABLE IF EXISTS candidate_to_committee_linkage ; 
CREATE TABLE candidate_to_committee_linkage (
    CAND_ID VARCHAR(9) COMMENT "Candidate Identification",
    CAND_ELECTION_YR VARCHAR(4) COMMENT "Candidate Election Year",
    FEC_ELECTION_YR VARCHAR(4) COMMENT "FEC Election Year",
    CMTE_ID VARCHAR(9) COMMENT "Committee Identification",
    CMTE_TP VARCHAR(1) COMMENT "Committee Type",
    CMTE_DSGN VARCHAR(1) COMMENT "Committee Designation",
    LINKAGE_ID VARCHAR(12) COMMENT "Linkage ID");
DROP TABLE IF EXISTS committee_master ; 
CREATE TABLE committee_master (
    CMTE_ID VARCHAR(9) COMMENT "Committee Identification",
    CMTE_NM VARCHAR(200) COMMENT "Committee Name",
    TRES_NM VARCHAR(90) COMMENT "Treasurer's Name",
    CMTE_ST1 VARCHAR(34) COMMENT "Street One",
    CMTE_ST2 VARCHAR(34) COMMENT "Street Two",
    CMTE_CITY VARCHAR(30) COMMENT "City or Town",
    CMTE_ST VARCHAR(2) COMMENT "State",
    CMTE_ZIP VARCHAR(9) COMMENT "Zip Code",
    CMTE_DSGN VARCHAR(1) COMMENT "Committee Designation",
    CMTE_TP VARCHAR(1) COMMENT "Committee Type",
    CMTE_PTY_AFFILIATION VARCHAR(3) COMMENT "Committee Party",
    CMTE_FILING_FREQ VARCHAR(1) COMMENT "Filing Frequency",
    ORG_TP VARCHAR(1) COMMENT "Interest Group Category",
    CONNECTED_ORG_NM VARCHAR(200) COMMENT "Connected Organization's Name",
    CAND_ID VARCHAR(9) COMMENT "Candidate Identification");
DROP TABLE IF EXISTS contributions_to_candidates ; 
CREATE TABLE contributions_to_candidates (
    CMTE_ID VARCHAR(9) COMMENT "Filer Identification Number",
    AMNDT_IND VARCHAR(1) COMMENT "Amendment Indicator",
    RPT_TP VARCHAR(3) COMMENT "Report Type",
    TRANSACTION_PGI VARCHAR(5) COMMENT "Primary-General Indicator",
    IMAGE_NUM VARCHAR(11) COMMENT "Microfilm Location (YYOORRRFFFF)",
    TRANSACTION_TP VARCHAR(3) COMMENT "Transaction Type",
    ENTITY_TP VARCHAR(3) COMMENT "Entity Type",
    NAME VARCHAR(200) COMMENT "Contributor/Lender/Transfer Name",
    CITY VARCHAR(30) COMMENT "City/Town",
    STATE VARCHAR(2) COMMENT "State",
    ZIP_CODE VARCHAR(9) COMMENT "Zip Code",
    EMPLOYER VARCHAR(38) COMMENT "Employer",
    OCCUPATION VARCHAR(38) COMMENT "Occupation",
    TRANSACTION_DT VARCHAR(10) COMMENT "Transaction Date(MMDDYYYY)",
    TRANSACTION_AMT FLOAT(14,2) COMMENT "Transaction Amount",
    OTHER_ID VARCHAR(9) COMMENT "Other Identification Number",
    CAND_ID VARCHAR(9) COMMENT "Candidate Identification Number",
    TRAN_ID VARCHAR(32) COMMENT "Transaction ID",
    FILE_NUM VARCHAR(22) COMMENT "File Number / Report ID",
    MEMO_CD VARCHAR(1) COMMENT "Memo Code",
    MEMO_TEXT VARCHAR(100) COMMENT "Memo Text",
    SUB_ID VARCHAR(19) COMMENT "FEC Record Number");
