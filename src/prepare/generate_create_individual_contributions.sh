#! /bin/bash

# Generate SQL script to create individual_contributions_v<version> table

source config.sh
tablename=$MySQL_table_individual_contributions


code="CREATE DATABASE IF NOT EXISTS FEC;\n
USE FEC;\n

DROP TABLE IF EXISTS $tablename;\n
CREATE TABLE $tablename(\n
    CMTE_ID VARCHAR(9) COMMENT 'Filer Identification Number',\n
    AMNDT_IND VARCHAR(1) COMMENT 'Amendment Indicator',\n
    RPT_TP VARCHAR(3) COMMENT 'Report Type',\n
    TRANSACTION_PGI VARCHAR(5) COMMENT 'Primary-General Indicator',\n
    IMAGE_NUM VARCHAR(11) COMMENT 'Microfilm Location (YYOORRRFFFF)',\n
    TRANSACTION_TP VARCHAR(3) COMMENT 'Transaction Type',\n
    ENTITY_TP VARCHAR(3) COMMENT 'Entity Type',\n
    NAME VARCHAR(200) COMMENT 'Contributor/Lender/Transfer Name',\n
    CITY VARCHAR(30) COMMENT 'City/Town',\n
    STATE VARCHAR(2) COMMENT 'State',\n
    ZIP_CODE VARCHAR(9) COMMENT 'Zip Code',\n
    EMPLOYER VARCHAR(38) COMMENT 'Employer',\n
    OCCUPATION VARCHAR(38) COMMENT 'Occupation',\n
    TRANSACTION_DT VARCHAR(10) COMMENT 'Transaction Date(MMDDYYYY)',\n
    TRANSACTION_AMT FLOAT(14,2) COMMENT 'Transaction Amount',\n
    OTHER_ID VARCHAR(9) COMMENT 'Other Identification Number',\n
    TRAN_ID VARCHAR(32) COMMENT 'Transaction ID',\n
    FILE_NUM VARCHAR(22) COMMENT 'File Number / Report ID',\n
    MEMO_CD VARCHAR(1) COMMENT 'Memo Code',\n
    MEMO_TEXT VARCHAR(100) COMMENT 'Memo Text',\n
    SUB_ID VARCHAR(19) COMMENT 'FEC Record Number',\n
    \n
    KEY TRAN_INDEX (TRAN_ID,CMTE_ID)\n
);\n
"

echo -e $code > ./mysql/create_individual_contributions.sql

