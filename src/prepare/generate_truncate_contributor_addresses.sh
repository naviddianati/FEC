#! /bin/bash

# Generate the SQL script that truncates contributor_addresses

source config.sh
tablename=$MySQL_table_individual_contributions
code="-- This script truncates the individual_addresses tables down to only those records whose TRAN_ID,CMTE_ID pair also appears in $tablename\n
\n
DROP TABLE IF EXISTS tmp;\n
DROP TABLE IF EXISTS contributor_addresses_truncated;\n
CREATE TABLE contributor_addresses_truncated like contributor_addresses;\n
\n
\n
-- This makes it work!!!\n
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED ;\n
\n
\n
INSERT INTO contributor_addresses_truncated \n
    SELECT contributor_addresses.*\n
        FROM contributor_addresses\n
            JOIN $tablename\n
            USING (TRAN_ID,CMTE_ID);\n
\n
COMMIT;\n
\n
\n
-- Added later. Now I want to just delete the old contributor_addresses and replace it with the truncated version\n
DROP TABLE contributor_addresses;\n
RENAME TABLE contributor_addresses_truncated to contributor_addresses;\n
"

echo -e $code > ./mysql/truncate_contributor_addresses.sql
