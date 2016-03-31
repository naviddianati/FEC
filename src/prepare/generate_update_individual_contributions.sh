#! /bin/bash

# Generate SQL code to alter individual_contributions_v<version>
source config.sh

tablename=$MySQL_table_individual_contributions
code="-- Add id column to $tablename\n
ALTER TABLE $tablename add column id INT NOT NULL AUTO_INCREMENT FIRST, ADD primary KEY ID_INDEX(id);\n"

echo -e $code > ./mysql/update_individual_contributions.sql
