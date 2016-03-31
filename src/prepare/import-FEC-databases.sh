#! /bin/bash

source config.sh


# Create FEC database and tables. Mainly, individual_contributions
echo "Creating database FEC and MySQL table FEC:$MySQL_table_individual_contributions..."
./generate_create_individual_contributions.sh
mysql -u $USER -p  < ./mysql/create_individual_contributions.sql

# Import all datasets into MySQL table individual_contributors
echo "Importing datasets into MySQL table FEC:$MySQL_table_individual_contributions..."

# Create sql script for importing all tables
# Loads eact csv file into table, then updates dates into correct format
SCRIPT=""
for FILE in ${URL_DATA}indiv*.txt
do
    SCRIPT="$SCRIPT  
    LOAD DATA LOCAL INFILE '$FILE' INTO TABLE $MySQL_table_individual_contributions   FIELDS TERMINATED BY '|'    LINES TERMINATED BY '\\\n';\n"
done
    SCRIPT="$SCRIPT  
    UPDATE $MySQL_table_individual_contributions set TRANSACTION_DT = str_to_date(TRANSACTION_DT,'%m%d%Y');\n
    ALTER TABLE $MySQL_table_individual_contributions MODIFY TRANSACTION_DT DATE;\n\n
    -- Add id column to $MySQL_table_individual_contributions \n
    ALTER TABLE $MySQL_table_individual_contributions add column id INT NOT NULL AUTO_INCREMENT FIRST, ADD primary KEY ID_INDEX(id);"


echo -e  $SCRIPT > ./mysql/import_data.sql
mysql -u $USER -p --local-infile=1 FEC < ./mysql/import_data.sql
# At this point, individual_contributions has the correct date format, has a multiple index "TRAN_INDEX from TRAN_ID and CMTE_ID, and it has a primary integer id "id"




