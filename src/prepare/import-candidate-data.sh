#! /bin/bash

source config.sh


# Create FEC database and candidate/committee tables. 
mysql -u $USER -p --local-infile=1 FEC < ./mysql/create_candidate_committee_tables.sql

# Import all datasets into MySQL table individual_contributors
echo "Importing datasets into MySQL tables FEC:candidate_master, committee_master, candidate_to_committee_linkage..."

####################################################################################################
# Loads each csv file into table, then updates dates into correct format
# Candidate master files
####################################################################################################
SCRIPT=""
for FILE in cn*.txt
do
    SCRIPT="$SCRIPT  
    LOAD DATA LOCAL INFILE '$FILE' INTO TABLE candidate_master  FIELDS TERMINATED BY '|'    LINES TERMINATED BY '\\\n';\n"
done

SCRIPT="$SCRIPT  
-- Add id column to candidate_master\n
ALTER TABLE candidate_master add column id INT NOT NULL AUTO_INCREMENT FIRST, ADD primary KEY ID_INDEX(id);"

echo -e  $SCRIPT > ./mysql/import_candidate_data.sql


####################################################################################################
# Loads each csv file into table, then updates dates into correct format
# candidate_committee linkage  files
####################################################################################################
SCRIPT=""
for FILE in ccl*.txt
do
    SCRIPT="$SCRIPT  
    LOAD DATA LOCAL INFILE '$FILE' INTO TABLE candidate_to_committee_linkage  FIELDS TERMINATED BY '|'    LINES TERMINATED BY '\\\n';\n"
done

SCRIPT="$SCRIPT  
-- Add id column to candidate_to_committee_linkage\n
ALTER TABLE candidate_to_committee_linkage add column id INT NOT NULL AUTO_INCREMENT FIRST, ADD primary KEY ID_INDEX(id);"

echo -e  $SCRIPT >> ./mysql/import_candidate_data.sql


####################################################################################################
# Loads each csv file into table, then updates dates into correct format
# committee master files
####################################################################################################
SCRIPT=""
for FILE in cm*.txt
do
    SCRIPT="$SCRIPT  
    LOAD DATA LOCAL INFILE '$FILE' INTO TABLE committee_master  FIELDS TERMINATED BY '|'    LINES TERMINATED BY '\\\n';\n"
done

SCRIPT="$SCRIPT  
-- Add id column to committee_master\n
ALTER TABLE committee_master add column id INT NOT NULL AUTO_INCREMENT FIRST, ADD primary KEY ID_INDEX(id);"

echo -e  $SCRIPT >> ./mysql/import_candidate_data.sql



####################################################################################################
# Loads each csv file into table, then updates dates into correct format
# contributions to candidates files
####################################################################################################
SCRIPT=""
for FILE in pas*.txt
do
    SCRIPT="$SCRIPT  
    LOAD DATA LOCAL INFILE '$FILE' INTO TABLE contributions_to_candidates  FIELDS TERMINATED BY '|'    LINES TERMINATED BY '\\\n';\n"
done

SCRIPT="$SCRIPT  
UPDATE contributions_to_candidates set TRANSACTION_DT = str_to_date(TRANSACTION_DT,'%m%d%Y');\n
ALTER TABLE contributions_to_candidates MODIFY TRANSACTION_DT DATE;\n\n
-- Add id column to contributions_to_candidates\n
ALTER TABLE contributions_to_candidates add column id INT NOT NULL AUTO_INCREMENT FIRST, ADD primary KEY ID_INDEX(id);"

echo -e  $SCRIPT >> ./mysql/import_candidate_data.sql








mysql -u $USER -p --local-infile=1 FEC < ./mysql/import_candidate_data.sql
# At this point, individual_contributions has the correct date format, has a multiple index "TRAN_INDEX from TRAN_ID and CMTE_ID, and it has a primary integer id "id"



