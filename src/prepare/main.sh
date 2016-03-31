#! /bin/bash

# Be sure to set the same version both in config.sh and in config.py
# of the disambiguation package. Both are used when generating the
# various scripts used here.



source config.sh




# Download and extract datasets for different time periods from FEC
#./dl-FEC-databases

# Import downloaded data into MySQL database
#./import-FEC-databases.sh

# Run Python script that generate some SQL scripts.
cd ./mysql/generators
./generate_state_table_create_scripts.py
./generate_usa_combined_script.py
cd ../..

# Download raw .fec files to extract contributor addresses
#./dl-raw-files.sh


# TODO: run main.py from FEC-raw-records to generate a sql dump 
# from the raw .fec files.

# TODO: write script to import the resulting sql dump to MySQL
#echo "Importing addresses into table contributor_addresses..."
#mysql -u $USER -p FEC < addresses-Mon-Oct-13-02:03:44-2014.sql 


# TODO: write script to update the contributor_addresses table by adding index
#echo "Adding index to contributor_addresses..."
#mysql -u $USER -p FEC < ./mysql/prepare-contributor_addresses.sql


# Truncate contributor_addresses so that it only contains records that have
# a corresponding (TRAN_ID,CMTE_ID) pair in individual_contributions.
# The result is an UPDATED contributor_addresses.
#echo "Truncating table contributor_addresses..."
#mysql -u $USER -p FEC < ./mysql/truncate_contributor_addresses.sql




# Create two new tables, subsets of contributor_addresses which contain records
# with unique, and  "identifiable" id pairs. This is necessary so we can join
# these address tables with individual_contributions without linking records
# with ambiguous id pairs.
# The important output is the table contributor_addresses_identifiable
#echo "Generating table contributor_addresses_identifiable..."
#mysql -u $USER -p`cat $HOME/.config/pass` FEC < ./mysql/create_contributor_addresses_unique_and_identifiable.sql 





# Here, the final product are the <state>_combined tables.
# To generate these tables, we need three intermediate tables:
#   <state>: records with unique (and non-empty) id pairs. Used to generate <state>_addresses
#   <state>_addresses: join between <state> and contributor_addresses_identifiable
#   <state>_full:  join between <state>_addresses and individual_contributions 
#   <state>_combined: join between <state>_full and <state>_addresses
# The script for generating <state>_combined is split into multiple pieces so 
# that they may be run in parallel
echo "Generating tables <state> and <state>_addresses..."
mysql -u $USER -p`cat $HOME/.config/pass` FEC <./mysql/create_state_addresses_tables.sql

echo "Generating tables <state>_full..."
mysql -u $USER -p`cat $HOME/.config/pass` FEC <./mysql/create_state_full_tables.sql

echo "Generating tables <state>_combined..."
for file in ./mysql/create_state_combined*.sql
do
   sed -r "s/combined/combined_v$FEC_version/g" $file | mysql -u $USER -p`cat $HOME/.config/pass` FEC 
done 


echo "Generating table usa_combined"
mysql -u $USER -p`cat $HOME/.config/pass` FEC < ./mysql/generate_usa_combined.sql



# drop intermediate tables
./drop_intermediate_tables.sh



# Download candidate/committee/linkage data files
#./dl-cadidate-data.sh

# Import candidate/committee/linkage data files into MySQL tables
#./import-candidate-data.sh



