#! /bin/bash

# Backup the current version of the tables to ./backup. The backed up tables are
# individual_contributions usa_combined identities identities_adjacency 

source config.sh
mkdir -p ./backup

for tablename_template in individual_contributions usa_combined identities identities_adjacency
    do
    tablename=${tablename_template}_v${FEC_version}
    echo "Dumping MySQL table $tablename..."
    mysqldump -u $USER -p`cat $HOME/.config/pass` FEC $tablename > ./backup/dump_$tablename.sql
    done
