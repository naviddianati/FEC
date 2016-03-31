#! /bin/bash

source config.sh


# drop <state>_full tables
echo "Dropping <state>_full tables..."
echo "select concat('drop table ', table_name, ';') from information_schema.tables where table_name like '%_full';" |mysql  -u $USER --batch -p`cat $HOME/.config/pass` FEC | tail -n+2 > ./mysql/drop_intermediate_tables.sql



# drop <state>_addresses tables
echo "Dropping <state>_addresses tables..."
echo "select concat('drop table ', table_name, ';') from information_schema.tables where table_name like '%_addresses';" |mysql  -u $USER --batch -p`cat $HOME/.config/pass` FEC | tail -n+2 >> ./mysql/drop_intermediate_tables.sql


mysql  -u $USER --batch -p`cat $HOME/.config/pass` FEC < ./mysql/drop_intermediate_tables.sql 
