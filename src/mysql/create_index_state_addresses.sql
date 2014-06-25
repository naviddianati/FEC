-- This script is a patch to fix old <state>_address tables. New ones are already created with the index
-- I ended up regenerating all <state>_addresses tables from scratch, so didn't need to run this script.

create index TRAN_INDEX on newyork_addresses (TRAN_ID,CMTE_ID);
create index TRAN_INDEX on massachusetts_addresses (TRAN_ID,CMTE_ID);
create index TRAN_INDEX on alaska_addresses (TRAN_ID,CMTE_ID);
create index TRAN_INDEX on vermont_addresses (TRAN_ID,CMTE_ID);
create index TRAN_INDEX on nevada_addresses (TRAN_ID,CMTE_ID);
create index TRAN_INDEX on ohio_addresses (TRAN_ID,CMTE_ID);
create index TRAN_INDEX on missouri_addresses (TRAN_ID,CMTE_ID);

