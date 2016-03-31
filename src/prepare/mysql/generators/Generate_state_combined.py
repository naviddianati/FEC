#! /usr/bin/python


from states import *
from disambiguation.core import utils


def get_query_without_id(i):
    '''
    Generate the combined tables, but assume they already have an id column.
    However, the index on id doesn't exist and must be created.
    '''
    state = list_states[i]
    tablename_full = utils.config.MySQL_table_state_full % state
    tablename_addresses = utils.config.MySQL_table_state_addresses % state
    tablename_combined = utils.config.MySQL_table_state_combined % state

    query = ("DROP TABLE IF EXISTS %s;\n"
    "CREATE TABLE %s\n"
    "        SELECT t2.*,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2\n"
    "            FROM %s AS t2\n"
    "                LEFT JOIN %s AS t3\n"
    "                USING (TRAN_ID,CMTE_ID)\n"
    "            ORDER BY NAME,TRANSACTION_DT,ZIP_CODE;                                   \n"
    "CREATE INDEX  TRAN_INDEX ON %s (TRAN_ID,CMTE_ID);\n" 
    "CREATE INDEX  ID_INDEX ON %s (id);\n-- -------------------------------------\n")% (tablename_combined,tablename_combined,tablename_full,tablename_addresses,tablename_combined,tablename_combined)

    return query


def get_query_index(i):
    state = list_states[i]
    #query = ( "ALTER TABLE %s_combined add column id INT NOT NULL AUTO_INCREMENT FIRST, ADD primary KEY ID_INDEX(id);\n"
    #"CREATE INDEX  TRAN_INDEX ON %s_combined (TRAN_ID,CMTE_ID);\n-- -------------------------------------\n")% (state,state)

    query = ("ALTER TABLE %s_combined add column id INT NOT NULL AUTO_INCREMENT FIRST, ADD primary KEY ID_INDEX(id);\n"
              "-- -------------------------------------\n")% (state)


    return query





list_states = sorted(dict_state.values())



N =  len(list_states)



for ind in range(10):
    query = ''
    query_index = ''
    for i in range(ind*6,(ind+1)*6):
        print i
        try:
            query += get_query_without_id(i)
            query_index += get_query_index(i)
        except IndexError:
            print "state no %d not found." % i


    f = open("../create_state_combined-%d.sql"%ind,'w')
    f.write(query)
    f.close()



