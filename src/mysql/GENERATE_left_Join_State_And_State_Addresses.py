from states import *


#state = "alaska"
f = open("left_Join_State_And_State_Addresses.sql",'w')


for code,state in dict_state.iteritems():
        query=("DROP TABLE IF EXISTS %s_combined;\n"
            "-- ---------------------------------------\n"
         "CREATE TABLE %s_combined LIKE %s_addresses;\n"
         "INSERT INTO %s_combined\n"
         "        SELECT *\n"
         "            FROM %s as t1\n"
         "                LEFT JOIN \n"
         "                       (select TRAN_ID,CMTE_ID,FIRST_NAME,LAST_NAME,FILE_NUMBER,CONTRIBUTOR_CITY, CONTRIBUTOR_STATE,CONTRIBUTOR_ZIP,CONTRIBUTOR_STREET_1,CONTRIBUTOR_STREET_2\n"
         "                         from %s_addresses ) as t2\n"
         "                USING (CMTE_ID,TRAN_ID);\n\n") % (state,state,state,state,state,state)
        print query

        f.write(query)
        break
f.close()


