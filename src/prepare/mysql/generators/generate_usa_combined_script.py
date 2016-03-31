#! /usr/bin/python

'''
Generate MySQL script to combine all <state>_combined tables
into usa_combined
'''

from disambiguation.core.states import dict_state
from disambiguation import config

mysql_filename = "../generate_usa_combined.sql"
list_states = sorted(dict_state.values())
tablename = config.MySQL_table_usa_combined
tablename_newyork_combined = config.MySQL_table_state_combined % "newyork"

with open(mysql_filename,'w') as f:
    f.write( "DROP TABLE IF EXISTS %s;\n" % tablename)
    f.write("CREATE TABLE %s LIKE %s;\n" % (tablename,tablename_newyork_combined))
    for state in list_states:
        tablename_state_combined = config.MySQL_table_state_combined % state
        f.write("INSERT INTO %s (SELECT * FROM %s);\n" % (tablename, tablename_state_combined))
