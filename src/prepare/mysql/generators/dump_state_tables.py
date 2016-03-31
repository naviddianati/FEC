#! /usr/bin/python
from disambiguation.core import utils
import os
# this script directly dumps state tables by running mysqldump

'''dict_states = {
                "alaska":"AK",
                "delaware":"DE",
                "missouri":"MO",
                "nevada":"NV",
                "newyork":"NY",
                "ohio":"OH",
                "massachussetes":"MA",
                "vermont":"VT"}

'''


def dump_states_full():
    for code,state in utils.states.dict_state.iteritems():
        name = state+"_full"
        command = "./dump_state_tables.sh %s %s"%(name,name+"_dump.sql")
        print command
        os.system(command)

def dump_states_combined():
    for code,state in utils.states.dict_state.iteritems():
        name = utils.config.MySQL_table_state_combined % state
        command = "./dump_state_tables.sh %s %s"%(name,name+"_dump.sql")
        print command
        os.system(command)

def dump_states_addresses():
    for code,state in utils.states.dict_state.iteritems():
        name = state+"_addresses"
        command = "./dump_state_tables.sh %s %s"%(name,name+"_dump.sql")
        print command
        os.system(command)


def dump_states():
    for code,state in utils.states.dict_state.iteritems():
        name = state
        command = "./dump_state_tables.sh %s %s"%(name,name+"_dump.sql")
        print command
        os.system(command)




#dump_states()
#dump_states_full()
dump_states_combined()
