#! /bin/bash

export USER='navid'
export FEC_version='2'

export URL_ROOT='/nfs/home/navid/data/'
echo "Data directory: $URL_ROOT"

export URL_WORKING="$URL_ROOT/FEC-prepare"
export URL_DLS="$URL_WORKING/downloads/"
export URL_DATA="$URL_WORKING/data/"
export URL_TMP="$URL_WORKING/tmp/"
mkdir -p $URL_WORKING/FEC-raw-reports
mkdir -p $URL_DLS
mkdir -p $URL_DATA
mkdir -p $URL_TMP


export MySQL_table_individual_contributions=individual_contributions_v$FEC_version

# Table containing all records for given state, 
# including the street address mined separately
# NOTE: THIS STRING IS A TEMPLATE. IT MUST BE INSTANTIATED
# WITH A STATE NAME STRING
export MySQL_table_state_combined=%%s_combined_v$FEC_version

# All <state>_combined tables concatenated.
export MySQL_table_usa_combined=usa_combined_v$FEC_version

# Identities table. This table maps each record id to
# an identity id.
export MySQL_table_identities=identities_v$FEC_version

# Identities adjacency table.
export MySQL_table_identities_adjacency=identities_adjacency_v$FEC_version




