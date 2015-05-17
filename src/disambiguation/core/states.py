# This module contains data on US states
'''
This module contains data on US states, and methods that retrieve data
on US states.

@var dict_state: a dict with uppercase state abbreviations as keys, and lower
case state names as values.
@var dict_state_abbr: a dict with lower-case state names as keys and upper-
case state names as valus.

'''

import pandas as pd
import os
from .. import  data


my_data_path = os.path.join(os.path.dirname(__file__), '')

dict_state = {
    "AL": "alabama",
    "AK": "alaska",
    "AS": "americansamoa",
    "AZ": "arizona",
    "AR": "arkansas",
    "CA": "california",
    "CO": "colorado",
    "CT": "connecticut",
    "DE": "delaware",
    "DC": "districtofcolumbia",
    "FM": "federatedstatesofmicronesia",
    "FL": "florida",
    "GA": "georgia",
    "GU": "guam",
    "HI": "hawaii",
    "ID": "idaho",
    "IL": "illinois",
    "IN": "indiana",
    "IA": "iowa",
    "KS": "kansas",
    "KY": "kentucky",
    "LA": "louisiana",
    "ME": "maine",
    "MH": "marshallislands",
    "MD": "maryland",
    "MA": "massachusetts",
    "MI": "michigan",
    "MN": "minnesota",
    "MS": "mississippi",
    "MO": "missouri",
    "MT": "montana",
    "NE": "nebraska",
    "NV": "nevada",
    "NH": "newhampshire",
    "NJ": "newjersey",
    "NM": "newmexico",
    "NY": "newyork",
    "NC": "northcarolina",
    "ND": "northdakota",
    "MP": "northernmarianaislands",
    "OH": "ohio",
    "OK": "oklahoma",
    "OR": "oregon",
    "PW": "palau",
    "PA": "pennsylvania",
    "PR": "puertorico",
    "RI": "rhodeisland",
    "SC": "southcarolina",
    "SD": "southdakota",
    "TN": "tennessee",
    "TX": "texas",
    "UT": "utah",
    "VT": "vermont",
    "VI": "virginislands",
    "VA": "virginia",
    "WA": "washington",
    "WV": "westvirginia",
    "WI": "wisconsin",
    "WY": "wyoming"
}







dict_state_abbr = {
    "alabama"                       :    "AL",
    "alaska"                        :    "AK",
    "americansamoa"                 :    "AS",
    "arizona"                       :    "AZ",
    "arkansas"                      :    "AR",
    "california"                    :    "CA",
    "colorado"                      :    "CO",
    "connecticut"                   :    "CT",
    "delaware"                      :    "DE",
    "districtofcolumbia"            :    "DC",
    "federatedstatesofmicronesia"   :    "FM",
    "florida"                       :    "FL",
    "georgia"                       :    "GA",
    "guam"                          :    "GU",
    "hawaii"                        :    "HI",
    "idaho"                         :    "ID",
    "illinois"                      :    "IL",
    "indiana"                       :    "IN",
    "iowa"                          :    "IA",
    "kansas"                        :    "KS",
    "kentucky"                      :    "KY",
    "louisiana"                     :    "LA",
    "maine"                         :    "ME",
    "marshallislands"               :    "MH",
    "maryland"                      :    "MD",
    "massachusetts"                 :    "MA",
    "michigan"                      :    "MI",
    "minnesota"                     :    "MN",
    "mississippi"                   :    "MS",
    "missouri"                      :    "MO",
    "montana"                       :    "MT",
    "nebraska"                      :    "NE",
    "nevada"                        :    "NV",
    "newhampshire"                  :    "NH",
    "newjersey"                     :    "NJ",
    "newmexico"                     :    "NM",
    "newyork"                       :    "NY",
    "northcarolina"                 :    "NC",
    "northdakota"                   :    "ND",
    "northernmarianaislands"        :    "MP",
    "ohio"                          :    "OH",
    "oklahoma"                      :    "OK",
    "oregon"                        :    "OR",
    "palau"                         :    "PW",
    "pennsylvania"                  :    "PA",
    "puertorico"                    :    "PR",
    "rhodeisland"                   :    "RI",
    "southcarolina"                 :    "SC",
    "southdakota"                   :    "SD",
    "tennessee"                     :    "TN",
    "texas"                         :    "TX",
    "utah"                          :    "UT",
    "vermont"                       :    "VT",
    "virginislands"                 :    "VI",
    "virginia"                      :    "VA",
    "washington"                    :    "WA",
    "westvirginia"                  :    "WV",
    "wisconsin"                     :    "WI",
    "wyoming"                       :    "WY"
}



def get_state_order(criterion='num_records'):
    '''
    Return a dictionary {state: order} where order is an
    integer ranking the state based on population. Lower
    order means higher population.

    @param criterion: can be 'num_records' or 'population'. If the former, use
    get_states_record_numbers. Otherwise use population data.
    '''
    dict_state_order = None
    if criterion == 'population':
        dict_state_order = __get_state_order_population()

    elif criterion == 'num_records':
        dict_state_order = __get_state_order_record_number()

    else:
        raise Exception('Error: in get_state_order(), criterion must be either "num_records" or "population"')

    return dict_state_order





def __get_state_order_population():
    '''
    Return a dictionary C{{state: order}} based on state
    population. Lower order means higher population.
    '''
    populations = pd.read_csv(os.path.join(my_data_path, 'us-population.csv'))
    data = populations[["NAME", "POPESTIMATE2014"]].sort("POPESTIMATE2014")["NAME"].tolist()
    dict_state_order = {s.lower().replace(" ", ""):i for i, s in enumerate(data[::-1])}
    tmp = {}
    for state in dict_state.values():
        try:
            tmp[state] = dict_state_order[state]
        except:
            tmp[state] = 100

    dict_state_order = tmp
    return dict_state_order


def __get_state_order_record_number():
    '''
    Return a dictionary C{{state: order}} based on number
    records. Lower order means higher number of records.
    '''
    dict_states_record_numbers = get_states_record_numbers()
    tmp = sorted(dict_state.values(), key=lambda state:dict_states_record_numbers[state], reverse=True)
    return {state:order for order,state in enumerate(tmp)}




def get_states_sorted(criterion='num_records'):
    '''
    Return a list of states ordered by the state population
    or number of records. Lower index means higher population.

    @param criterion: can be 'num_records' or 'population'. If the former, use
    L{get_states_record_numbers}. Otherwise use population data.

    '''
    dict_state_order = get_state_order(criterion)
    list_states = sorted(dict_state.values())
    list_states.sort(key=lambda state: dict_state_order[state])
    print list_states
    return list_states




import Database
import json
def get_states_record_numbers(recompute=False):
    '''
    From the database, get the number of records for
    each state.

    @return: dict {state: number of records}.
    '''

    # Attempt to load from file.
    dict_state_record_numbers = {}
    if not recompute:
        try:
            with open(data.DICT_PATH_DATAFILES['states_record_numbers.json']) as f:
                dict_state_record_numbers = json.load(f)
                return dict_state_record_numbers
        except:
            pass
    print "computing number of records for each state from database..."

    # If unsuccessful or if recompute = True, recompute.
    db = Database.DatabaseManager()
    db.db_connect()
    for state, state_abbr in dict_state_abbr.iteritems():
        query = "SELECT COUNT(*) FROM %s_combined;" % state
        result = db.runQuery(query)
        print state, result[0][0]
        dict_state_record_numbers[state] = result[0][0]

    # Export results to file for future use.
    try:
        with open(data.DICT_PATH_DATAFILES['states_record_numbers.json'], 'w') as f:
            json.dump(dict_state_record_numbers, f)
            return dict_state_record_numbers
    except:
        raise Exception("Error: Unable to load state_record_numbers from file or recompute from database.")








































if __name__ == "__main__":
    print get_state_order()






