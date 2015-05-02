# This module contains data on US states

import pandas as pd
import os

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



def get_state_order():
    '''
    Return a dictionary {state: order} where order is an
    integer ranking the state based on population. Lower
    order means higher population.
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



def get_states_sorted():
    '''
    Return a list of states ordered by the state population. Lower
    index means higher population.
    '''
    dict_state_order = get_state_order()
    list_states = sorted(dict_state.values())
    list_states.sort(key=lambda state: dict_state_order[state])
    print list_states
    return list_states


if __name__ == "__main__":
    print get_state_order()
