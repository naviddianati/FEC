'''
This module defines global variables used
uniformly by various scripts.
'''

import os

# Global data path.
data_path = os.path.expanduser('~/data/FEC-test/')


src_path = os.path.join(os.path.dirname(__file__), '')


dict_paths = { 
    "data_path": data_path,
    "data_path_vectors" :  data_path + "vectors/",
    "data_path_hashes" : data_path + "hashes/",
    "data_path_tokendata" : data_path + "tokendata/",
    "data_path_normalized_attributes" : data_path + "normalized_atrributes/",
    "data_path_affiliations_employer" : data_path + "affiliations/employer/",
    "data_path_affiliations_occupation" : data_path + "affiliations/occupation/",
    "data_path_match_buffers" : data_path + "match_buffers/",
    "data_path_near_misses" : data_path + "near_misses/"
}

# Make sure the data paths exist
for url, directory in dict_paths.iteritems():
    if not os.path.exists(directory):
        os.makedirs(directory)


tokendata_file_template = dict_paths["data_path_tokendata"] + "%s-%s-tokendata.pickle"


# This file contains a pickled dictionary
# {r.id: vector for r in list_of_records}
vectors_file_template = dict_paths["data_path_vectors"] + "%s-%s-vectors.pickle"


# This file contains the LSH hashes dictionary
# {r.id: hash for r in list_of_records}
hashes_file_template = dict_paths["data_path_hashes"] + "%s-%s-hashes.pickle"


# File containing a dictionary of the normalized attributes
# of the records: {r.id: {attr_name:attr_value}}
normalized_attributes_file_template = dict_paths["data_path_normalized_attributes"] + "%s-normalized_attributes.pickle"


# File containing the edge list of related records
match_buffer_file_template = dict_paths["data_path_match_buffers"] + "%s-match_buffer.txt"


# File containing the results of all pairwise record comparisons
comparisons_file_template = dict_paths["data_path_near_misses"] + "%s-record_comparisons.json.txt"


# File with each line a json object documenting a near miss pair of records.
near_misses_file_template = dict_paths["data_path_near_misses"] + "%s-near_misses.json.txt"
