'''
This module defines global variables used
uniformly by various scripts.
'''

import os

# Global data path.
data_path = os.path.expanduser('~/data/FEC/')



tokendata_file_template = data_path + "%s-tokendata.pickle"

# This file contains a pickled dictionary
# {r.id: vector for r in list_of_records}
vectors_file_template = data_path + "%s-vectors.pickle"

# This file contains the LSH hashes dictionary
# {r.id: hash for r in list_of_records}
hashes_file_template = data_path + "%s-hashes.pickle"


# File containing a dictionary of the normalized attributes
# of the records: {r.id: {attr_name:attr_value}}
normalized_attributes_file_template = data_path + "%s-normalized_attributes.pickle"


# File containing the edge list of related records
match_buffer_file_template = data_path + "%s-match_buffer.pickle"
