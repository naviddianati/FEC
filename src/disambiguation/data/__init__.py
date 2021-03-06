'''
This module defines variables used for accessing data files
used internally by various modules.

@var PATH_DATAFILES: Path to the folder containing this module.
@var DICT_PATH_DATAFILES:  Dict of paths to data files internally used.
'''

import os

PATH_DATAFILES, __this_filename = os.path.split(__file__)

# Dict of paths to data files internally used.
DICT_PATH_DATAFILES = {
                       'all-names.json' : os.path.join(PATH_DATAFILES, "all-names.json"),
                       'name-variants.json': os.path.join(PATH_DATAFILES, "name-variants.json"),
                       'name-variants.txt' : os.path.join(PATH_DATAFILES, "name-variants.txt"),
                       'name-variants-raw.csv' : os.path.join(PATH_DATAFILES, "name-variants-raw.csv"),
                       'states_record_numbers.json' : os.path.join(PATH_DATAFILES, "states_record_numbers.json")
                       }
