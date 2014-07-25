'''
Created on Jul 25, 2014

@author: navid
'''

from main_general import *



project1 = disambiguate_main('washington', record_limit=(0, 1000))
project2 = disambiguate_main('oregon', record_limit=(0, 1000))

list_of_Ds = [project1.D, project2.D]

D = Disambiguator.getCompoundDisambiguator(list_of_Ds)


project1.D = D

project1.save_data_textual(file_label="compound")