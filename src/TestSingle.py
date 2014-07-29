'''
Created on Jul 29, 2014

@author: navid
'''
'''
Created on Jul 25, 2014

@author: navid
'''

from copy import copy
import pickle

from main_general import *


def two_state_test():
    project1 = disambiguate_main('maryland', record_limit=(0, 400))
    
    project1.D.tokenizer.tokenize_functions = None
    project1.D.tokenizer.normalize_functions = None
    project1.D.project = None
        
    pickle.dumps(project1.D)
    quit()
    
    return project1



    project2 = disambiguate_main('oregon', record_limit=(0, 300))
    
    list_of_Ds = [project1.D, project2.D]
    
    D = Disambiguator.getCompoundDisambiguator(list_of_Ds)
    D.compute_LSH_hash(20)
    D.save_LSH_hash(batch_id='9999')
    D.compute_similarity(B1=40, m=20 , sigma1=None)
    
    #     project.save_data_textual(with_tokens=False, file_label="before")
    
    D.generate_identities()
    D.refine_identities()
        
    
    
    
    project1.D = D
    
    project1.save_data_textual(file_label="compound")









if __name__ == "__main__":
    
    two_state_test()
 