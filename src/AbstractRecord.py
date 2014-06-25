'''
Created on Jun 25, 2014

@author: Navid Dianati
'''

class AbstractRecord():
    '''
    TODO: document class
    '''

    ''' A vector is a dictionary {index:(0 or 1)} where index is a unique indicator of a feature
    '''
    def __init__(self, vector):
        self.vector = vector
        
    
    def addFeatures(self,list_featureName):
        for featureName in list_featureName:
            setattr(self,featureName,None)
        