'''
Created on Jul 9, 2014

@author: navid
'''

class Person(object):
   
    def __init__(self, params):
        # TODO: decide on how to store a list of records belonging to person.
        
        # Store the hashable record objects.
        self.dict_of_records = {}
        
        # TODO:  List of timestamps of self's records. Not sorted internally
        self.timeline = []

    # Add the given record's object id to self.dict_of_records    
    def addRecord(self, r):
        self.dict_of_records[r] = 1
        # TODO: update the timeline?
    
    # Remove the given record's object id from self.dict_of_records    
    def removeRecord(self,r):
        try:
            del self.dict_of_records[r]
        except KeyError:
            pass
        # TODO: update the timeline?
        
    
    
    # Absorb otherPerson into this Person
    def merge(self,otherPerson):
        for record in otherPerson.dict_of_records:
            self.dict_of_records[record] = 1
            
            # This rebinding of all the record's identity should release otherPerson for garbage collection
            record.identity = self
      
        # TODO: is this necessary any more?
        otherPerson.destroy()
        self.updateTimeline()
            
            
    # TODO: Good idea? Loop through records and build a timeline
    def updateTimeline(self):
        pass
        

    # unbind all references to self's attributes
    def destroy(self):
        self.dict_of_records = None