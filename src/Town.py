

class Town():
    '''
    A wrapper around the persons objects. 
    The main reason this is used is to allow the person.neighbor to contain integer id's of the neighbors
    rather than the person objects themselves. This solves an recursion problem when pickling D.set_of_persons.
    So, now the Town can receive a list of neighbor id's and return a list of the actual person objects themselves.
    The key feature allowing this is the dict_persons within the Town.
    '''
    
    def __init__(self):
        # A dictionary {integer id : person object}
        self.dict_persons = {}
    
    def getPersonsById(self,list_id):
        result = set()
        for id in list_id:
            try:
                person = self.dict_persons[id]
                result.add(person)
            except KeyError:
                pass
        return result
    
    
    def addPerson(self,person):
        self.dict_persons[id(person)] = person
        person.town = self
        
    
    
    def removePerson(self,person):
        try:
            del self.dict_persons[id(person)]
#             person.town = None
        except KeyError:
            pass
        
        
        