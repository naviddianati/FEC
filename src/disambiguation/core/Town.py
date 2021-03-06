'''
This module defines the L{Town} class which is a convenience container
of L{Person} objects. This class is used to solve a merely technical
problem related to pickling L{Disambiguator} objects, and is otherwise
unimportant.
'''

class Town():
    '''
    A wrapper around the persons objects.
    The main reason this is used is to allow the person.neighbor
    to contain integer id's of the neighbors rather than the person
    objects themselves. This solves an recursion problem when pickling
    C{D.set_of_persons}. So, now the Town can receive a list of neighbor id's
    and return a list of the actual person objects themselves. The key feature
    allowing this is the C{dict_persons} within the Town.
    '''

    def __init__(self):
        # A dictionary {integer id : person object}
        self.dict_persons = {}

    def getPersonsById(self, list_id):
        result = set()
        for id in list_id:
            try:
                person = self.dict_persons[id]
                result.add(person)
            except KeyError:
                pass
        return result




    def getAllPersons(self):
        return self.dict_persons.values()


    def generateAllPersons(self):
        for id, person in self.dict_persons.iteritems():
            yield person


    def addPerson(self, person):
        self.dict_persons[id(person)] = person
        person.town = self



    def removePerson(self, person):
        try:
            del self.dict_persons[id(person)]
#             person.town = None
        except KeyError:
            pass

    def merge(self, town):
        ''' Merge another town into this town'''
        self.dict_persons.update(town.dict_persons)




