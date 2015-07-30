'''
This module defines the L{Person} class which groups records together,
representing an identity.
'''

class Person(object):
    '''
    Hashable class representing a person (group of records). It implements
    methods for measuring the compatibility between a pair of persons, splitting
    a person into more than one if multiple middle names are detected inside,
    and merging different Person objects if they are sufficiently compatible.
    '''
    # Static compatibility threshold. Higher means more strict
    compatibility_threshold = 0.5
   
    def __init__(self, records=[]):
        
        self.set_of_records = set()

        # Store the hashable record objects.
        if records:
            for record in records:
                self.addRecord(record)
            
        # TODO:  List of timestamps of self's records. Not sorted internally
        self.timeline = []
        
        # From now on, self.neighbors contains numeric ids of the neighbors not the person objects themselves
        self.neighbors = set()
        self.isDead = False
        
        # Town object that contains a dictionary mapping person ids to person objects
        self.town = None

        # set of other persons already compared with this person.
        self.set_compared = set()


    ''' The following two methods need to be defined in order to make the object hashable.
    Note that my definition of __eq__ works fine as long as I don't ever need to compare
    equality between two objects based on their "content".
    '''
    
    # Override
    def __hash__(self):
        return id(self)
    
    # Override
    def __eq__(self, other):
        return  (id(self) == id(other)) 
    
    
    
    def get_dominant_attribute(self,attr):
        '''
        For a given record attribute including "N_first_name", "N_last_name",
        "N_middle_name", "N_employer", "N_occupation", find the dominant value
        among the records in this person. We don't need to be too rigorous here. 
        @param attr: a string can be "N_last_name","N_middle_name", "N_employer",
        "N_occupation".
        '''
        dict_attr_freqs = {}
        for record in self.set_of_records:
            try:
                value = record[attr]
            except KeyError:
                continue
            if not value: continue
            try:
                dict_attr_freqs[value] += 1
            except KeyError:
                dict_attr_freqs[value] = 1
        try:
            dominant_attr = sorted(dict_attr_freqs.items(), key=lambda item:item[1],reverse = True)[0][0]
        except:
            dominant_attr = None
        return dominant_attr
    
    
    
    
    
    # Add the given record's object id to self.set_of_records    
    def addRecord(self, r):
        self.set_of_records.add(r)
        
        # Add to the Record a reference to the current Person
       
        r.identity = self
        # TODO: update the timeline?
    
    # Remove the given record's object id from self.set_of_records    
    def removeRecord(self, r):
        try:
            self.set_of_records.remove(r)
            r.identity = None
        except KeyError:
            pass
        # TODO: update the timeline?
        
    
    
    # Absorb other Person into this Person
    def merge(self, otherPerson):
        for record in otherPerson.set_of_records:
            self.set_of_records.add(record)
            
            # This rebinding of all the record's identity should release otherPerson for garbage collection
            record.identity = self
        
        # on merge, inherit otherPerson's neighbors 
        self.neighbors = self.neighbors.union(otherPerson.neighbors)
        
        # TODO: is this necessary any more?
        otherPerson.destroy()
        self.updateTimeline()
        
        # All previous comparisons are null.
        self.resetAlreadyCompared()
            
            
            
    def toString(self):
        s = ''.join(["\t".join([r['NAME'],r['N_last_name'],
                                r['OCCUPATION'], r['EMPLOYER'],
                                r['ZIP_CODE'], r['CITY'],
                                r['STATE'], "\n"]) 
                     for r in self.set_of_records])
        return s
            
            
            
    # TODO: Return a numberf self and otherPerson are so similar and consistent that they must be merged
    def compatibility(self, person1, person2):
        #print "computing compatibility", id(person1), id(person2)
        n1 = len(person1.set_of_records)
        n2 = len(person2.set_of_records)
        score = 0.0 
        for i,r1 in enumerate(person1.set_of_records):
            for j,r2 in enumerate(person2.set_of_records):
                verdict, result = r1.compare(r2, mode='thorough')
                score += verdict 
        return float(score) / (n1 * n2)
                
            
    def setAlreadyCompared(self, other):
        '''
        We record other as having already been compared with self.
        '''
        self.set_compared.add(other)

        
    def getAlreadyCompared(self, other):
        '''
        determine if other has already been compared with self.
        '''
        return (other in self.set_compared)
    
    def resetAlreadyCompared(self):
        '''
        clear self.set_compared
        '''
        self.set_compared.clear()    


    # Wrapper for lazy boolean access to self.compatibility()
    def isCompatible(self, other):
        
        # Return True if compatibility between self and other is larger than threshold
        return (self.compatibility(self, other) > Person.compatibility_threshold)
    
    
            
    # Return a list of all middle INITIALS in this person's records
    def get_middle_names(self):
        return {r['N_middle_name'][0] for r in self.set_of_records if r['N_middle_name']}

    # Return a list of all first names in this person's records
    def get_first_names(self):
        return {r['N_first_name'] for r in self.set_of_records if r['N_first_name']}

    # Return a list of all last names in this person's records
    def get_last_names(self):
        return {r['N_last_name'] for r in self.set_of_records if r['N_last_name']}




    # Print a list of all names
    def print_names(self):
        for record in self.set_of_records:
            print record['NAME']
        print "-"*50




            
    
    # TODO: Return a list of Person objects that should replace the current Person
    def split_on_MIDDLENAME(self):
       
        # list of Persons
        spawns = []
        
        # Dictionary that maps the middle initial to the new Person object
        dict_spawns = {}
        
        list_middlenames = self.get_middle_names()
        list_undecided_records = []
        n = len(list_middlenames)
        
        
        # Create new persons for each of the middle names
        for middlename in list_middlenames:
            dict_spawns[middlename] = Person()
            spawns.append(dict_spawns[middlename])
        
        # Assign the records with middle names to one of the spawns
        for record in self.set_of_records:
            middlename = record['N_middle_name'][0] if record['N_middle_name'] else None
            if middlename:
                dict_spawns[middlename].addRecord(record)
            else:
                list_undecided_records.append(record)
                
        # a dictionary mapping each undecided record to the child person who wins that record
        dict_winners = {}
        
        # TODO: decide what to do with records without a middle name
        # Each undecided record will be added to one of the spawned children.
        for record in list_undecided_records:
            
            # Dummy Person with only one record, to measure compatibility
            tmp_person = Person([record])
            dict_compatibilities = {}
            
            # Compute compatibilities between this record and the newly spawned children + the parent's neighbors
            for new_person in set(dict_spawns.values()).union(self.town.getPersonsById(self.neighbors)):
                dict_compatibilities[new_person] = self.compatibility(tmp_person, new_person)
            
            winner_person = sorted(dict_compatibilities.keys(), key=lambda person:dict_compatibilities[person])[-1]
            
            # If you add the record to the winner now, it'll bias the future comparisons. So don't do it!
            dict_winners[record] = winner_person
        
        # Now add the undecided records to their respective winner persons.
        for record, winner in dict_winners.iteritems():
            winner.addRecord(record)
            
        
            
        # TODO: each child inherits parent's neighbors. Its siblings are also its neighbors
        for child in spawns:
            
            # add child to the town (the town will bind itself to the child)
            self.town.addPerson(child)
             
            child.neighbors = child.neighbors.union(self.neighbors)
            
            
            for sibling in spawns:
                # Don't add itself!
                if sibling is not child:
                    child.neighbors = child.neighbors.union(sibling.neighbors)
                    try:
                        child.neighbors.remove(id(child))
                    except KeyError:
                        pass

        # Now all records are assigned to one of the spawned children. Return the list of spawns.
        return set(spawns)





    
    # TODO: Good idea? Loop through records and build a timeline
    def updateTimeline(self):
        pass
        

    # unbind all references to self's attributes
    def destroy(self):
        self.dict_of_records = None
        self.isDead = True
        
        # Remove person from the neighbor list of all its neighbors
        for neighbor in self.town.getPersonsById(self.neighbors):
            if self is neighbor: continue
            try:
                neighbor.neighbors.remove(id(self))
            except:
                pass




    # given a string attribute name, returns a list of all unique values of that attribute among the records
    # belonging to the person. 
    # For example, using this, we can get a list of all unique STATEs in the person's timeline
    def get_distinct_attribute(self, attr):
        list_attr = set()
        for record in self.set_of_records:
            try:
                list_attr.add(record[attr])
            except KeyError:
                pass
        if len(list_attr) == 0 : list_attr = None
        return list_attr
        
        
        
        
        
        
        
