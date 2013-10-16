# Main branch

import numpy as np
import pylab as pl
import csv
import MySQLdb as mdb
import re
import nltk
from Disambiguator import *
import pprint
import time

# establish and return a connection to the MySql database server
def db_connect():
    con = None
    con = mdb.connect(host='localhost',  # hostname
                       user='navid',  # username                
                       passwd='YOURMYSQLPASSWORD',  # password
                       db='FEC',  # database 
                       use_unicode=True,
                       charset="utf8"
    )
    if con == None: print("Error connecting to MySql server")
    return con  

def MySQL_query(query):
    con = db_connect()
    cur = con.cursor()
    # cur.execute("select NAME,ZIP_CODE,EMPLOYER,TRANSACTION_DT from newyork order by NAME limit 1000 ;")
    cur.execute(query)
    return  cur.fetchall()
    

class FEC_analyst():
    
    def __init__(self):
        con = db_connect()
        cur = con.cursor()
        # cur.execute("select NAME,ZIP_CODE,EMPLOYER,TRANSACTION_DT from newyork order by NAME limit 1000 ;")
        cur.execute("select distinct NAME from newyork  order by NAME limit 100;")
        a = cur.fetchall()
        self.hash_dim = None
        self.D =  []  # Disambiguator object
        self.token_counts = {}
        self.token_2_index = {}
        self.index_2_token = {}
        self.all_tokens_sorted = []
        self.list_of_vectors = []
        self.no_of_tokens = 0
        self.list_of_strings = []
       
     
    def set_list_of_strings(self, list_of_strings):
        ''' This functions sets the main list of strings on which the similarity analysis is performed'''
        self.list_of_strings = list_of_strings

    def print_adj_rows(self, r=[], filename=None):
        ''' This function prints a sample of the rows of the adjacency matrix and the
            corresponding entries from the list'''
        if self.D and self.D.adjacency:
            separator = '----------------------------------------------------------------------------------------------------------------------'
            pp = pprint.PrettyPrinter(indent=4)
            pp.pprint(self.D.adjacency)
            if r:
                n_low, n_high = r[0], r[1]
            if filename: file = open(filename, 'w')
            for i in range(n_low, n_high):
                tokens = [str(x) for x in self.__get_tokens(self.list_of_strings[i])]
                s = "%d %s        %s\n" % (i, self.list_of_strings[i], '|'.join(tokens))
                if filename: file.write(s)
                else: print(s)
                
                for j in self.D.adjacency[i]:
                    tokens = [str(x) for x in self.__get_tokens(self.list_of_strings[j])]
                    s = "         %s        %s      %f\n" % (self.list_of_strings[j], '|'.join(tokens), Hamming_distance(self.D.LSH_hash[j], self.D.LSH_hash[i]) * 1.0 / self.hash_dim)
                    if filename: file.write(s)
                    else: print(s)
                if filename: file.write(separator + "\n")
                else: print(separator)
            if filename:
                file.close()
            
        
    def __get_tokens(self, s):
        
        # remove all numerals
        s1 = re.sub(r'\.|[0-9]+','',s)
        
        # List of all suffixes found then remove them
        suffix_list = re.findall(r'\bESQ\b|\bENG\b|\bINC\b|\bLLC\b|\bLLP\b|\bMRS\b|\bPHD\b|\bSEN\b',s)
        suffix_list += re.findall(r'\bDR\b|\bII\b|\bIII\b|\bIV\b|\bJR\b|\bMD\b|\bMR\b|\bMS\b|\bSR\b',s)
        s1 = re.sub(r'(\bESQ\b)|(\bENG\b)|(\bINC\b)|(\bLLC\b)|(\bLLP\b)|(\bMRS\b)|(\bPHD\b)|(\bSEN\b)','',s1)
        s1 = re.sub(r'(\bDR\b)|(\bII\b)|(\bIII\b)|(\bIV\b)|(\bJR\b)|(\bMD\b)|(\bMR\b)|(\bMS\b)|(\bSR\b)','',s1)
     
        s1 = re.sub(r'\.', '', s1)
        
#         print s1
        
        # Find the first single-letter token and save it as middle name, then remove it 
        middle_name_single = re.findall(r'\b\w\b',s1)
        if middle_name_single:
            middle_name_single= middle_name_single[0]
            s1 = re.sub(r'\b%s\b' % middle_name_single,'',s1)
        else:
            middle_name_single = ''    
        
        # Replace all word separators with spaces
#         s1 = re.sub(r'[\-\/;\+()]*', ' ', s1)
        
        # Compactify multiply whitespaces
        s1 = re.sub(r'\s+', ' ', s1)
        
#         print s1
        # Extract full last name and tokenize, then remove from s1
        last_name = re.findall(r'^[^,]*(?=\,)',s1)
        
        if not last_name:
            last_name_list=[]
            first_name_list=[]
            middle_name=[]
            
        else:
            last_name = last_name[0]        
            s1 = s1.replace(last_name,'')
            #Remove surrounding whitespace from last name
            last_name = re.sub(r'^\s+|\s+$','',last_name)
            last_name_list = re.split(r' ',last_name)
            
#             print s1
            # Extract first name
            first_name = re.findall(r'[^,]+',s1)
            if first_name:
                first_name = first_name[0]
                #Remove surrounding whitespace from last name
                first_name = re.sub(r'^\s+|\s+$','',first_name)
                first_name_list = re.split(r' ',first_name)
            
            if len(first_name_list)>1 and not middle_name_single:
                middle_name = first_name_list[-1]
                first_name_list.remove(middle_name)
            else: 
                middle_name = middle_name_single

        print s
        print "    First name:--------- ", first_name_list
        print "    Las tname:--------- ", last_name_list
        print "    Middle name:--------- ", middle_name
        print "    Suffixes:--------- ", suffix_list

        tokens = []
#         if middle_name==[]: middle_name=''
        for s in last_name_list:
            tokens.append((1,s))
        for s in first_name_list:
            tokens.append((2,s))
#         for s in suffix_list:
#             tokens.append((4,s))
            
        # middle_name is set up to be a string at this point
        if len(middle_name)>0:
            tokens.append((3,middle_name[0]))
        
        return tokens    
        
                

    
    def __get_tokens_old(self, s):
        ''' This function returns a tuple (index,word) where index is in {1,2,3} and indicates whether the word
            was part of the first name, last name of middle name. word is the actual token.'''
        # Remove suffied such as MR, MS, MRS
        s1 = re.sub(r'( MRS\b)|( MR\b)|( MS\b)|( DR\b)|\.', '', s)
        s1 = re.sub(r'[\-\/;+()]*', ' ', s1)
        s1 = re.sub(r'\s+', ' ', s1)
        
        # Find and remove last name
        # Remove middle name initials
#         print s
        s1 = re.sub(r' [a-zA-Z]$| [a-zA-Z][ .]', ' ', s1)
#         print s,'\n\n'
        s1 = re.sub(r' $', '', s1)
       
        # NLTK tokenize
        tokens = re.split(r' ', s1)
        
        if len(tokens) > 1:
            return tokens
        else: 
            s1 = re.sub(r'\,|\.', '', s)
            s1 = re.sub(r'[\-\/;+()]*#', ' ', s1)
            s1 = re.sub(r'\s+', ' ', s1)
            # DON'T! Remove middle name initials
#             s1 = re.sub(r' [a-zA-Z]$| [a-zA-Z][ .]',' ',s1)
            s1 = re.sub(r' $', '', s1)
        
        tokens = re.split(r' ', s1)
        if len(tokens) > 1:
            return tokens    
        else: 
            tokens.append('!!ERROR!!') 
            return tokens
 
#         return nltk.word_tokenize(s)
        
         
    
    def tokenize(self):
        for i in range(len(self.list_of_strings)):
            s = self.list_of_strings[i]
        #     print s
        
            # s_plit is a list of tuples: [(1,'sdfsadf'),(2,'ewre'),...]     
            s_split = self.__get_tokens(s)
            print s_split
            
        #     print '----------------------'
            vec = {}
            for token in s_split:
                if token in self.token_2_index:
                    self.token_counts[token] += 1
                else:
                    self.token_2_index[token] = self.no_of_tokens
                    self.index_2_token[self.no_of_tokens]=token
                    self.token_counts[token] = 1
                    self.no_of_tokens += 1
                vec[self.token_2_index[token]] = 1
            self.list_of_vectors.append(vec)
                
        self.token_frequencies = sorted(self.token_counts.values(), reverse=1)
        
        self.all_token_sorted = sorted(self.token_counts, key=self.token_counts.get, reverse=0)
        for token in self.all_token_sorted:
            print token, self.token_counts[token]

        print "Total number of tokens identified: ", len(self.token_counts)


    def save_list_of_strings_to_file(self, filename='../data/list_of_strings.txt'):
        f = open(filename, 'w')
        for s, i in zip(self.list_of_strings, range(len(self.list_of_strings))):
            f.write("%d %s\n" % (i, s))
        f.close()
        
        
    def save_adjacency_to_file(self, filename='../data/adjacency.txt'):
        # save adjacency matrix to file
#         filename = '/home/navid/edges.txt'
        f = open(filename, 'w') 
        if  self.D.adjacency: 
            for node1 in self.D.adjacency:
                for node2 in self.D.adjacency[node1]:
                    f.write(str(node1) + ' ' + str(node2) + "\n")
            f.close()



    def analyze(self, hash_dim=400, sigma=0.2, B=10):
        # Number of random vectors to generate
        # N = 400
        self.hash_dim = hash_dim
        self.tokenize()
        
        # dimension of input vectors
        dim = len(self.token_counts)
        
        # desired dimension (length) of hashes
        # hash_dim = 200
        
        # Number of times the hashes are permutated and sorted
        no_of_permutations = 100
        
        # Hamming distance threshold for adjacency 
        # sigma = 0.2
        
        # Number of adjacent hashes to compare
        # B = 10
        
        
        self.D = Disambiguator(self.list_of_vectors,self.index_2_token,self.token_2_index, dim)
        
        # compute the hashes
        print "Computing the hashes..."
        self.D.compute_LSH_hash(hash_dim)
        print "Hashes computed..."
        
        self.D.save_LSH_hash()
        
        self.D.compute_similarity(B=B, m=no_of_permutations , sigma=sigma)
        
            
        self.D.show_sample_adjacency()
            
        
        
def find_all_in_list(regex,str_list):
    ''' Given a list of strings, str_list and a regular expression, regex, return a dictionary with the
        frequencies of all mathces in the list.'''
    dict_matches = {}
    for s in str_list:
#         s_list = re.findall(r'\b\w\b', s)
        s_list = re.findall(regex, s)
        for s1 in s_list:
            if s1 in dict_matches:
                dict_matches[s1] += 1 
            else:
                dict_matches[s1] = 1
    return dict_matches

        

pp = pprint.PrettyPrinter(indent=4)
# pp.pprint(self.D.adjacency)


analyst = FEC_analyst()

# Get string list from MySQL query and set it as analyst's list_of_strings
query_result = MySQL_query("select distinct NAME from newyork  where NAME <>'' order by NAME limit 20000,1000;")
tmp_list = []
for i in range(len(query_result)):
    tmp_list.append(query_result[i][0])


# tmp_list = ['Navid, Dianati', 'Navid, Dianati', 'Dianati, Navid A.', 'Navid, Dianati M MR.', 'Navid, Dianati Mr.', 'Navid, Dianati D.', 'Dianati, N. M. MR', 'Navid, Dianati', 'Navid, Dianati', 'Dianati, Navid A.', 'Navid, Dianati M MR.', 'Navid, Dianati Mr.', 'Navid, Dianati D.', 'Dianati, N. M. MR']
tmp_list = [s.upper() for s in tmp_list]

# for s in tmp_list:
#     analyst.get_tokens_new(s)
# quit()



analyst.set_list_of_strings(tmp_list)

t1 = time.time()
analyst.analyze(hash_dim=100, sigma=0.26, B=10)
t2 = time.time()
print t2 - t1

analyst.save_list_of_strings_to_file()
analyst.save_adjacency_to_file()

analyst.D.imshow_adjacency_matrix(r=(0, 900))
pl.show()
  
  


    

analyst.print_adj_rows(r=[0, 900], filename='../data/adj_text.txt')
