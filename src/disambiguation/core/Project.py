import disambiguation.config as config
from utils import *

class Project(dict):
    def __init__(self, batch_id):
        self["batch_id"] = batch_id
        self["data_path"] = config.data_path 
        self["logfilename"] = '../records/' + str(self["batch_id"]) + '.record'
        # self["logfile"] = open(self["logfilename"], 'w', 0)
        self["messages"] = []
        self["list_tokenized_fields"] = []
        self["list_auxiliary_fields"] = []
        self["state"] = "" 
    
        
    def saveSettings(self, file_label=""):
        settings = {}
        for item in self.keys():
            if item not in ['list_of_records', "D", "tokenizer"]:
#             if True:
                try:
                    tmp = json.dumps(self[item])
                    settings[item] = self[item]
                except TypeError:
                    print "WWARNING: ", item, " not serializable. Skiping." 
        f = open(self["data_path"] + file_label + self["batch_id"] + '-settings.json', 'w')
        f.write(json.dumps(settings, indent=4))
        f.close()
        
        
       
        
    def putData(self, key, value):
        self[key] = value
    
    def log(self, key, value):
#         self.messages.append((key, value))
        logfile = open(self["logfilename"], 'a', 0)
        logfile.write("%s : %s\n" % (key, value))  # write without buffering
        logfile.close()
#         self.logfile.close()
#         self.logfile = open(self.logfilename, 'w')
    
    def setBatchId(self, batch_id):
        self["batch_id"] = batch_id
        self.log('Batch ID' , self.batch_id)

        
      

    def save_graph_to_file(self, list_of_nodes=[], file_label=""):
        '''
        Take the adjacency matrix resulting from the Disambiguator object,
        and write it to file in edgelist (.edges) and .json formats
        '''
        filename_json = self["data_path"] + file_label + self["batch_id"] + '-adjacency.json'
        filename_edgelist = self["data_path"] + file_label + self["batch_id"] + '-adjacency.edges'
        f_json = open(filename_json, 'w') 
        f_edgelist = open(filename_edgelist, 'w') 

        if  not self.D.index_adjacency: return 
        if not list_of_nodes: list_of_nodes = range(len(self.D.list_of_records))
        nmin = list_of_nodes[0]
        list_of_links = []
        for node1 in list_of_nodes:
            for node2 in self.D.index_adjacency[node1]:
                list_of_links.append((node1 - nmin, node2 - nmin))
                f_edgelist.write(str(node1 - nmin) + ' ' + str(node2 - nmin) + "\n")
        f_json.write(json.dumps(list_of_links))
        f_json.close()
        f_edgelist.close()

    
    
    def dump_full_adjacency(self, file_label=""):
        '''
        Compute the full adjacency matrix from the D.set_of_persons
        and dumps it to a text file as edgelist. In this graph, there
        is an edge between any two records belonging to the same person.
        '''
        print "writing full adjacency to file... "
        filename_edgelist = self["data_path"] + file_label + '-adjacency.edges'
        f = open(filename_edgelist, 'w')
        for id, person in self.D.town.dict_persons.iteritems():
            for record1 in person.set_of_records:
                for record2 in person.set_of_records:
                    if record1 is not record2:
                        f.write(str(record1.id) + " " + str(record2.id) + "\n")
        f.close()
                        

    
    
    def set_list_of_records_auxiliary(self, tmp_list):
        '''
        This functions sets the list of auxiliary records
        associated with the items in list_of_records_identifier.
        '''
        self.list_of_records_auxiliary = tmp_list


    def set_list_of_records_identifier(self, list_of_records_identifier):
        '''
        This functions sets the main list of strings
        on which the similarity analysis is performed.
        '''
        self.list_of_records_identifier = list_of_records_identifier
    
    
    def save_data_textual(self, with_tokens=False, file_label=""):
        css_code = "table{border-collapse:collapse;\
                    padding:5px;\
                    font-family:sans;\
#                     width:100%;\
                    font-size:10px;\
                    border:dotted thin #efefef;}\
                    td{padding:5px;\
                    border:dotted thin #efefef;} "
        filename1 = self["data_path"] + file_label + self["batch_id"] + '-adj_clusters.json'
        filename2 = self["data_path"] + file_label + self["batch_id"] + '-adj_clusters.html'

        f1 = open(filename1, 'w')
        f2 = open(filename2, 'w')
        f2.write("<head><style>"
                 + css_code

                 + "</style>"
                 + "</head>")
        
        list_tokens = []

        if with_tokens:
            dict_tokens = {}
            for record in self.D.list_of_records:
                dict_tokens[record.id] = self.D.tokenizer._get_tokens(record, self["list_tokenized_fields"])
            
        len(self.D.list_of_records)
#         quit()


        # how many blocks at a time to dump to file
        page_size = 20;
        
        if self.D:
           
            list_id = []
            dataframe_data = []
            
            # Old version, before D.set_of_persons was implemented
            # for g in sorted(self.persons_subgraphs, key=lambda g: min([int(v['name']) for v in g.vs])):
            #     for v in sorted(g.vs, key=lambda v:int(v['name'])):
            #           index = int(v['name'])
            #           r = self.list_of_records [index]
            
            # Where in self["list_tokenized_fields"] is the date field? Used below
            time_index = self["list_tokenized_fields"].index('TRANSACTION_DT')
            
            person_counter = 0 
            list_persons = self.D.town.getAllPersons()
            list_persons.sort(key=lambda person:min([r['NAME'] for r in person.set_of_records ]))
            for person in list_persons:
                new_block = []
                for r in sorted(list(person.set_of_records), key=lambda record : record['NAME']):
                
                
                    record_as_list_tokenized = [r[field] for field in self["list_tokenized_fields"]]
                    record_as_list_auxiliary = [r[field] for field in self["list_auxiliary_fields"]]
                    
                    if with_tokens:
                        tmp_tokens = dict_tokens[r.id]
                        tokens_str = [str(x) for x in tmp_tokens]
                    
                    # new_row = record_as_list_tokenized + [r['N_first_name'], r['N_last_name'], r['N_middle_name']]

                    # With tokens
                    # new_row = record_as_list_tokenized + [' '.join(tokens_str)] + [r['N_first_name'], r['N_last_name'], r['N_middle_name']]
                    
                    # Without tokens
                    # new_row = record_as_list_tokenized + [r['N_first_name'], r['N_last_name'], r['N_middle_name']]
                    
                    # without normalized names
                    new_row = record_as_list_tokenized  # + [r['N_address']]
                    
                    new_row = ["" if s is None else s.encode('ascii', 'ignore') if isinstance(s, unicode) else s  for s in new_row ] + [r.id] + [r['N_address']] + [r['N_middle_name']]
                    new_block.append(new_row)

                    
                    if with_tokens:
                        s1 = "%d %s        %s\n" % (r.id, record_as_list_tokenized , '|'.join(tokens_str))
                    else:
                        s1 = "%d %s\n" % (r.id, record_as_list_tokenized)
#                     f1.write(s1)
                new_block = sorted(new_block, key=lambda row:row[time_index])
                dataframe_data += new_block
                dataframe_data += [["" for i in range(len(dataframe_data[0]) - 2)] + ["|"] + [""] for j in range(3)]
                
                person_counter += 1
                
                # Save a group of blocks to file
                if person_counter % page_size == 0:
                    df = pd.DataFrame(dataframe_data, columns=self["list_tokenized_fields"] + ['N_address'] + ['id'] + ['N_middle_name'])
                    # df = pd.DataFrame(dataframe_data, columns=self["list_tokenized_fields"] + ['N_address'] + ['id'])
                    df.set_index('id', inplace=True)
                    f1.write(df.to_string(justify='left').encode('ascii', 'ignore'))
                    f2.write(df.to_html().encode('ascii', 'ignore'))
                    f2.write("<br/><br/>")
                    
                    # Reset the output buffer
                    list_id = []
                    dataframe_data = []
                    


                
#                 f1.write('\n' + separator + '\n')   

#             df = pd.DataFrame(dataframe_data, index=list_id, columns=self["list_tokenized_fields"]+['N_first_name', 'N_last_name', 'N_middle_name'])
#             df = pd.DataFrame(dataframe_data, index=list_id, columns=self["list_tokenized_fields"] + ['tokens']+['N_first_name', 'N_last_name', 'N_middle_name'])
            
            # if there's a fraction of a page left at the end, write that too. 
            if dataframe_data:
                df = pd.DataFrame(dataframe_data, columns=self["list_tokenized_fields"] + ['N_address'] + ['id'] + ['N_middle_name'])
                f1.write(df.to_string(justify='left').encode('ascii', 'ignore'))
    
                f2.write(df.to_html().encode('ascii', 'ignore'))
                f2.write("<br/><br/>")

            f1.close()
            f2.close()
                
                
                    
                    
            
            

    
    def save_data(self, r=[], verbose=False, with_tokens=False, file_label=""):
            ''' This function does three things:
                1- saves a full description of the nodes with all attributes in json format to a file <batch_id>-list_of_nodes.json
                   This file, together with the <batch-id>-adjacency.txt file provides all the information about the graph and its
                   node attributes.
                2- saves a formatted text representation of the adjacency matrix with identifier information
                3- saves a formatted text representation of the adjacency matrix with auxiliary field information.
            '''
            
            # Save the adjacency matrix to file in both edgelist and json formats
            self.save_graph_to_file(file_label=file_label)
            
            
            filename1 = self["data_path"] + file_label + self["batch_id"] + '-adj_text_identifiers.json'
            filename2 = self["data_path"] + file_label + self["batch_id"] + '-adj_text_auxiliary.json'
            filename3 = self["data_path"] + file_label + self["batch_id"] + '-list_of_nodes.json'
            if self.D and self.D.index_adjacency:
                separator = '-' * 120
                pp = pprint.PrettyPrinter(indent=4)
#                 pp.pprint(self.D.index_adjacency)
    
                n = len(self.D.list_of_records)
                if r:
                    save_range = range(max(0, r[0]), min(n, r[1]))
                else: 
                    save_range = range(len(self.D.list_of_records))
    
                file1 = open(filename1, 'w')
                file2 = open(filename2, 'w')
                file3 = open(filename3, 'w')
                dict_all3 = {}
                



                if with_tokens:
                    list_tokens = []
                    for i in save_range:
                        list_tokens.append(self.tokenizer._get_tokens(self.D.list_of_records [i], self["list_tokenized_fields"]))
                    
                for i in save_range:
                    
                    record_as_list_tokenized = [self.D.list_of_records [i][field] for field in sorted(self["list_tokenized_fields"])]
                    record_as_list_auxiliary = [self.D.list_of_records [i][field] for field in sorted(self["list_auxiliary_fields"])]

                    dict_all3[i] = {'data':[self.D.list_of_records[i][field] for field in self["all_fields"]]}

                    if with_tokens:
                        tmp_tokens = list_tokens[i]
                        tokens_str = [str(x) for x in tmp_tokens]
                        tokens = {x[0]:x[1] for x in tmp_tokens} 
                        dict_all3[i]['ident_tokens'] = tokens
                    
                        s1 = "%d %s        %s\n" % (i, record_as_list_tokenized , '|'.join(tokens_str))
                    else:
                        s1 = "%d %s\n" % (i, record_as_list_tokenized)

                    s2 = "%d %s \n" % (i, record_as_list_auxiliary)
                    file1.write(separator + '\n' + s1)   
                    file2.write(separator + '\n' + s2)
                    for j in sorted(self.D.index_adjacency[i], key=lambda k:self.D.list_of_records [k]['TRANSACTION_DT']):
                        record_as_list_tokenized__2 = [self.D.list_of_records [j][field] for field in sorted(self["list_tokenized_fields"])]
                        record_as_list_auxiliary__2 = [self.D.list_of_records [j][field] for field in sorted(self["list_auxiliary_fields"])]
                        
                        
                        if with_tokens:
                            tmp_tokens = [str(x) for x in list_tokens[j]]
                            tokens_str = [str(x) for x in tmp_tokens]
                            tokens = {x[0]:x[1] for x in tmp_tokens} 
                            s1 = "    %d %s        %s\n" % (j, record_as_list_tokenized__2 , '|'.join(tokens_str))
                        else:
                            s1 = "    %d %s\n" % (j, record_as_list_tokenized__2)
                            
                        s2 = "    %d %s \n" % (j, record_as_list_auxiliary__2)
                        file1.write(s1)   
                        file2.write(s2)    
    
                file3.write(json.dumps(dict_all3))    
                
                file1.close()
                file2.close()
                file3.close()
                
            
