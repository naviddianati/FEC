'''
This module contains methods used for the second stage of disambiguation.
in this stage, we use combined national hashes and the state-level results
to identify pairs of records that are candidates for comparison but haven't
already been clustered together (will be mostly cross-state pairs). Then,
We divide these records into approximately independent partitions each of
which can be independently analyzed by a child process. This analysis consists
of performing pairwise comparisons for each specified pair and deciding about
whether to merge their corresponding clusters.
In making this decision, cluster level statistics will be used. That is,
information regarding name/employer/occupation/etc. frequencies obtained
from stage1 clusters are used.
'''

from disambiguation.core import utils
from disambiguation.core import hashes
import disambiguation.config as config
from disambiguation.core import Database



def get_candidate_pairs(num_pairs, state='USA',):
    '''
    Get pairs of record ids that are similar according
    to the national (combined) hashes, but aren't already
    linked at the state level.
    @param state: the state whose hashes will be used.
    @param num_pairs: number of new records to select for comparison
    @return: list of tuples of record ids.
    '''
    identity_manager = Database.IdentityManager(state=state)

    # Dictionary id:identity from stage 1
    dict_id_2_identity = identity_manager.get_dict_id_2_identity()

    def is_new(edge):
        '''
        Determine if the stage 1 identities of records in
        pair are identical. If not, the pair is new.
        '''
        return True if dict_id_2_identity[edge[0]] != dict_id_2_identity[edge[1]] else False

    # Number of adjacency hashes to log
    B = 20

    # number of times to shuffle the list of hashes.
    num_shuffles = 2

    # list of candidate pairs to be compared.
    list_pairs = []

    # Get the full sorted edgelist from national hashes.
    filename = config.hashes_file_template % (state, 'Tokenizer')
    edgelist = hashes.get_edgelist_from_hashes_file(filename, B, num_shuffles, num_procs=12)

    # Main loop.
    while (len(list_pairs) < num_pairs) and edgelist:
        edge = edgelist.pop()
        if is_new(edge):
            list_pairs.append(edge)
            print "new"
        else:
            print "old"

    return list_pairs







def partition_records(list_record_pairs, num_partitions, file_label):
    '''
    Partition the full record set into num_procs subsets
    with minimal inter-set links, and export the record ids
    to a separate file for each subset.
    @param list_record_pairs: list of tuples of record ids.
    @param num_partitions: number of partitions to divide list_record_pairs into.
    @param file_label: filename prefix for the output files.
    '''
    pass


def disambiguate_subsets_multiproc(list_filenames, num_procs):
    '''
    Compare record pairs within each subset and save results.
    @param list_filenames: list of filenames exported by partition_records()

    '''

    pass


