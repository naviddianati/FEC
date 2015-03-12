import re

"""
this file contains generic functions used by various scripts
"""



def bad_identifier(identifier, type='employer'):    
    if identifier == '': return True
    if type == 'employer':
        regex = r'\bNA\b|N\.A|employ|self|N\/A|\
                |information request|retired|teacher\b|scientist\b|\
                |applicable|not employed|none|\
                |homemaker|requested|executive|educator\b|\
                |attorney\b|physician|real estate|\
                |student\b|unemployed|professor|refused|doctor|housewife|\
                |at home|president|best effort|consultant\b|\
                |email sent|letter sent|software engineer|CEO|founder|lawyer\b|\
                |instructor\b|chairman\b'
    elif type == 'occupation':
        regex = r'unknown|requested|retired|none|retire|retited|ret\b|declined|N.A\b|refused|NA\b|employed|self'
    else:
        print 'invalid identifier type'
        quit()
#     print identifier
    if re.search(regex, identifier, flags=re.IGNORECASE): 
        return True
    else: 
        return False
