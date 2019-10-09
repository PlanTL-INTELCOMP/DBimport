"""
Created on Feb25 2019
@author: Jerónimo Arenas García

Import Semantic Scholar Database to MySQL DB

    * Creating database from downloaded gzip files

"""

import argparse
import configparser

from dbmanager.S2manager import S2manager
from lemmatizer.ENlemmatizer import ENLemmatizer

import ipdb
import time


import re

try:
    # UCS-4
    regex = re.compile('[\U00010000-\U0010ffff]')
except re.error:
    # UCS-2
    regex = re.compile('[\uD800-\uDBFF][\uDC00-\uDFFF]')


def clean_utf8(rawdata):
    return regex.sub(' ', rawdata)


def main(resetDB=False, importData=False, importCitations=False, importAuthorship=False,
         importEntities=False, lemmatize=False, lemmas_query=None):
    """
    """

    cf = configparser.ConfigParser()
    cf.read('config.cf')

    #########################
    # Configuration variables
    #
    dbUSER = cf.get('DB', 'dbUSER')
    dbPASS = cf.get('DB', 'dbPASS')
    dbSERVER = cf.get('DB', 'dbSERVER')
    dbCONNECTOR = cf.get('DB', 'dbCONNECTOR')
    dbNAME = cf.get('SemanticScholar', 'dbNAME')

    #########################
    # Datafiles
    #
    data_files = cf.get('SemanticScholar', 'data_files')

    ####################################################
    #1. Database connection

    DB = S2manager (db_name=dbNAME, db_connector=dbCONNECTOR, path2db=None,
                    db_server=dbSERVER, db_user=dbUSER, db_password=dbPASS)
    #               db_port=dbPORT)
    DB._conn.query('SET GLOBAL connect_timeout=60000')
    DB._conn.query('SET GLOBAL wait_timeout=60000')
    DB._conn.query('SET GLOBAL interactive_timeout=60000')

    ####################################################
    #2. If activated, remove and create again database tables
    if resetDB:
        print('Regenerating the database. Existing data will be removed.')
        # The following method deletes all existing tables, and create them
        # again without data
        DB.deleteDBtables()
        DB.createDBschema()

    ####################################################
    # 3. If activated, authors and papers data
    # will be imported from S2 data files
    if importData:
        print('Importing papers data ...')
        DB.importData(data_files)

    ####################################################
    # 4. If activated, citations data
    # will be imported from S2 data files
    if importCitations:
        print('Importing citations data ...')
        DB.importCitations(data_files)

    ####################################################
    # 5. If activated, authorship data
    # will be imported from S2 data files
    if importAuthorship:
        print('Importing authorship data ...')
        DB.importAuthorship(data_files)

    ####################################################
    # 6. If activated, entities associated to each paper
    # will be imported from S2 data files
    if importEntities:
        print('Importing entities associated to each paper ...')
        DB.importEntities(data_files)

    ####################################################
    # 7. If activated, will carry out lemmas extraction for the
    # imported papers
    if lemmatize:
        print('Lemmatizing Titles and Abstracts ...')

        #Now we start the heavy part. To avoid collapsing the server, we will 
        #read and process in chunks of N articles
        chunksize = 25000
        cont = 0
        lemmas_server = cf.get('Lemmatizer', 'server')
        stw_file = cf.get('Lemmatizer', 'stw_file')
        dict_eq_file = cf.get('Lemmatizer', 'dict_eq_file')
        POS = cf.get('Lemmatizer', 'POS')
        concurrent_posts = int(cf.get('Lemmatizer', 'concurrent_posts'))
        removenumbers = cf.get('Lemmatizer', 'removenumbers') == 'True'
        keepSentence = cf.get('Lemmatizer', 'keepSentence') == 'True'

        #Initialize lemmatizer
        ENLM = ENLemmatizer(lemmas_server=lemmas_server, stw_file=stw_file,
                    dict_eq_file=dict_eq_file, POS=POS, removenumbers=removenumbers,
                    keepSentence=keepSentence)
        selectOptions = 'paperID, title, paperAbstract'
        if lemmas_query:
            filterOptions = 'paperID>0 AND ' + lemmas_query
        else:
            filterOptions = 'paperID>0'
        init_time = time.time()
        df = DB.readDBtable('S2papers', limit=chunksize, selectOptions=selectOptions,
                 filterOptions = filterOptions, orderOptions='paperID ASC')
        while (len(df)):
            cont = cont+len(df)
            
            #Next time, we will read from the largest paperID. This is the
            #last element of the dataframe, given that we requested an ordered df
            largest_id = df['paperID'][len(df)-1]
            print('Number of articles processed:', cont)
            print('Last Article Id read:', largest_id)

            df['alltext'] = df['title'] + '. ' + df['paperAbstract']
            df['alltext'] = df['alltext'].apply(clean_utf8)
            lemasBatch = ENLM.lemmatizeBatch(df[['paperID', 'alltext']].values.tolist(),
                                                processes=concurrent_posts)
            #Remove entries that where not lemmatized correctly
            lemasBatch = [[el[0], clean_utf8(el[1])] for el in lemasBatch if len(el[1])]
            print('Successful lemmatized documents:', len(lemasBatch))
            DB.setField('S2papers', 'paperID', ['LEMAS'], lemasBatch)
            if lemmas_query:
                filterOptions = 'paperID>' + str(largest_id) + ' AND ' + lemmas_query
            else:
                filterOptions = 'paperID>' + str(largest_id)
            df = DB.readDBtable('S2papers', limit=chunksize, selectOptions=selectOptions,
                 filterOptions = filterOptions, orderOptions='paperID ASC')
            elapsed_time = time.time() - init_time
            print('Elapsed Time (seconds):', time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))

    return


if __name__ == "__main__":

    parser = argparse.ArgumentParser(prog='importSScholar')    
    parser.add_argument('--resetDB', action='store_true', help='If activated, the database will be reset and re-created')
    parser.add_argument('--importData', action='store_true', help='If activated, import author and paper data')
    parser.add_argument('--importCitations', action='store_true', help='If activated, import citation data')
    parser.add_argument('--importAuthorship', action='store_true', help='If activated, import authorship data')
    parser.add_argument('--importEntities', action='store_true', help='If activated, import entities data')
    parser.add_argument('--lemmatize', action='store_true', help='If activated, lemmatize database')
    parser.add_argument('--lemmas_query', type=str, dest='lemmas_query', help='Query for DB elements to lemmatize')
    parser.set_defaults(lemmas_query=None)
    args = parser.parse_args()

    main(resetDB=args.resetDB, importData=args.importData, importCitations=args.importCitations, 
         importAuthorship=args.importAuthorship, importEntities=args.importEntities,
         lemmatize=args.lemmatize, lemmas_query=args.lemmas_query)
