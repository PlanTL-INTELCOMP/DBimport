"""
Created on Feb25 2019
@author: Jerónimo Arenas García

Import Semantic Scholar Database to Mongo DB

    * Creating database from downloaded gzip files

"""

import argparse
import configparser

from dbmanager.S2manager import S2manager
from lemmatizer.ENlemmatizer import ENLemmatizer

import ipdb
import time

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
        #read and process in chunks of 100000 articles
        chunksize = 500
        cont = 0
        lemmas_server = cf.get('Lemmatizer', 'server')
        stw_file = cf.get('Lemmatizer', 'stw_file')
        dict_eq_file = cf.get('Lemmatizer', 'dict_eq_file')
        POS = cf.get('Lemmatizer', 'POS')

        #Initialize lemmatizer
        ENLM = ENLemmatizer(lemmas_server, stw_file, dict_eq_file)
        selectOptions = 'paperID, title, paperAbstract'
        if lemmas_query:
            filterOptions = 'paperID>0 AND ' + lemmas_query
        else:
            filterOptions = 'paperID>0'
        init_time = time.time()
        df = DB.readDBtable('S2papers', limit=chunksize, selectOptions=selectOptions,
                 filterOptions = filterOptions, orderOptions='paperID ASC')
        while (len(df) and cont<5000):
            cont = cont+len(df)
            
            #Next time, we will read from the largest paperID. This is the
            #last element of the dataframe, given that we requested an ordered df
            largest_id = df['paperID'][len(df)-1]
            print('Number of articles processed:', cont)
            print('Last Article Id read:', largest_id)
            for el in df.values.tolist():
                lemas = ENLM.extractEnglishSentences(el[1]+' '+el[2])
                lemas = ENLM.lemmatize(lemas, POS=POS, removenumbers=True)
            #     self.insertInTable('SCOPUS', columns, values)
            if lemmas_query:
                filterOptions = 'paperID>' + str(largest_id) + ' AND ' + lemmas_query
            else:
                filterOptions = 'paperID>' + str(largest_id)
            df = DB.readDBtable('S2papers', limit=chunksize, selectOptions=selectOptions,
                 filterOptions = filterOptions, orderOptions='paperID ASC')
        elapsed_time = time.time() - init_time
        print('Elapsed Time (seconds):', elapsed_time)

    return







    # # ####################################################
    # # 3. Lematización de textos en inglés
    # if lemmatize:
    #     enLM = ENLemmatizer(generic_stw, specific_stw)
    #     df = DB.readDBtable('projects',limit=None,selectOptions='rcn, title, objective, report')
    #     allprojects = df.values.tolist()

    #     #Chunks for monitoring progress and writing in the database
    #     lchunk = 10
    #     nproyectos = len(allprojects)
    #     bar = Bar('Lemmatizing English Descriptions', max=1+nproyectos/lchunk)

    #     allLEMAS = []
    #     for index,x in enumerate(allprojects):
    #         if not (index+1)%lchunk:
    #             DB.setField('projects', 'rcn', 'LEMAS_UC3M_ENG', allLEMAS)
    #             allLEMAS = []
    #             bar.next()
    #         allLEMAS.append((x[0], enLM.processENstr(x[1]) + ' ***** ' + enLM.processENstr(x[2]) + \
    #                          ' ***** ' + enLM.processENstr(x[3]) ))
    #     bar.finish()
    #     DB.setField('proyectos', 'rcn', 'LEMAS_UC3M_ENG', allLEMAS)


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
