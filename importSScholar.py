"""
Created on Feb25 2019
@author: Jerónimo Arenas García

Import Semantic Scholar Database to Mongo DB

    * Creating database from downloaded gzip files

"""

import argparse
import configparser

from dbmanager.S2manager import S2manager

#from tika import parser as tikaparser
#from bs4 import BeautifulSoup

#from lemmatizer.ENlemmatizer import ENLemmatizer

def main(resetDB=False, importData=False, importCitations=False, importAuthorship=False):
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

    """


if __name__ == "__main__":

    parser = argparse.ArgumentParser(prog='importSScholar')    
    parser.add_argument('--resetDB', action='store_true', help='If activated, the database will be reset and re-created')
    parser.add_argument('--importData', action='store_true', help='If activated, import author and paper data')
    parser.add_argument('--importCitations', action='store_true', help='If activated, import citation data')
    parser.add_argument('--importAuthorship', action='store_true', help='If activated, import authorship data')
    args = parser.parse_args()

    main(resetDB=args.resetDB, importData=args.importData, 
         importCitations=args.importCitations, importAuthorship=args.importAuthorship)
