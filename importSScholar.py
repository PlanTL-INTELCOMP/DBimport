"""
Created on Feb25 2019
@author: Jerónimo Arenas García

Import Semantic Scholar Database to Mongo DB

    * Creating database from downloaded gzip files

"""

import argparse
import configparser
from progress.bar import Bar
import os
import gzip
import json
import ipdb
#from tika import parser as tikaparser
#from bs4 import BeautifulSoup

#from lemmatizer.ENlemmatizer import ENLemmatizer

def main(resetDB=False):
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
    dbNAME = 'db_Pu_S2'

    #########################
    # Datafiles
    #
    data_files = cf.get('SemanticScholar', 'data_files')

    if resetDB:
        #Now, we start popullating the collection with data
        gz_files = [data_files+el for el in os.listdir(data_files) if el.startswith('s2-corpus')]
        bar = Bar('Inserting papers in Mongo Database', max=len(gz_files))
        for gzf in gz_files:
            bar.next()
            with gzip.open(gzf, 'rt', encoding='utf8') as f:
                papers_infile = f.read().replace('}\n{','},{')
                papers_infile = json.loads('['+papers_infile+']')
                ipdb.set_trace()
        bar.finish()


    else:
        print(data_files, 'false')

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
    args = parser.parse_args()

    main(resetDB=args.resetDB)
