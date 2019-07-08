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
import numpy as np
import ipdb

from dbmanager.S2manager import S2manager

#from tika import parser as tikaparser
#from bs4 import BeautifulSoup

#from lemmatizer.ENlemmatizer import ENLemmatizer

def main(resetDB=False, importData=False):
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
    #3. If activated, data will be imported from S2 data files
    if importData:
        print('Importing data ...')

        #We need to pass through all data files first to import venues and journalNames
        all_venues = []
        all_journals = []

        gz_files = [data_files+el for el in os.listdir(data_files) if el.startswith('s2-corpus')]
        bar = Bar('Extracting all venues and journalNames', max=len(gz_files))
        for fileno, gzf in enumerate(gz_files[:3]):
            bar.next()
            with gzip.open(gzf, 'rt', encoding='utf8') as f:
                papers_infile = f.read().replace('}\n{','},{')
                papers_infile = json.loads('['+papers_infile+']')

                # We extract venues and journals, getting rid of repetitions
                all_venues += [el['venue'] for el in papers_infile]
                ipdb.set_trace()
                all_venues = list(set(all_venues))
                all_journals += [el['journalName'] for el in papers_infile]
                all_journals = list(set(all_journals))

        # We sort data in alphabetical order and insert in table
        all_venues.sort()
        all_journals.sort()
        DB.insertInTable('S2venues', 'venue', [[el] for el in all_venues])
        DB.insertInTable('S2journals', 'journalName', [[el] for el in all_journals])




    """
    if resetDB:
        #Now, we start popullating the collection with data



        gz_files = [data_files+el for el in os.listdir(data_files) if el.startswith('s2-corpus')]
        bar = Bar('Inserting papers in Mongo Database', max=len(gz_files))
        for fileno, gzf in enumerate(gz_files):
            bar.next()
            with gzip.open(gzf, 'rt', encoding='utf8') as f:
                papers_infile = f.read().replace('}\n{','},{')
                papers_infile = json.loads('['+papers_infile+']')

                n_id.append((fileno, sum([1 for el in papers_infile if 'id' in el.keys()])))
                longest_id.append((fileno, np.max([len(el['id']) for el in papers_infile])))
                n_title.append((fileno, sum([1 for el in papers_infile if 'title' in el.keys()]) ))
                longest_title.append((fileno, np.max([len(el['title']) for el in papers_infile])))
                n_paperAbstract.append((fileno, sum([1 for el in papers_infile if 'paperAbstract' in el.keys()]) ))
                longest_abstract.append((fileno, np.max([len(el['paperAbstract']) for el in papers_infile])))
                n_entities.append((fileno, sum([1 for el in papers_infile if 'entities' in el.keys()]) ))
                longest_entities.append((fileno, np.max([len('\t'.join(el['entities'])) for el in papers_infile])))
                n_s2PdfUrl.append((fileno, sum([1 for el in papers_infile if 's2PdfUrl' in el.keys()]) ))
                longest_s2PdfUrl.append((fileno, np.max([len(el['s2PdfUrl']) for el in papers_infile])))
                n_pdfUrls.append((fileno, sum([1 for el in papers_infile if 'pdfUrls' in el.keys()]) ))
                longest_pdfUrls.append((fileno, np.max([len('\t'.join(el['pdfUrls'])) for el in papers_infile])))
                n_authors.append((fileno, sum([1 for el in papers_infile if 'authors' in el.keys()]) ))
                lg_author_name = 0
                lg_author_id = 0
                for el in papers_infile:
                    if 'authors' in el.keys():
                        for el2 in el['authors']:
                            if len(el2['name'])>lg_author_name:
                                lg_author_name = len(el2['name'])
                            if len(el2['ids'])==1:
                                if len(el2['ids'][0])>lg_author_id:
                                    lg = len(el2['ids'][0])
                            if len(el2['ids'])>1:
                                print(el2)
                longest_author_name.append((fileno, lg_author_name))
                longest_author_id.append((fileno, lg_author_id))
                n_inCitations.append((fileno, sum([1 for el in papers_infile if 'inCitations' in el.keys()]) ))
                n_outCitations.append((fileno, sum([1 for el in papers_infile if 'outCitations' in el.keys()]) ))
                n_year.append((fileno, sum([1 for el in papers_infile if 'year' in el.keys()]) ))
                max_year.append((fileno, np.max([el['year'] for el in papers_infile if 'year' in el.keys()])))
                min_year.append((fileno, np.min([el['year'] for el in papers_infile if 'year' in el.keys()])))
                n_venue.append((fileno, sum([1 for el in papers_infile if 'venue' in el.keys()]) ))
                all_venues += [el['venue'] for el in papers_infile]
                all_venues = list(set(all_venues))
                n_journalName.append((fileno, sum([1 for el in papers_infile if 'journalName' in el.keys()]) ))
                all_journals += [el['journalName'] for el in papers_infile]
                all_journals = list(set(all_journals))
                n_journalVolume.append((fileno, sum([1 for el in papers_infile if 'journalVolume' in el.keys()]) ))
                longest_journalVolume.append((fileno, np.max([len(el['journalVolume'].strip()) for el in papers_infile])))
                n_journalPages.append((fileno, sum([1 for el in papers_infile if 'journalPages' in el.keys()]) ))
                longest_journalPages.append((fileno, np.max([len(el['journalPages'].strip()) for el in papers_infile])))
                n_sources.append((fileno, sum([1 for el in papers_infile if 'sources' in el.keys()]) ))
                lista_vacia = []
                for el in papers_infile:
                    lista_vacia += el['sources']
                lista_vacia = list(set(lista_vacia))
                all_sources += lista_vacia
                all_sources = list(set(lista_vacia))
                n_doi.append((fileno, sum([1 for el in papers_infile if 'doi' in el.keys()]) ))
                longest_doi.append((fileno, np.max([len(el['doi']) for el in papers_infile])))
                n_doiUrl.append((fileno, sum([1 for el in papers_infile if 'doiUrl' in el.keys()]) ))
                longest_doiUrl.append((fileno, np.max([len(el['doiUrl']) for el in papers_infile])))
                n_pmid.append((fileno, sum([1 for el in papers_infile if 'pmid' in el.keys()]) ))
                longest_pmid.append((fileno, np.max([len(el['pmid']) for el in papers_infile])))



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

    """


if __name__ == "__main__":

    parser = argparse.ArgumentParser(prog='importSScholar')    
    parser.add_argument('--resetDB', action='store_true', help='If activated, the database will be reset and re-created')
    parser.add_argument('--importData', action='store_true', help='If activated, import data')
    args = parser.parse_args()

    main(resetDB=args.resetDB, importData=args.importData)
