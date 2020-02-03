"""
Datamanager for importing Semantic Scholar papers
into a MySQL database

Created on Jul 7 2019

@author: Jerónimo Arenas García

"""

import os
import pandas as pd
import numpy as np
from tqdm import *
import gzip
import json
import ipdb
import time
import langid
#from utils import get_size
from collections import Counter
from multiprocessing import Pool

from dbmanager.dbManager.base_dm_sql import BaseDMsql

import re

try:
    # UCS-4
    regex = re.compile('[\U00010000-\U0010ffff]')
except re.error:
    # UCS-2
    regex = re.compile('[\uD800-\uDBFF][\uDC00-\uDFFF]')

"""Some functions need to be defined outside the class for allowing 
   parallel processing of the Semantic Scholar files. It is necessary
   to do so to make pickle serialization work"""
def ElementInList(source_list, search_string):
    if search_string in source_list:
        return 1
    else:
        return 0

def process_paper(paperEntry):
    """This function takes a dictionary with paper information as input
    and returns a list to insert in S2papers
    """
    if 'year' in paperEntry.keys():
        paper_list = [paperEntry['id'],
                          regex.sub(' ', paperEntry['title']),
                          regex.sub(' ', paperEntry['title'].lower()),
                          regex.sub(' ', paperEntry['paperAbstract']),
                          '\t'.join(paperEntry['entities']),
                          '\t'.join(paperEntry['fieldsOfStudy']),
                          paperEntry['s2PdfUrl'],
                          '\t'.join(paperEntry['pdfUrls']),
                          paperEntry['year'],
                          paperEntry['journalVolume'].strip(),
                          paperEntry['journalPages'].strip(),
                          ElementInList(paperEntry['sources'], 'DBLP'),
                          ElementInList(paperEntry['sources'], 'Medline'),
                          paperEntry['doi'],
                          paperEntry['doiUrl'],
                          paperEntry['pmid']
                          ]
    else:
        paper_list = [paperEntry['id'],
                          regex.sub(' ', paperEntry['title']),
                          regex.sub(' ', paperEntry['title'].lower()),
                          regex.sub(' ', paperEntry['paperAbstract']),
                          '\t'.join(paperEntry['entities']),
                          '\t'.join(paperEntry['fieldsOfStudy']),
                          paperEntry['s2PdfUrl'],
                          '\t'.join(paperEntry['pdfUrls']),
                          9999,
                          paperEntry['journalVolume'].strip(),
                          paperEntry['journalPages'].strip(),
                          ElementInList(paperEntry['sources'], 'DBLP'),
                          ElementInList(paperEntry['sources'], 'Medline'),
                          paperEntry['doi'],
                          paperEntry['doiUrl'],
                          paperEntry['pmid']
                          ]

    return paper_list

def process_paperFile(gzfile):
    """Process Semantic Scholar gzip file, and extract a list of
    journals, a list of venues, a list of fields of study, and a
    list wih paper information to save in the S2papers table
    Args:
    :param gzfil: String containing the name of the file to process

    Returns:
    A list containing 3 lists: papers in file, unique journals in file,
    unique venues in file, unique fields in file
    """
    with gzip.open(gzfile, 'rt', encoding='utf8') as f:
        papers_infile = f.read().replace('}\n{','},{')
    
    papers_infile = json.loads('['+papers_infile+']')

    # We extract venues and journals, getting rid of repetitions
    thisfile_venues = [el['venue'] for el in papers_infile]
    thisfile_venues = list(set(thisfile_venues))
    thisfile_journals = [el['journalName'] for el in papers_infile]
    thisfile_journals = list(set(thisfile_journals))
    # We extract all fields, and flatten before getting rid of repetitions
    # Flatenning is necessary because each paper has a list of fields
    thisfile_fields = [el['fieldsOfStudy'] for el in papers_infile]
    thisfile_fields = [item for sublist in thisfile_fields for item in sublist]
    thisfile_fields = list(set(thisfile_fields))
    """
    Entities are not included in current Semantic Scholar versions
    # We extract all entities, and flatten before getting rid of repetitions
    # Flatenning is necessary because each paper has a list of entities
    thisfile_entities = [el['entities'] for el in papers_infile]
    thisfile_entities = [item for sublist in thisfile_entities for item in sublist]
    thisfile_entities = list(set(thisfile_entities))"""    # We extract fields for the S2papers table
    lista_papers = [process_paper(el) for el in papers_infile]

    return [lista_papers, thisfile_venues, thisfile_journals, thisfile_fields]


class S2manager(BaseDMsql):

    def createDBschema(self):
        """
        Create DB table structure
        """
        for sql_cmd in schema:

            self._c.execute(sql_cmd)

        #Commit changes to database
        self._conn.commit()

        return

    def createDBindices(self):
        """
        Create DB table structure
        """
        for sql_cmd in indices:
            print('Creating index:', sql_cmd)
            self._c.execute(sql_cmd)

        #Commit changes to database
        self._conn.commit()

        return

    def importPapers(self, data_files, ncpu, chunksize=100000):
        """
        Import data from Semantic Scholar compressed data files
        available at the indicated location
        Only paper data will be imported
        """
        #STEP 1
        #Read and Insert paper data
        #We need to pass through all data files first to import venues, journalNames, entities
        #and fields. We populate also the S2papers table
        all_venues = []
        all_journals = []
        #all_entities = []
        all_fields = []

        print('Filling in table S2papers')

        gz_files = sorted([data_files+el for el in os.listdir(data_files) if el.startswith('s2-corpus')])

        if ncpu:
            #Parallel processing
            with Pool(ncpu) as p:
                with tqdm(total = len(gz_files)) as pbar:
                    for file_data in p.imap(process_paperFile, gz_files):
                        pbar.update()
                        #Populate tables with the new data
                        self.insertInTable('S2papers', ['S2paperID', 'title', 'lowertitle', 
                                'paperAbstract', 'entities', 'fieldsOfStudy', 's2PdfUrl',
                                'pdfUrls', 'year', 'journalVolume', 'journalPages',
                                'isDBLP', 'isMedline', 'doi', 'doiUrl', 'pmid'],
                                file_data[0], chunksize=chunksize, verbose=False)
                        all_venues += file_data[1]
                        all_venues = list(set(all_venues))
                        all_journals += file_data[2]
                        all_journals = list(set(all_journals))
                        all_fields += file_data[3]
                        all_fields = list(set(all_fields))
                        
            pbar.close()
            p.close()
            p.join()

        else:

            pbar = tqdm(total=len(gz_files))

            for gzf in gz_files:
                pbar.update(1)
                
                file_data = process_paperFile(gzf)
                #Populate tables with the new data
                self.insertInTable('S2papers', ['S2paperID', 'title', 'lowertitle', 
                                'paperAbstract', 'entities', 'fieldsOfStudy', 's2PdfUrl',
                                'pdfUrls', 'year', 'journalVolume', 'journalPages',
                                'isDBLP', 'isMedline', 'doi', 'doiUrl', 'pmid'],
                                file_data[0], chunksize=chunksize, verbose=False)
                all_venues += file_data[1]
                all_venues = list(set(all_venues))
                all_journals += file_data[2]
                all_journals = list(set(all_journals))
                all_fields += file_data[3]
                all_fields = list(set(all_fields))

            pbar.close()

        # We sort data in alphabetical order and insert in table
        all_venues.sort()
        all_journals.sort()
        all_fields.sort()
        print('Filling in tables S2venues, S2journals and S2fields')
        self.insertInTable('S2venues', 'venueName', [[el] for el in all_venues])
        self.insertInTable('S2journals', 'journalName', [[el] for el in all_journals])
        self.insertInTable('S2fields', 'fieldName', [[el] for el in all_fields])
        """if len(all_entities):
            all_entities.sort()
            self.insertInTable('S2entities', 'entityname', [[el] for el in all_entities])
        """

        return
        
    def importCitations(self, data_files, chunksize):
        """Imports Citation information"""

        # First, we need to create a dictionary to access the paperID 
        # corresponding to each S2paperID
        print('Generating S2 to ID dictionary')

        S2_to_ID = {}

        for df in self.readDBchunks('S2papers', 'paperID', chunksize=chunksize,
                        selectOptions='paperID, S2paperID', verbose=True):
            ID_to_S2_list = df.values.tolist()
            S2_to_ID_list = [[el[1], el[0]] for el in ID_to_S2_list]
            aux_dict = dict(S2_to_ID_list)
            S2_to_ID = {**S2_to_ID, **aux_dict}

        # A pass through all data files is needed to fill in tables citations

        def process_Citations(paperEntry):
            """This function takes a dictionary with paper information as input
            and returns a list ready to insert in citations table
            """
            cite_list = []
            for el in paperEntry['outCitations']:
                try:
                    cite_list.append([S2_to_ID[paperEntry['id']], S2_to_ID[el]])
                except:
                    pass
            return cite_list

        gz_files = sorted([data_files+el for el in os.listdir(data_files) if el.startswith('s2-corpus')])
        print('Filling in citations ...')
        bar = tqdm(total=len(gz_files))
        for gzf in gz_files:
            bar.update(1)
            with gzip.open(gzf, 'rt', encoding='utf8') as f:
                papers_infile = f.read().replace('}\n{','},{')
                papers_infile = json.loads('['+papers_infile+']')

                lista_citas = []
                for paper in papers_infile:
                    lista_citas += process_Citations(paper)

                #Populate table with the new data
                self.insertInTable('citations', ['paperID1', 'paperID2'], lista_citas, chunksize=chunksize, verbose=True)
        bar.close()

        del S2_to_ID

        return

    def importFields(self, data_files, chunksize):
        """Imports Journals, Volumes, and Field of Study associated to each paper"""

        # We extract venues, journals and fields as dictionaries for inserting new data in tables
        df = self.readDBtable('S2venues', selectOptions='venueName, venueID')
        venues_dict = dict(df.values.tolist())
        df = self.readDBtable('S2journals', selectOptions='journalName, journalID')
        journals_dict = dict(df.values.tolist())
        df = self.readDBtable('S2fields', selectOptions='fieldName, fieldID')
        fields_dict = dict(df.values.tolist())

        #Now, we need to create a dictionary to access the paperID 
        # corresponding to each S2paperID
        print('Generating S2 to ID dictionary')

        S2_to_ID = {}

        for df in self.readDBchunks('S2papers', 'paperID', chunksize=chunksize,
                        selectOptions='paperID, S2paperID', verbose=True):
            ID_to_S2_list = df.values.tolist()
            S2_to_ID_list = [[el[1], el[0]] for el in ID_to_S2_list]
            aux_dict = dict(S2_to_ID_list)
            S2_to_ID = {**S2_to_ID, **aux_dict}


        # A pass through all data files is needed to extract the data of interest
        # and fill in the tables

        def process_Fields(paperEntry):
            """This function takes a dictionary with paper information as input
            and returns lists ready to insert in the corresponding tables
            """
            fields_list = [[S2_to_ID[paperEntry['id']], fields_dict[el]] 
                            for el in paperEntry['fieldsOfStudy']]
            journal_list = [[S2_to_ID[paperEntry['id']], journals_dict[paperEntry['journalName']]]]
            venues_list = [[S2_to_ID[paperEntry['id']], venues_dict[paperEntry['venue']]]]

            return fields_list, journal_list, venues_list

        gz_files = sorted([data_files+el for el in os.listdir(data_files) if el.startswith('s2-corpus')])
        print('Filling in venue, journal and field of study data ...')
        bar = tqdm(total=len(gz_files))
        for gzf in gz_files:
            bar.update(1)
            with gzip.open(gzf, 'rt', encoding='utf8') as f:
                papers_infile = f.read().replace('}\n{','},{')
                papers_infile = json.loads('['+papers_infile+']')

                lista_fields = []
                lista_journals = []
                lista_venues = []
                for paper in papers_infile:
                    try:
                        all_lists = process_Fields(paper)
                    except:
                        all_lists = [[],[],[]]
                    lista_fields += all_lists[0]
                    lista_journals += all_lists[1]
                    lista_venues += all_lists[2]

                #Populate tables
                self.insertInTable('paperField', ['paperID', 'fieldID'], lista_fields, chunksize=chunksize, verbose=True)
                self.insertInTable('paperVenue', ['paperID', 'venueID'], lista_venues, chunksize=chunksize, verbose=True)
                self.insertInTable('paperJournal', ['paperID', 'journalID'], lista_journals, chunksize=chunksize, verbose=True)
        bar.close()

        del S2_to_ID

        return
        return

    def importAuthors(self, data_files):
        """Imports Authorship information"""
        """
                thisfile_authors = []
                thisfile_authors2 = []
                for el in papers_infile:
                    if len(el['authors']):
                        for author in el['authors']:
                            if len(author['ids']):
                                thisfile_authors.append((int(author['ids'][0]), author['name']))
                author_counts = author_counts + Counter(thisfile_authors)
                print('Finished processing file:', gzfile)
                #print('Number of authors (str):', len(author_counts), '(', get_size(author_counts)/1e9, 'g )')
                #print('Number of authors (int):', len(author_counts2), '(', get_size(author_counts2)/1e9, 'g )')
                #print('Max author id this far:', max([el[0] for el in author_counts2.keys()]))
                #print('Last iteration time:', time.time()-start)
                #start=time.time()

        # We insert author data in table but we need to get rid of duplicated ids
        id_name_count = [[el[0], el[1], author_counts[el]] for el in author_counts]
        df = pd.DataFrame(id_name_count, columns=['id', 'name', 'counts'])
        #sort according to 'id' and then by 'counts'
        df.sort_values(by=['id', 'counts'], ascending=False, inplace=True)
        #We get rid of duplicates, keeping first element (max counts)
        df.drop_duplicates(subset='id', keep='first', inplace=True)
        self.insertInTable('S2authors', ['authorID', 'name'], df[['id', 'name']].values.tolist(), chunksize=100000, verbose=True)

        """


        # First, we need to create a dictionary to access the paperID 
        # corresponding to each S2paperID
        print('Generating S2 to ID dictionary')

        chunksize = 100000
        cont = 0
        S2_to_ID = {}
        df = self.readDBtable('S2papers', limit=chunksize, selectOptions='paperID, S2paperID',
                               filterOptions='paperID>0', orderOptions='paperID ASC')
        while len(df):
            cont = cont+len(df)
            #Next time, we will read from the largest retrieved ID. This is the
            #last element of the dataframe, given that we requested an ordered df
            smallest_id = df['paperID'][0]
            largest_id = df['paperID'][len(df)-1]
            print('Number of elements processed:', cont)
            print('Last Id read:', largest_id)
            ID_to_S2_list = df.values.tolist()
            S2_to_ID_list = [[el[1], el[0]] for el in ID_to_S2_list]
            aux_dict = dict(S2_to_ID_list)
            S2_to_ID = {**S2_to_ID, **aux_dict}
            df = self.readDBtable('S2papers', limit=chunksize, selectOptions='paperID, S2paperID',
                    filterOptions = 'paperID>'+str(largest_id), orderOptions='paperID ASC')

        # A pass through all data files is needed to fill in table PaperAuthor

        def process_Authorship(paperEntry):
            """This function takes a dictionary with paper information as input
            and returns a list ready to insert in PaperAuthor
            """
            author_list = [[S2_to_ID[paperEntry['id']], el['ids'][0]] 
                            for el in paperEntry['authors'] if len(el['ids'])]

            return author_list

        gz_files = [data_files+el for el in os.listdir(data_files) if el.startswith('s2-corpus')]
        print('Filling in authorship information ... ')
        bar = tqdm.tqdm(total=len(gz_files))
        for fileno, gzf in enumerate(gz_files):
            bar.update(1)
            with gzip.open(gzf, 'rt', encoding='utf8') as f:
                papers_infile = f.read().replace('}\n{','},{')
                papers_infile = json.loads('['+papers_infile+']')

                lista_author_paper = []
                for paper in papers_infile:
                    lista_author_paper += process_Authorship(paper)
                    
                #Populate tables with the new data
                self.insertInTable('PaperAuthor', ['paperID', 'authorID'], lista_author_paper, chunksize=100000, verbose=True)

        return

    def importEntities(self, data_files):
        """Imports Entities associated to each paper"""

        # First, we need to create a dictionary to access the paperID 
        # corresponding to each S2paperID
        print('Generating S2 to ID dictionary')

        chunksize = 100000
        cont = 0
        S2_to_ID = {}
        df = self.readDBtable('S2papers', limit=chunksize, selectOptions='paperID, S2paperID',
                               filterOptions='paperID>0', orderOptions='paperID ASC')
        while len(df):
            cont = cont+len(df)
            #Next time, we will read from the largest retrieved ID. This is the
            #last element of the dataframe, given that we requested an ordered df
            smallest_id = df['paperID'][0]
            largest_id = df['paperID'][len(df)-1]
            print('Number of elements processed:', cont)
            print('Last Id read:', largest_id)
            ID_to_S2_list = df.values.tolist()
            S2_to_ID_list = [[el[1], el[0]] for el in ID_to_S2_list]
            aux_dict = dict(S2_to_ID_list)
            S2_to_ID = {**S2_to_ID, **aux_dict}
            df = self.readDBtable('S2papers', limit=chunksize, selectOptions='paperID, S2paperID',
                    filterOptions = 'paperID>'+str(largest_id), orderOptions='paperID ASC')

        # We extract also a dictionary with entities values
        df = self.readDBtable('S2entities', selectOptions='entityname, entityID')
        entities_dict = dict(df.values.tolist())        

        # A pass through all data files is needed to fill in table PaperAuthor

        def process_Entities(paperEntry):
            """This function takes a dictionary with paper information as input
            and returns a list ready to insert in PaperEntity
            """
            entities_list = [[S2_to_ID[paperEntry['id']], entities_dict[el]] 
                            for el in paperEntry['entities']]

            return entities_list

        gz_files = [data_files+el for el in os.listdir(data_files) if el.startswith('s2-corpus')]
        print('Filling in entities information ... ')
        bar = tqdm.tqdm(total=len(gz_files))
        for fileno, gzf in enumerate(gz_files):
            bar.update(1)
            with gzip.open(gzf, 'rt', encoding='utf8') as f:
                papers_infile = f.read().replace('}\n{','},{')
                papers_infile = json.loads('['+papers_infile+']')

                lista_entity_paper = []
                for paper in papers_infile:
                    lista_entity_paper += process_Entities(paper)
                lista_entity_paper = list(set([tuple(el) for el in lista_entity_paper]))

                #Populate tables with the new data
                self.insertInTable('PaperEntity', ['paperID', 'entityID'], lista_entity_paper, chunksize=100000, verbose=True)

        return

"""===============================================================================
==================================================================================

         *******   *******   *      *   *******   *       *      *
         *         *         *      *   *         * *   * *     * *
         *******   *         ********   ****      *   *   *    *   *
               *   *         *      *   *         *       *   *******
         *******   *******   *      *   *******   *       *   *     *

==================================================================================
==============================================================================="""

schema = [

"""CREATE TABLE S2papers(

    paperID INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    S2paperID CHAR(40),
                        
    title TEXT,
    lowertitle TEXT,
    paperAbstract TEXT,
    entities TEXT,
    fieldsOfStudy TEXT,

    s2PdfUrl VARCHAR(77),
    pdfUrls MEDIUMTEXT,

    year SMALLINT UNSIGNED,

    journalVolume VARCHAR(300),
    journalPages VARCHAR(100),

    isDBLP TINYINT(1),
    isMedline TINYINT(1),

    doi VARCHAR(128),
    doiUrl VARCHAR(256),
    pmid VARCHAR(16),

    ESP_contri TINYINT(1),
    AIselection TINYINT(1),

    langid VARCHAR(3),
    LEMAS MEDIUMTEXT

    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci""",


"""CREATE TABLE S2authors(

	authorID INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    S2authorID INT UNSIGNED,
    orcidID VARCHAR(20),
    orcidGivenName VARCHAR(40),
    orcidFamilyName VARCHAR(100),
    scopusID BIGINT(20),
    name VARCHAR(256),
    influentialCitationCount SMALLINT UNSIGNED,
    ESP_affiliation TINYINT(1)

    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci""",


"""CREATE TABLE S2entities(

    entityID INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    entityName VARCHAR(120)

    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci""",


"""CREATE TABLE S2fields(

    fieldID INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    fieldName VARCHAR(32)

    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci""",


"""CREATE TABLE S2venues(

    venueID MEDIUMINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    venueName VARCHAR(320)
                        
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci""",


"""CREATE TABLE S2journals(

    journalID MEDIUMINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    journalName VARCHAR(320)
                        
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci""",


"""CREATE TABLE paperAuthor(

    #ID UNSIGNED INT AUTO_INCREMENT UNIQUE FIRST,

    paperAuthorID INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    paperID INT UNSIGNED,
    authorID INT UNSIGNED,

    FOREIGN KEY (paperID)  REFERENCES S2papers (paperID),
    FOREIGN KEY (authorID) REFERENCES S2authors (authorID)

    )""",


"""CREATE TABLE paperEntity(

	paperEntityID INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    paperID INT UNSIGNED,
    entityID INT UNSIGNED,

    FOREIGN KEY (paperID)  REFERENCES S2papers (paperID),
    FOREIGN KEY (entityID) REFERENCES S2entities (entityID)

    )""",


"""CREATE TABLE paperField(

    paperFieldID INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    paperID INT UNSIGNED,
    fieldID INT UNSIGNED

    )""",

"""CREATE TABLE paperVenue(

    paperVenueID INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    paperID INT UNSIGNED,
    venueID INT UNSIGNED

    )""",

"""CREATE TABLE paperJournal(

    paperJournalID INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    paperID INT UNSIGNED,
    journalID INT UNSIGNED

    )""",

"""CREATE TABLE citations(

    citationID INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    paperID1 INT UNSIGNED,
    paperID2 INT UNSIGNED,

    isInfluential TINYINT(1),
    MethodIntent TINYINT(1),
    BackgrIntent TINYINT(1),
    ResultIntent TINYINT(1)

    )"""

]

indices = [

'CREATE INDEX S2id on S2papers (S2paperID)',
'CREATE INDEX paper1 on citations (paperID1)',
'CREATE INDEX paper2 on citations (paperID2)'

]