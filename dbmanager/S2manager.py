"""
Datamanager for importing Semantic Scholar papers
into a MySQL database

Created on Jul 7 2019

@author: Jerónimo Arenas García

"""

import os
import pandas as pd
import numpy as np
from progress.bar import Bar
import gzip
import json
import ipdb
from collections import Counter

from dbmanager.dbManager.base_dm_sql import BaseDMsql

import re

# try:
#     # UCS-4
#     regex = re.compile('[\U00010000-\U0010ffff]')
# except re.error:
#     # UCS-2
#     regex = re.compile('[\uD800-\uDBFF][\uDC00-\uDFFF]')


class S2manager(BaseDMsql):

    def createDBschema(self):
        """
        Create DB table structure
        """

        sql_cmd = """CREATE TABLE S2papers(

                        paperID INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                        S2paperID CHAR(40),
                        
                        title VARCHAR(300),
                        lowertitle VARCHAR(300),
                        paperAbstract TEXT,
                        entities TEXT,

                        s2PdfUrl VARCHAR(77),
                        pdfUrls MEDIUMTEXT,

                        year SMALLINT UNSIGNED,

                        venueID MEDIUMINT UNSIGNED,
                        journalNameID SMALLINT UNSIGNED,
                        journalVolume VARCHAR(300),
                        journalPages VARCHAR(100),

                        isDBLP TINYINT(1),
                        isMedline TINYINT(1),

                        doi VARCHAR(128),
                        doiUrl VARCHAR(128),
                        pmid VARCHAR(16),

                        ESP_contri TINYINT(1),
                        AIselection TINYINT(1),

                        LEMAS MEDIUMTEXT

                        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"""

        self._c.execute(sql_cmd)

        sql_cmd = """CREATE TABLE S2authors(

                        authorID VARCHAR(10) PRIMARY KEY,
                        ORCIDID VARCHAR(20),
                        ORCID_givename VARCHAR(40),
                        ORCID_familyname VARCHAR(100),
                        SCOPUSID BIGINT(20),
                        name VARCHAR(256),
                        influentialCitationCount SMALLINT UNSIGNED,
                        ESP_affiliation TINYINT(1)

                        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"""

        self._c.execute(sql_cmd)

        sql_cmd = """CREATE TABLE PaperAuthor(

                        paperID INT UNSIGNED,
                        authorID VARCHAR(10),

                        PRIMARY KEY (paperID, authorID),

                        FOREIGN KEY (paperID)  REFERENCES S2papers (paperID),
                        FOREIGN KEY (authorID) REFERENCES S2authors (authorID)

                        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"""

        self._c.execute(sql_cmd)

        sql_cmd = """CREATE TABLE citations(

                        paperID1 INT UNSIGNED,
                        paperID2 INT UNSIGNED,

                        PRIMARY KEY (paperID1, paperID2),

                        isInfluential TINYINT(1)

                        )"""

        self._c.execute(sql_cmd)

        sql_cmd = """CREATE TABLE S2venues(

                        venueID MEDIUMINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                        venue VARCHAR(300)
                        
                        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"""

        self._c.execute(sql_cmd)

        sql_cmd = """CREATE TABLE S2journals(

                        journalNameID SMALLINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                        journalName VARCHAR(300)
                        
                        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"""

        self._c.execute(sql_cmd)

        #Commit changes to database
        self._conn.commit()

        return

    def importData(self, data_files):
        """
        Import data from Semantic Scholar compressed data files
        available at the indicated location
        Only author and paper data will be imported
        """
        #STEP 1
        #We need to pass through all data files first to import venues and journalNames
        #We populate also the authors table
        all_venues = []
        all_journals = []
        author_counts = Counter()

        gz_files = [data_files+el for el in os.listdir(data_files) if el.startswith('s2-corpus')]
        bar = Bar('Extracting all venues, journalNames, and valid authors', max=len(gz_files))
        for fileno, gzf in enumerate(gz_files):
            bar.next()
            with gzip.open(gzf, 'rt', encoding='utf8') as f:
                papers_infile = f.read().replace('}\n{','},{')
                papers_infile = json.loads('['+papers_infile+']')

                # We extract venues and journals, getting rid of repetitions
                all_venues += [el['venue'] for el in papers_infile]
                all_venues = list(set(all_venues))
                all_journals += [el['journalName'] for el in papers_infile]
                all_journals = list(set(all_journals))

                list_authors = []
                for el in papers_infile:
                    if len(el['authors']):
                        for author in el['authors']:
                            if len(author['ids']):
                                list_authors.append((author['ids'][0], author['name']))

                author_counts = author_counts + Counter(list_authors)

        # We sort data in alphabetical order and insert in table
        all_venues.sort()
        all_journals.sort()
        self.insertInTable('S2venues', 'venue', [[el] for el in all_venues])
        self.insertInTable('S2journals', 'journalName', [[el] for el in all_journals])

        # We insert author data in table but we need to get rid of duplicated ids
        id_name_count = [[el[0], el[1], author_counts[el]] for el in author_counts]
        df = pd.DataFrame(id_name_count, columns=['id', 'name', 'counts'])
        #sort according to 'id' and then by 'counts'
        df.sort_values(by=['id', 'counts'], ascending=False, inplace=True)
        #We get rid of duplicates, keeping first element (max counts)
        df.drop_duplicates(subset='id', keep='first', inplace=True)
        self.insertInTable('S2authors', ['authorID', 'name'], df[['id', 'name']].values.tolist(), chunksize=100000, verbose=True)
        
        # We extract venues and journals as dictionaries for inserting new data in tables
        df = self.readDBtable('S2venues', selectOptions='venue, venueID')
        venues_dict = dict(df.values.tolist())
        df = self.readDBtable('S2journals', selectOptions='journalName, journalNameID')
        journals_dict = dict(df.values.tolist())

        # STEP 2
        # Now we need to read all files again, this time importing paper data
        # Note that papers are stored in database with a new index in addition to S2ID
        # this is useful because it will reduce indexing time and will also reduce
        # the size of citations and PaperAuthor table to be filled in STEP 3
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
                          paperEntry['title'].encode().decode('unicode_escape'),
                          paperEntry['title'].lower().encode().decode('unicode_escape'),
                          paperEntry['paperAbstract'].encode().decode('unicode_escape'),
                          '\t'.join(paperEntry['entities']),
                          paperEntry['s2PdfUrl'],
                          '\t'.join(paperEntry['pdfUrls']),
                          paperEntry['year'],
                          venues_dict[paperEntry['venue']],
                          journals_dict[paperEntry['journalName']],
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
                          # regex.sub(' ', paperEntry['title']),
                          # regex.sub(' ', paperEntry['title'].lower()),
                          # regex.sub(' ', paperEntry['paperAbstract']),
                          paperEntry['title'].encode().decode('unicode_escape'),
                          paperEntry['title'].lower().encode().decode('unicode_escape'),
                          paperEntry['paperAbstract'].encode().decode('unicode_escape'),
                          '\t'.join(paperEntry['entities']),
                          paperEntry['s2PdfUrl'],
                          '\t'.join(paperEntry['pdfUrls']),
                          9999,
                          venues_dict[paperEntry['venue']],
                          journals_dict[paperEntry['journalName']],
                          paperEntry['journalVolume'].strip(),
                          paperEntry['journalPages'].strip(),
                          ElementInList(paperEntry['sources'], 'DBLP'),
                          ElementInList(paperEntry['sources'], 'Medline'),
                          paperEntry['doi'],
                          paperEntry['doiUrl'],
                          paperEntry['pmid']
                          ]

            return paper_list

        gz_files = [data_files+el for el in os.listdir(data_files) if el.startswith('s2-corpus')]
        print('\n')
        bar = Bar('Filling in the paper table', max=len(gz_files))
        current_paper = 0
        for fileno, gzf in enumerate(gz_files):
            bar.next()
            with gzip.open(gzf, 'rt', encoding='utf8') as f:
                papers_infile = f.read().replace('}\n{','},{')
                papers_infile = json.loads('['+papers_infile+']')

                lista_papers = [process_paper(el) for el in papers_infile]

                #Populate tables with the new data
                self.insertInTable('S2papers', ['S2paperID', 'title', 'lowertitle', 
                    'paperAbstract', 'entities', 's2PdfUrl', 'pdfUrls', 'year',
                    'venueID', 'journalNameID', 'journalVolume', 'journalPages',
                    'isDBLP', 'isMedline', 'doi', 'doiUrl', 'pmid'], lista_papers,
                    chunksize=50000, verbose=True)

                # Alternative version loading data from file. Slightly faster
                # but there are some errors deriving from empty fields, etc

                # print('Writing data to file')
                # temp_file = os.path.join(data_files,'tmpfile.csv')
                # with open(temp_file, 'w', encoding='utf8') as fout:
                #     [fout.write('*****'.join(el).replace('\n', '. ') + '\n') for el in lista_papers]

                # sql_cmd = """
                #     LOAD DATA LOCAL INFILE '%s'
                #     INTO TABLE S2papers
                #     FIELDS TERMINATED BY '*****'
                #     LINES TERMINATED BY '\n'
                #     (S2paperID, title, lowertitle, paperAbstract, entities, s2PdfUrl, pdfUrls,
                #      year, venueID, journalNameID, journalVolume, journalPages,
                #      isDBLP, isMedline, doi, doiUrl, pmid)
                # """
                # sql_cmd = sql_cmd %(temp_file)
                # print(sql_cmd)
                # self._c.execute(sql_cmd)
                # self._conn.commit()
                # os.remove(temp_file)

        return

    def importCitations(self, data_files):
        """Imports Citation information"""

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

        # A pass through all data files is needed to fill in tables citations

        def process_Citations(paperEntry):
            """This function takes a dictionary with paper information as input
            and a list ready to insert in citations table
            """
            cite_list = []
            for el in paperEntry['outCitations']:
                try:
                    cite_list.append([S2_to_ID[paperEntry['id']], S2_to_ID[el]])
                except:
                    pass
            return cite_list

        gz_files = [data_files+el for el in os.listdir(data_files) if el.startswith('s2-corpus')]
        print('\n')
        bar = Bar('Filling in citations ...', max=len(gz_files))
        for fileno, gzf in enumerate(gz_files):
            bar.next()
            with gzip.open(gzf, 'rt', encoding='utf8') as f:
                papers_infile = f.read().replace('}\n{','},{')
                papers_infile = json.loads('['+papers_infile+']')

                lista_citas = []
                for paper in papers_infile:
                    lista_citas += process_Citations(paper)

                #Populate table with the new data
                self.insertInTable('citations', ['paperID1', 'paperID2'], lista_citas, chunksize=100000, verbose=True)

        return

    def importAuthorship(self, data_files):
        """Imports Authorship information"""

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
        print('\n')
        bar = Bar('Filling in authorship information ... ', max=len(gz_files))
        for fileno, gzf in enumerate(gz_files):
            bar.next()
            with gzip.open(gzf, 'rt', encoding='utf8') as f:
                papers_infile = f.read().replace('}\n{','},{')
                papers_infile = json.loads('['+papers_infile+']')

                lista_author_paper = []
                for paper in papers_infile:
                    lista_author_paper += process_Authorship(paper)
                    
                #Populate tables with the new data
                self.insertInTable('PaperAuthor', ['paperID', 'authorID'], lista_author_paper, chunksize=100000, verbose=True)

        return

