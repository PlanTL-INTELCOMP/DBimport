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

try:
    # UCS-4
    regex = re.compile('[\U00010000-\U0010ffff]')
except re.error:
    # UCS-2
    regex = re.compile('[\uD800-\uDBFF][\uDC00-\uDFFF]')


class S2manager(BaseDMsql):

    def createDBschema(self):
        """
        Create DB table structure
        """

        sql_cmd = """CREATE TABLE S2papers(

                        paperID CHAR(40) CHARACTER SET utf8 PRIMARY KEY,
                        
                        title VARCHAR(300) CHARACTER SET utf8,
                        lowertitle VARCHAR(300) CHARACTER SET utf8,
                        paperAbstract TEXT CHARACTER SET utf8,
                        entities TEXT CHARACTER SET utf8,

                        s2PdfUrl VARCHAR(77) CHARACTER SET utf8,
                        pdfUrls MEDIUMTEXT CHARACTER SET utf8,

                        year SMALLINT UNSIGNED,

                        venueID MEDIUMINT UNSIGNED,
                        journalNameID SMALLINT UNSIGNED,
                        journalVolume VARCHAR(300) CHARACTER SET utf8,
                        journalPages VARCHAR(100) CHARACTER SET utf8,

                        isDBLP TINYINT(1),
                        isMedline TINYINT(1),

                        doi VARCHAR(128) CHARACTER SET utf8,
                        doiUrl VARCHAR(128) CHARACTER SET utf8,
                        pmid VARCHAR(16) CHARACTER SET utf8,

                        ESP_contri TINYINT(1),
                        AIselection TINYINT(1),

                        LEMAS MEDIUMTEXT,
                        LEMAS_STW MEDIUMTEXT

                        )"""

        self._c.execute(sql_cmd)

        sql_cmd = """CREATE TABLE S2authors(

                        authorID VARCHAR(10) CHARACTER SET utf8 PRIMARY KEY,
                        name VARCHAR(256) CHARACTER SET utf8,
                        influentialCitationCount SMALLINT UNSIGNED,
                        ESP_affiliation TINYINT(1)

                        )"""

        self._c.execute(sql_cmd)

        # sql_cmd = """CREATE TABLE PaperAuthor(

        #                 paperID CHAR(40) CHARACTER SET utf8,
        #                 authorID VARCHAR(10) CHARACTER SET utf8,

        #                 PRIMARY KEY (paperID, authorID),

        #                 FOREIGN KEY (paperID)  REFERENCES S2papers (paperID),
        #                 FOREIGN KEY (authorID) REFERENCES S2authors (authorID)

        #                 )"""

        # self._c.execute(sql_cmd)

        sql_cmd = """CREATE TABLE citations(

                        paperID1 CHAR(40) CHARACTER SET utf8,
                        paperID2 CHAR(40) CHARACTER SET utf8,

                        PRIMARY KEY (paperID1, paperID2),

                        isInfluential TINYINT(1)

                        )"""

        self._c.execute(sql_cmd)

        sql_cmd = """CREATE TABLE S2venues(

                        venueID MEDIUMINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                        venue VARCHAR(300) CHARACTER SET utf8
                        
                        )"""

        self._c.execute(sql_cmd)

        sql_cmd = """CREATE TABLE S2journals(

                        journalNameID SMALLINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                        journalName VARCHAR(300) CHARACTER SET utf8
                        
                        )"""

        self._c.execute(sql_cmd)

        #Commit changes to database
        self._conn.commit()

        return

    def importData(self, data_files):
        """
        Import data from Semantic Scholar compressed data files
        available at the indicated location
        """

        #We need to pass through all data files first to import venues and journalNames
        #We populate also the authors table
        all_venues = []
        all_journals = []
        author_counts = Counter()

        gz_files = [data_files+el for el in os.listdir(data_files) if el.startswith('s2-corpus')]
        bar = Bar('Extracting all venues, journalNames, and valid authors', max=len(gz_files))
        for fileno, gzf in enumerate(gz_files[:3]):
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
        self.insertInTable('S2authors', ['authorID', 'name'], df[['id', 'name']].values.tolist(), chunksize=25000, verbose=True)
        
        # We extract venues and journals as dictionaries for inserting new data in tables
        df = self.readDBtable('S2venues', selectOptions='venue, venueID')
        venues_dict = dict(df.values.tolist())
        df = self.readDBtable('S2journals', selectOptions='journalName, journalNameID')
        journals_dict = dict(df.values.tolist())

        # Now we need to read all files again, this time importing data
        # to the S2papers, citations, and PaperAuthor tables
        def ElementInList(source_list, search_string):
            if search_string in source_list:
                return 1
            else:
                return 0

        def process_paper(paperEntry):
            """This function takes a dictionary with paper information as input
            and returns three lists ready to insert in 
            S2papers, PaperAuthor, and citations tables
            """
            if 'year' in paperEntry.keys():
                paper_list = [[paperEntry['id'],
                          regex.sub(' ', paperEntry['title']),
                          regex.sub(' ', paperEntry['title'].lower()),
                          regex.sub(' ', paperEntry['paperAbstract']),
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
                          ]]
            else:
                paper_list = [[paperEntry['id'],
                          regex.sub(' ', paperEntry['title']),
                          regex.sub(' ', paperEntry['title'].lower()),
                          regex.sub(' ', paperEntry['paperAbstract']),
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
                          ]]

            author_list = [[paperEntry['id'], el['ids'][0]] 
                            for el in paperEntry['authors'] if len(el['ids'])]

            cite_list = [[paperEntry['id'], el] for el in paperEntry['outCitations']]

            return paper_list, author_list, cite_list

        gz_files = [data_files+el for el in os.listdir(data_files) if el.startswith('s2-corpus')]
        print('\n')
        bar = Bar('Extracting paper information', max=len(gz_files))
        for fileno, gzf in enumerate(gz_files[:3]):
            bar.next()
            with gzip.open(gzf, 'rt', encoding='utf8') as f:
                papers_infile = f.read().replace('}\n{','},{')
                papers_infile = json.loads('['+papers_infile+']')

                lista_papers = []
                lista_author_paper = []
                lista_citas = []
                for paper in papers_infile:
                    lp, lap, lc = process_paper(paper)
                    lista_papers += lp
                    lista_author_paper += lap
                    lista_citas += lc

                #Populate tables with the new data
                self.insertInTable('S2papers', ['paperID', 'title', 'lowertitle', 
                    'paperAbstract', 'entities', 's2PdfUrl', 'pdfUrls', 'year',
                    'venueID', 'journalNameID', 'journalVolume', 'journalPages',
                    'isDBLP', 'isMedline', 'doi', 'doiUrl', 'pmid'], lista_papers, chunksize=25000, verbose=True)
                #self.insertInTable('PaperAuthor', ['paperID', 'authorID'], lista_author_paper, chunksize=25000, verbose=True)
                self.insertInTable('citations', ['paperID1', 'paperID2'], lista_citas, chunksize=100000, verbose=True)

        return
