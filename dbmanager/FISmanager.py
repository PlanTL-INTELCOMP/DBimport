"""
Datamanager for importing FIS projects
into a MySQL database

Created on Feb 09, 2020

@author: Jerónimo Arenas García

"""

import os
import pandas as pd
import numpy as np
from tqdm import *
import ipdb
import time

from dbmanager.dbManager.base_dm_sql import BaseDMsql


class FISmanager(BaseDMsql):

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

"""CREATE TABLE FISprojects(

    projectID INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    S2paperID VARCHAR(32),
                        
    title TINYTEXT,
    abstract MEDIUMTEXT,
    keywords TINYTEXT,

    startDate DATE,
    endDate DATE,

    PI VARCHAR(64),

    beneficiaryCentre VARCHAR(128),
    executionCentre VARCHAR(128),

    ComunidadAutonoma VARCHAR(32),
    Province VARCHAR(32),

    Budget MEDIUMINT UNSIGNED

    LEMAS MEDIUMTEXT

    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci"""
]

indices = [
]