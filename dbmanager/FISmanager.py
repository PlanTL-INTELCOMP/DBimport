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
from bs4 import BeautifulSoup

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

    def importData(self, data_folder):
        """
        Import data from FIS html pages
        """
        html_files = sorted([data_folder+el for el in os.listdir(data_folder) if el.endswith('.html')])
        print('Retrieving data for', len(html_files), 'projects')

        pbar = tqdm(total=len(html_files))
        all_projects = []

        for file in html_files:
            pbar.update(1)
            with open(file, 'r') as fin:
                contents = fin.read().replace('<br>', '\t')
                soup = BeautifulSoup(contents, 'lxml')

            code = file.split('/')[-1].split('.html')[0]
            title = soup.h2.text.strip()
            other_fields = [el.text.strip() for el in soup.findAll('p')]
            abstract = other_fields[0]
            keywords = other_fields[1]
            #Change from Spanish date format to US format
            startDate = '-'.join(other_fields[2].split(' - ')[0].split('/')[::-1])
            endDate = '-'.join(other_fields[2].split(' - ')[1].split('/')[::-1])
            PResearcher = other_fields[3]
            #Empty researchers appear with a comma to separate empty name and empty surname
            if PResearcher == ',':
                PResearcher = ''
            benCentre = other_fields[4]
            exeCentre = other_fields[5]
            CA = other_fields[6]
            province = other_fields[7]
            budget = int(other_fields[8].replace('.','').replace(' €',''))
            
            all_projects.append([code, title, abstract, keywords, startDate,
                endDate, PResearcher, benCentre, exeCentre, CA, province, budget])

        print('Filling in table FISprojects')

        self.insertInTable('FISprojects', ['FISprojectID', 'title', 'abstract', 
                'keywords', 'startDate', 'endDate', 'PI', 'beneficiaryCentre',
                'executionCentre', 'ComunidadAutonoma', 'Province', 'Budget'],
                all_projects)
 
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
    FISprojectID VARCHAR(32),
                        
    title MEDIUMTEXT,
    abstract MEDIUMTEXT,
    keywords MEDIUMTEXT,

    startDate DATE,
    endDate DATE,

    PI VARCHAR(64),

    beneficiaryCentre VARCHAR(128),
    executionCentre VARCHAR(128),

    ComunidadAutonoma VARCHAR(32),
    Province VARCHAR(32),

    Budget MEDIUMINT UNSIGNED,

    LEMAS MEDIUMTEXT

    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci"""
]

indices = [
]