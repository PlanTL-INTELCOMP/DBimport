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
from dbmanager.dbManager.base_dm_sql import BaseDMsql


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

        sql_cmd = """CREATE TABLE PaperAuthor(

                        paperID CHAR(40) CHARACTER SET utf8,
                        authorID VARCHAR(10) CHARACTER SET utf8,

                        PRIMARY KEY (paperID, authorID),

                        FOREIGN KEY (paperID)  REFERENCES S2papers (paperID),
                        FOREIGN KEY (authorID) REFERENCES S2authors (authorID)

                        )"""

        self._c.execute(sql_cmd)

        sql_cmd = """CREATE TABLE citations(

                        paperID1 CHAR(40) CHARACTER SET utf8,
                        paperID2 CHAR(40) CHARACTER SET utf8,

                        PRIMARY KEY (paperID1, paperID2),

                        isInfluential TINYINT(1)

                        )"""

        self._c.execute(sql_cmd)

        sql_cmd = """CREATE TABLE S2venues(

                        venueID MEDIUMINT AUTO_INCREMENT PRIMARY KEY,
                        venue VARCHAR(300) CHARACTER SET utf8
                        
                        )"""

        self._c.execute(sql_cmd)

        sql_cmd = """CREATE TABLE S2journals(

                        journalNameID MEDIUMINT AUTO_INCREMENT PRIMARY KEY,
                        journalName VARCHAR(300) CHARACTER SET utf8
                        
                        )"""

        self._c.execute(sql_cmd)

        #Commit changes to database
        self._conn.commit()

        return

