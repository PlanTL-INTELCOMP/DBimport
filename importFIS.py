"""
Created on Feb25 2019
@author: Jerónimo Arenas García

Import FIS Database to MySQL DB

    * Creating database from crawling the FIS portal

"""

import argparse
import configparser

from dbmanager.FISmanager import FISmanager

import ipdb
import time
import os

import re

try:
    # UCS-4
    regex = re.compile('[\U00010000-\U0010ffff]')
except re.error:
    # UCS-2
    regex = re.compile('[\uD800-\uDBFF][\uDC00-\uDFFF]')


def clean_utf8(rawdata):
    return regex.sub(' ', rawdata)


def main(download=False, resetDB=False, importData=False, 
         lemmatize=False, lemmas_query=None):
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
    dbNAME = cf.get('FIS', 'dbNAME')

    #########################
    # Datafiles
    #
    data_folder = cf.get('FIS', 'download_folder')
    ttsleep = int(cf.get('FIS', 'ttsleep'))

    ####################################################
    #1. Data download

    if download:
        #Import is only carried out if download option is activated
        #this avoids error messages if you already have the files
        #but do not have selenium installed in your system
        from selenium import webdriver
        from selenium.common.exceptions import NoSuchElementException
        browser = webdriver.Firefox()

        """Step 1: Retrieve all valid project URLs from the FIS portal"""
        if os.path.isfile(os.path.join(data_folder, 'allUrls.txt')):
            print('Reading URLs from file', os.path.join(data_folder, 'allUrls.txt'))
            print('Remove the file if you want to retrieve project URLs again')
            print('\n')
            with open(os.path.join(data_folder, 'allUrls.txt'), 'r') as fin:
                allUrls = fin.readlines()
                allUrls = [el.strip() for el in allUrls]

        else:

            allUrls = []

            FISUrl = 'https://portalfis.isciii.es/es/Paginas/Busqueda.aspx'
            browser.get(FISUrl)
            #Fill in the field for the search and submit query
            #searchtext = driver.find_element_by_id('ctl00_ctl34_g_b8905950_4e9a_4a7e_9d2d_d728f1b64287_txtBusqueda')
            browser.find_element_by_id(
                'ctl00_ctl34_g_b8905950_4e9a_4a7e_9d2d_d728f1b64287_txtBusqueda'
            ).send_keys('de')
            browser.find_element_by_id(
                'ctl00_ctl34_g_b8905950_4e9a_4a7e_9d2d_d728f1b64287_chkCoincidenciaExacta'
            ).click()
            browser.find_element_by_id(
                'ctl00_ctl34_g_b8905950_4e9a_4a7e_9d2d_d728f1b64287_btnBuscar'
            ).click()
            time.sleep(ttsleep)

            #get links to all project pages
            pageUrls = browser.find_elements_by_class_name('enlaceProyecto')
            for elm in pageUrls:
                if elm.tag_name == 'a':
                    allUrls.append(elm.get_attribute('href'))

            #Next we iterate over "next page buttom"
            try:
                nxtbtn = browser.find_element_by_id(
                    'ctl00_ctl34_g_b8905950_4e9a_4a7e_9d2d_d728f1b64287_ctl00_lnbSiguiente'
                )
            except NoSuchElementException:
                nxtbtn = None

            while(nxtbtn):
                nxtbtn.click()
                time.sleep(ttsleep)
                pageUrls = browser.find_elements_by_class_name('enlaceProyecto')
                #get links to all project pages
                for elm in pageUrls:
                    if elm.tag_name == 'a':
                        allUrls.append(elm.get_attribute('href'))
                try:
                    nxtbtn = browser.find_element_by_id(
                        'ctl00_ctl34_g_b8905950_4e9a_4a7e_9d2d_d728f1b64287_ctl00_lnbSiguiente'
                    )
                except NoSuchElementException:
                    nxtbtn = None

            #Save retrieved URLs in predefined file
            with open(os.path.join(data_folder, 'allUrls.txt'), 'w') as fout:
                fout.write('\n'.join(allUrls))

        """Step 2: Download project HTML page for all available projects"""
        for elm in allUrls:
            idProyecto = elm.split('idProyecto=')[1].replace('%2f','_')
            if os.path.isfile(os.path.join(data_folder, idProyecto+'.txt')):
                print('Ya se ha descargado el proyecto:', idProyecto)
            else:
                browser.get(elm)
                time.sleep(ttsleep)
                with open(os.path.join(data_folder, idProyecto+'.txt'), 'w') as fout:
                    fout.write(browser.page_source )

        browser.close()

    ####################################################
    #2. Database connection

    if resetDB or importData or lemmatize:
        DB = FISmanager (db_name=dbNAME, db_connector=dbCONNECTOR, path2db=None,
                        db_server=dbSERVER, db_user=dbUSER, db_password=dbPASS)
        #               db_port=dbPORT)

    ####################################################
    #3. If activated, remove and create again database tables
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
        #read and process in chunks of N articles
        chunksize = 25000
        cont = 0
        lemmas_server = cf.get('Lemmatizer', 'server')
        stw_file = cf.get('Lemmatizer', 'stw_file')
        dict_eq_file = cf.get('Lemmatizer', 'dict_eq_file')
        POS = cf.get('Lemmatizer', 'POS')
        concurrent_posts = int(cf.get('Lemmatizer', 'concurrent_posts'))
        removenumbers = cf.get('Lemmatizer', 'removenumbers') == 'True'
        keepSentence = cf.get('Lemmatizer', 'keepSentence') == 'True'

        #Initialize lemmatizer
        ENLM = ENLemmatizer(lemmas_server=lemmas_server, stw_file=stw_file,
                    dict_eq_file=dict_eq_file, POS=POS, removenumbers=removenumbers,
                    keepSentence=keepSentence)
        selectOptions = 'paperID, title, paperAbstract'
        if lemmas_query:
            filterOptions = 'paperID>0 AND ' + lemmas_query
        else:
            filterOptions = 'paperID>0'
        init_time = time.time()
        df = DB.readDBtable('S2papers', limit=chunksize, selectOptions=selectOptions,
                 filterOptions = filterOptions, orderOptions='paperID ASC')
        while (len(df)):
            cont = cont+len(df)
            
            #Next time, we will read from the largest paperID. This is the
            #last element of the dataframe, given that we requested an ordered df
            largest_id = df['paperID'][len(df)-1]
            print('Number of articles processed:', cont)
            print('Last Article Id read:', largest_id)

            df['alltext'] = df['title'] + '. ' + df['paperAbstract']
            df['alltext'] = df['alltext'].apply(clean_utf8)
            lemasBatch = ENLM.lemmatizeBatch(df[['paperID', 'alltext']].values.tolist(),
                                                processes=concurrent_posts)
            #Remove entries that where not lemmatized correctly
            lemasBatch = [[el[0], clean_utf8(el[1])] for el in lemasBatch if len(el[1])]
            print('Successful lemmatized documents:', len(lemasBatch))
            DB.setField('S2papers', 'paperID', ['LEMAS'], lemasBatch)
            if lemmas_query:
                filterOptions = 'paperID>' + str(largest_id) + ' AND ' + lemmas_query
            else:
                filterOptions = 'paperID>' + str(largest_id)
            df = DB.readDBtable('S2papers', limit=chunksize, selectOptions=selectOptions,
                 filterOptions = filterOptions, orderOptions='paperID ASC')
            elapsed_time = time.time() - init_time
            print('Elapsed Time (seconds):', time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))

    return


if __name__ == "__main__":

    parser = argparse.ArgumentParser(prog='importFIS')    
    parser.add_argument('--download', action='store_true', help='If activated, download data from FIS portal')
    parser.add_argument('--resetDB', action='store_true', help='If activated, the database will be reset and re-created')
    parser.add_argument('--importData', action='store_true', help='If activated, import downloaded data into database')
    parser.add_argument('--lemmatize', action='store_true', help='If activated, lemmatize database')
    parser.add_argument('--lemmas_query', type=str, dest='lemmas_query', help='Query for DB elements to lemmatize')
    parser.set_defaults(lemmas_query=None)
    args = parser.parse_args()

    main(download=args.download, resetDB=args.resetDB, importData=args.importData, 
         lemmatize=args.lemmatize, lemmas_query=args.lemmas_query)
