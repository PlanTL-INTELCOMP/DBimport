"""
Created on Aug 2019, by Jerónimo Arenas

@author: jarenas
"""

import os
import re
    
import requests
import json
import langid
from nltk.tokenize import sent_tokenize

import ipdb


class ENLemmatizer (object):

    """Class for English lemmatization, etc
       Based on Lemmatization service published by Carlos-Badenes et al at:
       https://github.com/librairy/nlpEN-service
       This class offers a wrapper for the above service and provides the posibility
       to multithread requests
       It also provides some utilities method for language detection, removing
       stopwords, etc
    ====================================================
    Public methods:
    - lemmatize: Function that extracts lemmas from a string
                 (applies stopword removal and equivalent words as indicated
                  during object initialization; optionally remove numbers)




    processESstr: Full processing of string. Includes the following stages:
        1. If keepsentence=True, The returned string will separate the original
           strings with \n
        2. Tokenization of each sentence
        3. Lemmatization (with removal/addition of tildes as selected)
        4. Stopwords removal
        5. Ngram identification
        6. Replacing equivalences
        7. If selected, remove numbers

        Steps 2-5 are implemented using the REST API of the Ontology group at UPM

    =====================================================
    """

    def __init__(self, lemmas_server, stw_file='', dict_eq_file=''):
        """
        Initilization Method
        Stopwwords and the dictionary of equivalences will be loaded
        during initialization
        :param lemmas_server: URL of the server running the librAIry lemmatization service
        :stw_file: File of stopwords
        :dict_eq_file: Dictionary of equivalent words A : B means A will be replaced by B

        """

        self.__stopwords = []

        # Unigrams for word replacement
        self.__useunigrams = False
        self.__pattern_unigrams = None
        self.__unigramdictio = None

        #Lemmatization service variables
        self.__url = lemmas_server
        self.__headers = {  'accept':'application/json',
                            'Content-Type':'application/json'
                            }

        # Load stopwords
        # Carga de stopwords genericas
        if os.path.isfile(stw_file):
            self.__stopwords = self.__loadStopFile(stw_file)
        else:
            self.__stopwords = []
            print ('No stopwords were loaded')

        # Anyadimos equivalencias predefinidas
        if os.path.isfile(dict_eq_file):
            self.__unigramdictio, self.__pattern_unigrams = self.__loadEQFile(dict_eq_file)
            if len(self.__unigramdictio):
                self.__useunigrams = True

        return


    def lemmatize(self, rawtext, POS='"NOUN", "VERB", "ADJECTIVE"', 
                    removenumbers=True, verbose=False):
        """Function to lemmatize a string
        :param rawtext: string with the text to lemmatize
        :POS: Part of Speech that will be kept
        :removenumbers: If true, tokens that can be converted to numbers will be removed
        :verbose: Display info for strings that cannot be lemmatized
        """
        if rawtext==None or rawtext=='':
            return ''
        elif langid.classify(rawtext)[0]!='en':
            if verbose:
                print('Not English:', langid.classify(rawtext), rawtext)
            return ''
        else:
            rawtext = rawtext.replace('\n',' ').replace('"', '').replace('\\','')
            data = '''{ "filter": [ '''+ POS +''' ],
                                 "multigrams": true,
                                 "references": false,
                                 "text": "'''+ rawtext +'''"}'''
            response = requests.post(self.__url, headers=self.__headers, data=str(data).encode('utf-8'))
                    
            if (response.ok):
                # 2. and 3. and 5. Tokenization and lemmatization and N-gram detection
                resp = json.loads(response.text)
                texto = [x['token']['lemma'] for x in resp['annotatedText']]
                # 4. Stopwords Removal
                texto = ' '.join(self.__removeSTW(texto))
                # 6. Make equivalences according to dictionary
                if self.__useunigrams:
                    texto = self.__pattern_unigrams.sub(
                        lambda x: self.__unigramdictio[x.group()], texto)
                # 7. Removenumbers if activated
                if removenumbers:
                    texto = ' '.join([word for word in texto.split() if not
                                self.__is_number(word)])
                return texto
            else:
                if verbose:
                    print('Cannot Lemmatize:', rawtext)
                return ''


    def extractEnglishSentences(self, rawtext):
        """Function to extract the English sentences in a string
        :param rawtext: string that we want to clean
        """
        sentences = sent_tokenize(rawtext, 'english')
        return ' '.join([el for el in sentences if langid.classify(el)[0]=='en'])


    def processENstr(self, text, keepsentence=True, removenumbers=True):
        """
        Full processing of English string. The following operations will be
        carried out on the selected string
        This function is only intended for English strings, weird things can
        happen for other languages
        1. If keepsentence=True, The returned string will separate the original
           strings with \n
        2. Tokenization of each sentence
        3. Lemmatization (with removal/addition of tildes as selected)
        4. Stopwords removal
        5. Ngram identification
        6. Replacing equivalences
        7. If selected, remove numbers
        :param text: The string to process
        :param keepsentence: If True, sentences will be separated by \n
        :param removenumbers: If True, tokens which are purely numbers will
                              also be removed
        """

        if text==None or text=='':
            return ''
        else:
            # 1. Detect sentences
            if keepsentence:
                strlist = sent_tokenize(text, 'english')
            else:
                strlist = [text]

            lematizedlist = []

            try:
                for el in strlist:

                    data = '''{ "filter": [ "NOUN", "VERB", "ADJECTIVE" ],
                                 "multigrams": true,
                                 "references": false,
                                 "text": "'''+ el +'''"}'''

                    response = requests.post(self.__url, headers=self.__headers, data=str(data).encode('utf-8'))
                    
                    if (response.ok):
                        # 2. and 3. and 5. Tokenization and lemmatization and N-gram detection
                        resp = json.loads(response.text)
                        texto = [x['token']['lemma'] for x in resp['annotatedText']]
                        # 4. Stopwords Removal
                        texto = ' '.join(
                            [word for word in texto if not word in self.__stopwords])
                        # 6. Make equivalences according to dictionary
                        if self.__useunigrams:
                            texto = self.__pattern_unigrams.sub(
                                lambda x: self.__unigramdictio[x.group()], texto)
                        # 7. Removenumbers if activated
                        if removenumbers:
                            texto = ' '.join(
                                [word for word in texto.split() if not
                                 self.__is_number(word)])

                        lematizedlist.append(texto)
            except:
                print('No se puede lematizar el resumen:', text)

            return '\n'.join(lematizedlist)


    def __loadStopFile(self, file):
        """Function to load the stopwords from a file. The stopwords will be
        read from the file, one stopword per line
        :param file: The file to read the stopwords from
        """
        with open(file, encoding='utf-8') as f:
            stopw = f.read().splitlines()

        return list(set([word.strip() for word in stopw if word]))

    def __loadEQFile(self, file):
        """Function to load equivalences from a file. The equivalence file
        will contain an equivalence per line in the format original : target
        where original will be changed to target after lemmatization
        :param file: The file to read the equivalences from
        """

        unigrams = []
        with open(file, 'r', encoding='utf-8') as f:
            unigramlines = f.read().splitlines()
        unigramlines = [x.split(' : ') for x in unigramlines]
        unigramlines = [x for x in unigramlines if len(x) == 2]

        if len(unigramlines):
            #This dictionary contains the necessary replacements to carry out
            unigramdictio = dict(unigramlines)
            unigrams = [x[0] for x in unigramlines]
            #Regular expression to find the tokens that need to be replaced
            pattern_unigrams = re.compile(r'\b(' + '|'.join(unigrams) + r')\b')
            return unigramdictio, pattern_unigrams
        else:
            return None, None


    def __removeSTW(self, tokens):
        """Removes stopwords from the provided list
        :param tokens: Input list of string to be cleaned from stw
        """
        return [el for el in tokens if el not in self.__stopwords]


    def __is_number(self, s):
        """Función que devuelve True si el string del argumento se puede convertir
        en un número, y False en caso contrario
        :Param s: String que se va a tratar de convertir en número
        :Return: True / False
        """
        try:
            float(s)
            return True
        except ValueError:
            return False


    def __remove_tildes(self, s):
        """Remove tildes from the input string
        :Param s: Input string
        :Return: String without tildes
        """
        if isinstance(s, unicode):
            try:
                s = s.encode('utf8')
            except:
                return ''

        list1 = ['á','é','í','ó','ú','Á','É','Í','Ó','Ú','à','è','ì','ò',
                 'ù','ü','â','ê','î','ô','û','ç','Ç']
        list2 = ['a','e','i','o','u','A','E','I','O','U','a','e','i','o',
                 'u','u','a','e','i','o','u','c','C']

        try:
            for i,letra in enumerate(list1):
                s = s.replace(letra,list2[i])
        except:
            s = ''

        return s