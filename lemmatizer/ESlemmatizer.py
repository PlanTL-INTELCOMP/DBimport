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

import multiprocessing
import time
import re
import tqdm

#For monitoring progress of Batch lemmatization
#Cannot define it as member of ENLemmatizer class because
#it will not serialize correctly for multiprocessing
tqdm = tqdm.tqdm(total=100, mininterval=5)
tqdm.clear()

class ESLemmatizer (object):

    """Class for Spanish lemmatization, etc
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
    - cleanAndLemmatize Function that performs the following actions on a list [ID, text]:
                    1. Spanish sentences extraction
                    2. If keepsentence is True a token separating sentences is introduced
                    3. Lemmatization
                    4. If keepsentence is true the token is replaced by \n.
                       In this way, each line represents the lemmas in a sentence
                       This is necessary for training Word Embeddings
    - lemmatizeBatch: Function to lemmatize a batch of strings. Allows concurrent posts to
                      accelerate the lemmatization of large databases
    =====================================================
    """

    def __init__(self, lemmas_server, stw_file='', dict_eq_file='',
    				POS='"NOUN", "VERB", "ADJECTIVE"', 
                    removenumbers=True, keepSentence=True):
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

        # Other variables for the service
        self.__POS = POS
        self.__removenumbers = removenumbers
        self.__keepSentence = keepSentence 

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


    def lemmatize(self, rawtext, verbose=False, port=None):
        """Function to lemmatize a string
        :param rawtext: string with the text to lemmatize
        :param verbose: Display info for strings that cannot be lemmatized
        :param port: If port is not None, replace '7777' by indicated port
                     This is necessary for the current parallel implementation
                     since the service fails to run with multiple threads
        """
        if rawtext==None or rawtext=='':
            return ''
        elif langid.classify(rawtext)[0]!='es':
            if verbose:
                print('Not Spanish:', langid.classify(rawtext), rawtext)
            return ''
        else:
            rawtext = rawtext.replace('\n',' ').replace('"', '').replace('\\','')
            rawtext = rawtext.replace('{','').replace('}','')
            data = '''{ "filter": [ '''+ self.__POS +''' ],
                                 "lang": "es",
                                 "multigrams": true,
                                 "references": false,
                                 "text": "'''+ rawtext +'''"}'''
            try:
                if port:
                    response = requests.post(self.__url.replace('7777', str(port)), headers=self.__headers, data=str(data).encode('utf-8'))
                else:
                    response = requests.post(self.__url, headers=self.__headers, data=str(data).encode('utf-8'))
                    print(response)
            except:
                print('Error processing request at the lemmatization service, port:', str(port))
                #sleep for 5 seconds to allow the container to restart
                time.sleep(1)
                return ''

            print(response)
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
                if self.__removenumbers:
                    texto = ' '.join([word for word in texto.split() if not
                                self.__is_number(word)])
                return texto
            else:
                if verbose:
                    print('Cannot Lemmatize:', rawtext)
                return ''


    def cleanAndLemmatize(self, IDtext):
        """Function to clean and lemmatize a string
        :param IDtext: A list or duple, in the format: [ID, text]

		:Returns: A list with two elements, in the format: [ID, lemas]

        For each string to lemmatize the following steps are carried out:
        1. English text extraction
        2. If keepsentence is true a token separating sentences is introduced
        3. Lemmatization
        4. If keepsentence is true the token is replaced by \n
        """
        ID = IDtext[0]
        rawtext = IDtext[1]
        rawtext = self.__extractSpanishSentences(rawtext)
        if self.__keepSentence:
            sentences = sent_tokenize(rawtext, 'spanish')
            separator = ' newsentencespanish '
            rawtext = separator.join(sentences)
        lemas = self.lemmatize(rawtext)
        if self.__keepSentence:
            #Regular expression for replacing back. For instance, it couuld
            #be something like r'[\s\_]newsentence217([\s\_]newsentence217)*[\s\_]'
            #means that we will search for one or several repetitions of the separator
            #string separated by spaces and/or underscores (necessary for ngrams)
            separator = 'newsentencespanish'
            regexp = r'[\s\_]*'+separator+r'([\s\_]'+separator+r')*[\s\_]*'
            lemas = re.sub(regexp, '\n', lemas)
        tqdm.update(1)
        return [ID, lemas]


    def lemmatizeBatch(self, IDTextList, processes=1, verbose=False):
        """Function to lemmatize a batch of strings
        :param IDTextList: A list of lists or duples, in the format: [[ID, text], [], ...]
        :param processes: Number of concurrent posts to the lemmatization service
        :param verbose: Display info for strings that cannot be lemmatized

        :Returns: A list of lists in the format [[ID, lemas], [], ...]

        For each string to lemmatize the following steps are carried out:
        1. English text extraction
        2. If keepsentence is true a token separating sentences is introduced
        3. Lemmatization
        4. If keepsentence is true the token is replaced by \n
        5. Return a list in the format [[ID, lemas], [], ...]
        """
        tqdm.reset(total=len(IDTextList)/processes)
        pool = multiprocessing.Pool(processes=processes)
        IDLemasList = pool.map(self.cleanAndLemmatize, IDTextList)
        pool.close()
        pool.join()
        return IDLemasList


    # def cleanAndLemmatize_tmp(self, IDtext):
    #     """Function to clean and lemmatize a string
    #     :param IDtext: A list or duple, in the format: [ID, text]

    #     :Returns: A list with two elements, in the format: [ID, lemas]

    #     For each string to lemmatize the following steps are carried out:
    #     1. English text extraction
    #     2. If keepsentence is true a token separating sentences is introduced
    #     3. Lemmatization
    #     4. If keepsentence is true the token is replaced by \n
    #     """

    #     """=================================================================
    #     This is a temp version necessary until the NLPAIlibrary runs in 
    #     parallel correctly, with multiple threads
    #     ================================================================="""
    #     port = IDtext[0]
    #     IDTextList = IDtext[1]
    #     lemasList = []

    #     for elm in IDTextList:
    #         ID = elm[0]
    #         rawtext = elm[1]
    #         rawtext = self.__extractEnglishSentences(rawtext)
    #         if self.__keepSentence:
    #             sentences = sent_tokenize(rawtext, 'english')
    #             separator = ' newsentence' + str(ID) + ' '
    #             rawtext = separator.join(sentences)
    #         lemas = self.lemmatize(rawtext, port=port)
    #         if self.__keepSentence:
    #             #Regular expression for replacing back. For instance, it couuld
    #             #be something like r'[\s\_]*newsentence217([\s\_]newsentence217)*[\s\_]*'
    #             #means that we will search for one or several repetitions of the separator
    #             #string separated by spaces and/or underscores (necessary for ngrams)
    #             separator = 'newsentence' + str(ID)
    #             regexp = r'[\s\_]*'+separator+r'([\s\_]'+separator+r')*[\s\_]*'
    #             lemas = re.sub(regexp, '\n', lemas)
    #         lemasList.append([ID, lemas])
    #     return lemasList


    # def lemmatizeBatch_tmp(self, IDTextList, processes=1, verbose=False):
    #     """Function to lemmatize a batch of strings
    #     :param IDTextList: A list of lists or duples, in the format: [[ID, text], [], ...]
    #     :param processes: Number of concurrent posts to the lemmatization service
    #     :param verbose: Display info for strings that cannot be lemmatized

    #     :Returns: A list of lists in the format [[ID, lemas], [], ...]

    #     For each string to lemmatize the following steps are carried out:
    #     1. English text extraction
    #     2. If keepsentence is true a token separating sentences is introduced
    #     3. Lemmatization
    #     4. If keepsentence is true the token is replaced by \n
    #     5. Return a list in the format [[ID, lemas], [], ...]
    #     """

    #     """=================================================================
    #     This is a temp version necessary until the NLPAIlibrary runs in 
    #     parallel correctly, with multiple threads
    #     ================================================================="""

    #     #Since service currently do not parallelize, we need to create
    #     #n_processes sublists, so that each worker runs on a different list
    #     #and zip that with the port that needs to be used
    #     #
    #     #Inconvenient is that the speed will be that of the slower worker
    #     def chunkify(lst,n):
    #         return [lst[i::n] for i in range(n)]
    #     sublists = chunkify(IDTextList, processes)
    #     ports = [7777+el for el in range(processes)]
    #     newIDTextList = list(zip(ports, sublists)) 

    #     pool = multiprocessing.Pool(processes=processes)
    #     IDLemasList = pool.map(self.cleanAndLemmatize_tmp, newIDTextList)
    #     pool.close()
    #     pool.join()
    #     #return flattenlist
    #     IDLemasList = [item for sublist in IDLemasList for item in sublist]
    #     return IDLemasList


    def __extractSpanishSentences(self, rawtext):
        """Function to extract the English sentences in a string
        :param rawtext: string that we want to clean
        """
        sentences = sent_tokenize(rawtext, 'spanish')
        return ' '.join([el for el in sentences if langid.classify(el)[0]=='es'])


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
