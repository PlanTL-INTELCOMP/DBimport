# -*- coding: utf-8 -*-
"""
Created on Oct 03 2015

Modified on Jan 2017 by Saúl Blanco
Modified on Jan 13, 2017, by Jerónimo Arenas
Modified on May 2018, by Jerónimo Arenas and Ángel Navia

@author: jarenas
"""

import os
import re
    
import requests
import json
import ipdb

# # Stopwords
stw_file = './tendencias/lemmatizer/lemafiles/stopwords/stop-words-english5.txt'
# # Fichero con un diccionario de equivalencias
# dict_eq_file = './lemmatizer/lemafiles/diccionario_equivalencias.txt'
dict_eq_file = ''


class ENLemmatizer (object):

    """Class for English lemmatization, etc
    ====================================================
    Public methods:

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

    def __init__(self):
        """
        Initilization Method
        Stopwwords and the dictionary of equivalences will be loaded
        during initialization

        """

        self.__stopwords = []

        # Unigrams for word replacement
        self.__useunigrams = True
        self.__pattern_unigrams = None
        self.__unigramdictio = None
        self.__url = 'http://localhost:7777/en/annotations'
        self.__headers = {  'accept':'application/json',
                            'Content-Type':'application/json'
                            }

        # Load stopwords
        # Carga de stopwords genericas
        if os.path.isfile(stw_file):
            self.__stopwords = self.__loadStopFile(stw_file)
        else:
            self.__stopwords = []
            print ('The file with generic stopwords could not be found')

        # Anyadimos equivalencias predefinidas
        if os.path.isfile(dict_eq_file):
            unigrams = []
            with open(dict_eq_file, 'r', encoding='utf-8') as f:
                unigramlines = f.read().splitlines()
            unigramlines = [x.split(' : ') for x in unigramlines]
            unigramlines = [x for x in unigramlines if len(x) == 2]

            if len(unigramlines):
                self.__unigramdictio = dict(unigramlines)
                unigrams = [x[0] for x in unigramlines]
                self.__pattern_unigrams = re.compile(
                    r'\b(' + '|'.join(unigrams) + r')\b')
            else:
                self.__useunigrams = False
        else:
            self.__useunigrams = False


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

    def __tokenize(self, s):
        """Removes undesired punctuation symbols, and returns 'tokens'
        that satisfy the provided regular expression
        :Param s: String to tokenize
        :Return: List with the tokens in s
        """

        tokens = []
        for match in self.__PAT_ALPHABETIC.finditer(s):
            tokens.append(match.group())

        return tokens

    def __getLema(self, word):
        """
        Uses Hunspell to lemmatize the input word
        Implementation details:

        - Hunspell returns the lowercase version of the input word when it
          appears on the dictionary,
          except when returning a proper name
        - It is using the Spanish dictionary, and it is sensitive to the
          presence of tildes.
          Incorrect placement of tildes will result in the word not being
          found in the dictionary, and consequently not lemmatized
        - When a word is not found in the dictionary, we return the lowercase
          version of the original word

        :param word: The word that will be lemmatized
        :Returns : The lemma
        """

        if word == '':
            return ''

        lema = self.__dic.stem(word)

        if len(lema) > 0:
            # La palabra se ha lematizado con éxito
            return lema[0]

        # Si la palabra no se pudo lematizar tratamos de lematizarla
        # reponiendo la tilde
        word = word.lower()

        if self.__tildesDict and word in self.__tildesDict:

            # La palabra aparece en la lista de diccionarios con tilde si
            # podemos lematizarla se la devuelve lematizada; en caso contrario
            # devolvemos la palabra original
            lema = self.__dic.stem(self.__tildesDict[word])

            if len(lema) == 0:
                return self.__tildesDict[word]
            else:
                return lema[0]

        else:

            # La palabra no aparece en la lista de diccionario de tilde
            # Vamos a tratar de estudiar si existe una versión con algún acento
            # en la vocal tal que la palabra sí puede ser lematizada

            # Recorremos la palabra desde el final al principio y vamos
            # poniendo acentos en las vocales en cuanto funcione una la damos
            # por correcta:
            acentEquivs = {'a': 'á', 'e': 'é', 'i': 'í', 'o': 'ó', 'u': 'ú'}
            # recorremos la palabra por cada caracter y vamos comprobando si
            # tenemos una vocal sin acento:
            for pos, char in enumerate(word):
                if word[pos] in acentEquivs:
                    # dividmos la palabra en tres, antes de la vocal, la vocal
                    # y de la vocal en adelante: si intentamos hacer esto mismo
                    # convirtiendo la palabra en una lista,
                    # da errores en los acentos que se convierten en dos
                    # posiciones:
                    word1 = word[:pos]
                    word2 = word[pos]
                    word3 = word[pos+1:]
                    # reemplazamos la vocal y juntamos la palabra:
                    wordTmp = word1 + word2.replace(
                        char, acentEquivs[char], 1) + word3
                    lema = self.__dic.stem(wordTmp)
                    if len(lema) > 0:
                        # En cuanto encontremos la palabra, se la devuelve
                        # lematizada
                        return lema[0]

            # Si no hemos encontrado una versión compatible, se devuelve la
            # palabra sin lematizar
            return word

    def __loadStopFile(self, file):
        """Function to load the stopwords from a file. The stopwords will be
        read from the file, one stopword per line
        :param file: The file to read the stopwords from
        """
        with open(file, encoding='utf-8') as f:
            stopw = f.read().splitlines()

        return list(set([word.strip() for word in stopw if word]))

    def __removeSTW(self, tokens):
        """Removes stopwords from the provided list
        :param tokens: Input list of string to be cleaned from stw
        """
        return [el for el in tokens if el not in self.__stopwords]

    def __loadNgramsFile(self, ngram_file):

        with open(ngram_file, 'r', encoding='utf-8') as fin:
            ngramas = fin.readlines()
        ngramas = list(map(lambda x: x.strip(), ngramas))

        # Since we do not know how Ngrams were created, we will lemmatize
        # them ourselves
        multiw = []
        for ng in ngramas:
            lem_tokens = [self.__getLema(word) for word in self.__tokenize(ng)]
            lem_tokens = self.__removeSTW(lem_tokens)

            if len(lem_tokens) > 1:
                ngr = ' '.join(lem_tokens)
                multiw.append(ngr)

        return multiw

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

#     def remove_tildes(self, s):
#         """Remove tildes from the input string
#         :Param s: Input string (en utf-8)
#         :Return: String without tildes (en utf-8)
#         """
#         #We encode in utf8; If not possible
#         #return an empty array
#         if isinstance(s, unicode):
#             try:
#                 s = s.encode('utf8')
#             except:
#                 return ''

#         list1 = ['á','é','í','ó','ú','Á','É','Í','Ó','Ú','à','è','ì','ò',
#                  'ù','ü','â','ê','î','ô','û','ç','Ç']
#         list2 = ['a','e','i','o','u','A','E','I','O','U','a','e','i','o',
#                  'u','u','a','e','i','o','u','c','C']

#         try:
#             for i,letra in enumerate(list1):
#                 s = s.replace(letra,list2[i])
#         except:
#             s = ''

#         return s
