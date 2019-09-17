from os import listdir
from os.path import isfile, join
import xml.etree.ElementTree as ET
import ipdb
from lemmatizer.ESlemmatizer import ESLemmatizer
import random

import re

try:
    # UCS-4
    regex = re.compile('[\U00010000-\U0010ffff]')
except re.error:
    # UCS-2
    regex = re.compile('[\uD800-\uDBFF][\uDC00-\uDFFF]')

def clean_utf8(rawdata):
    return regex.sub(' ', rawdata)

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

lemmas_server = 'http://hator00.tsc.uc3m.es:6100/nlp/annotations/'
stw_file = './lemmatizer/lemafiles/stopwords/ESstopwords_SNOWBALL.txt'
dict_eq_file = ''
POS = '"NOUN", "VERB", "ADJECTIVE"'
concurrent_posts = 10
removenumbers = True
keepSentence = True

#Initialize lemmatizer
ESLM = ESLemmatizer(lemmas_server=lemmas_server, stw_file=stw_file,
            dict_eq_file=dict_eq_file, POS=POS, removenumbers=removenumbers,
            keepSentence=keepSentence)

xml_dir = './data_Law_BOE/XML'
LEMAS_dir = './data_Law_BOE/LEMAS'

xml_files = [f for f in listdir(xml_dir) if isfile(join(xml_dir, f))]

cont = 0
for chk in chunks(xml_files, 1000):
    print('Lematizando', cont, 'de 200')
    cont+=1
    to_lemmatize = []

    for f in chk:
        tree = ET.parse(join(xml_dir, f))
        root = tree.getroot()
        all_text = []
        for el in root:
            if el.tag=='texto':
                for parrafo in el:
                    if parrafo.tag=='p':
                        if parrafo.text:
                            all_text.append(parrafo.text.strip())
        if all_text:
            to_lemmatize.append([f.split('.xml')[0], clean_utf8(' '.join(all_text))])

    random.shuffle(to_lemmatize)
    lemasBatch = ESLM.lemmatizeBatch(to_lemmatize, processes=concurrent_posts)
    #Remove entries that where not lemmatized correctly
    lemasBatch = [[el[0], clean_utf8(el[1])] for el in lemasBatch if len(el[1])]
    ipdb.set_trace()
    