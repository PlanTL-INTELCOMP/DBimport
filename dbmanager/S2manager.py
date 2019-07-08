"""
Datamanager for importing Semantic Scholar papers
into a MySQL database

Created on Jul 7 2019

@author: Jerónimo Arenas García

"""

import os
import pandas as pd
import numpy as np
import copy
import requests
import ipdb
from progress.bar import Bar

# Tools for language detection and translation
from langid.langid import LanguageIdentifier, model
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

                        isDBLP VARCHAR(1) TINYINT(1),
                        isMedline VARCHAR(1) TINYINT(1),

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

                        paperID_1 CHAR(40) CHARACTER SET utf8,
                        paperID_1 CHAR(40) CHARACTER SET utf8,

                        PRIMARY KEY (paperID1, paperID_2)

                        isInfluential TINYINT(1)

                        )"""

        self._c.execute(sql_cmd)

        sql_cmd = """CREATE TABLE S2venues(

                        venueID MEDIUMINT UNSIGNED AUTO_INCREMENT,
                        venue VARCHAR(300) CHARACTER SET utf8
                        
                        )"""

        self._c.execute(sql_cmd)

        sql_cmd = """CREATE TABLE S2journals(

                        journalNameID SMALLINT UNSIGNED AUTO_INCREMENT,
                        journalName VARCHAR(300) CHARACTER SET utf8
                        
                        )"""

        self._c.execute(sql_cmd)

        #Commit changes to database
        self._conn.commit()

        return


"""








###################
# Translation functions
###################

BASE_URL = 'https://api.deepl.com/v1/translate'
#Key = 'af57cd8e-0dcb-832a-2f22-55f3134043c1'
Key = 'b60c7526-3c20-2f3a-e1f7-82422c17884d'

LANGUAGES = {
    'auto': 'Auto',
    'DE': 'German',
    'EN': 'English',
    'FR': 'French',
    'ES': 'Spanish',
    'IT': 'Italian',
    'NL': 'Dutch',
    'PL': 'Polish'
}

class TranslationError(Exception):
    def __init__(self, message):
        super(TranslationError, self).__init__(message)

def deepL_translate(text):
    if text is None or text=='':
        return ''
    if len(text) > 5000:
        raise TranslationError('Text too long (limited to 5000 characters).')
    # if to_lang not in LANGUAGES.keys():
    #     raise TranslationError('Language {} not available.'.format(to_lang))
    # if from_lang is not None and from_lang not in LANGUAGES.keys():
    #     raise TranslationError('Language {} not available.'.format(from_lang))

    text = text.replace('\\', ' \\ ').replace('&',' ').replace('?', ' ').replace('\n',' ')
    text = text.replace('\x04',' ').replace('\x00', ' ').replace('\x07',' ')
    text = text.replace('\x05',' ').replace('\x08', ' ').replace('\x06',' ')
    text = text.replace('\x01',' ').replace('\x02', ' ').replace('\x03',' ')
    text = text.replace('\xa0',' ')
    text = text.replace('\r', ' ').replace('\t',' ').replace('#8194',' ')
    text = text.replace('\x7f','')
    text = text.replace(' ', '%20').strip()
    data = str('auth_key='+Key+'&text='+text+'&target_lang=EN&source_lang=ES').encode('utf8')
    headers = {  'Content-Type':'application/x-www-form-urlencoded',
                 'Content-Length':str(len(text))
                }

    #print(text)
    response = requests.post(BASE_URL, headers=headers, data=data)
    if response.status_code!=200:
        ipdb.set_trace()
    #print(response)
    response = response.json()
    translations = response['translations'][0]['text']

    if len(translations) == 0:
        print('No se pudo traducir:', text)
        return ''
        #raise TranslationError('No translations found.')
    else:
        return translations

def google_tr(text, translate_service=None, target_language='es'):
    '''
    Translates text from English to Spanish using the selected translation
    service

    Args:
        text  :Text to be translated. It is assumed to be in English.
        translate_service
              :The api used to implement the automatic translation.
               Available options are:
                   - 'google' :
                   - None     :No translation is applied. The input text is
                               returned as output without changes.
        target_language: Target language (e.g., 'en', 'es')

    Returns:
        text_out     :Output text.
    '''

    if translate_service == 'google':

        # This service requires to have an active count in the Google
        # Cloud platform, install the cloud tools
        # (see https://cloud.google.com/sdk/docs/) and install the google
        # cloud library (pip install --upgrade google-cloud)

        # Instantiates a client
        translate_client = translate.Client()

        # Send text string to translate in blocks of 'maxsize' strings
        n_text = len(text)
        # The following variable should likely be smaller than 120.
        # Google does not allow lists with many strings to translate.
        maxsize = 50
        numblocks = (n_text - 1)//maxsize + 1

        # Block-by-block translation
        text_aux = []
        print ("Total number of blocks = {0}".format(numblocks))
        for n in range(numblocks):
            # Get next block of maxsize strings
            n0 = n * maxsize
            n1 = min((n + 1) * maxsize, n_text)
            q = text[n0:n1]

            print ("Block no. {0} with size {1}".format(n, len(q)))
            print ("Characters = {0}".format(np.sum([len(x) for x in q])))

            # TRANSLATE. This is the google translate request.
            newtext = translate_client.translate(q, target_language)
            text_aux += newtext
            #Wait n seconds between requests
            time.sleep(10)

        text_out = map(lambda d: d['translatedText'], text_aux)

    elif translate_service == 'deepl':

        text_out=[]
        for i,sentence in enumerate(text):
            if not(i%200):
                print(i)
            ENsentence = deepL_translate(sentence)
            #print(ENsentence)
            text_out.append(ENsentence)
            time.sleep(0.01)

    else:
        text_out = text

    return text_out

def getSpanishVersion(txt_es,txt_en, translate_service=None):

    # Create a language detector object
    langDetector = LanguageIdentifier.from_modelstring(model, norm_probs=True)
    langDetector.set_languages(['en', 'es'])

    # Cleaning dataset
    txt_out = []
    lang = []

    n_en = 0
    n_es = 0
    n_none = 0

    for k, (t_es, t_en) in enumerate(zip(txt_es, txt_en)):

        if not (k%1000):
            print(k)

        # Check status of the KEYes and KEYen fields.
        # This is required because some fields mcan be empty and even there
        # can be errors in the location of the Spanish and or English
        # versions
        if t_es == '':
            status_es = ''
        else:
            lang_d = langDetector.classify(t_es)
            status_es = lang_d[0]

        if t_en == '':
            status_en = ''
        else:
            lang_d = langDetector.classify(t_en)
            status_en = lang_d[0]

        # Compute the output list
        if status_es == '' and status_en == '':
            # There is no text anywere
            txt_out.append('')
            lang.append('')
            n_none += 1

        elif status_es == 'es':
            # There is text in Spanish in the right place
            txt_out.append(t_es)
            lang.append('es')
            n_es += 1

        elif status_en == 'es':
            # There is text in Spanish, though in the wrong place
            txt_out.append(t_en)
            lang.append('es')
            n_es += 1

        elif status_en == 'en':
            # No spanish text, translate from english:
            # new_key = hispanizer.translate(d[idKEYen])
            t_out = google_tr(t_en, translate_service, target_language='es')
            txt_out.append(t_out)
            lang.append('en')
            n_en += 1

        else:
            # No spanish text, english in the wrong place, translate:
            # new_key = hispanizer.translate(d[idKEYes])
            t_out = google_tr(t_es, translate_service, target_language='es')
            txt_out.append(t_out)
            lang.append('en')
            n_en += 1

    stats = {'es': n_es, 'en': n_en, 'none': n_none}

    return txt_out, stats, lang

def getEnglishVersion(txt_es,txt_en, translate_service=None):

    # Create a language detector object
    langDetector = LanguageIdentifier.from_modelstring(model, norm_probs=True)
    langDetector.set_languages(['en', 'es'])

    # Cleaning dataset
    txt_out = []
    lang = []

    n_en = 0
    n_es = 0
    n_none = 0

    for k, (t_es, t_en) in enumerate(zip(txt_es, txt_en)):

        if not (k%1000):
            print(k)

        # Check status of the KEYes and KEYen fields.
        # This is required because some fields mcan be empty and even there
        # can be errors in the location of the Spanish and or English
        # versions
        if t_es == '':
            status_es = ''
        else:
            lang_d = langDetector.classify(t_es)
            status_es = lang_d[0]

        if t_en == '':
            status_en = ''
        else:
            lang_d = langDetector.classify(t_en)
            status_en = lang_d[0]

        # Compute the output list
        if status_es == '' and status_en == '':
            # There is no text anywere
            txt_out.append('')
            lang.append('')
            n_none += 1

        elif status_en == 'en':
            # There is text in English in the right place
            txt_out.append(t_en)
            lang.append('en')
            n_en += 1

        elif status_es == 'en':
            # There is text in English, though in the wrong place
            txt_out.append(t_es)
            lang.append('en')
            n_en += 1

        elif status_es == 'es':
            # No English text, translate from Spanish:
            t_out = google_tr(t_es, translate_service, target_language='en')
            txt_out.append(t_out)
            lang.append('es')
            n_es += 1

        else:
            # No English text, Spanish in the wrong place, translate:
            t_out = google_tr(t_en, translate_service, target_language='en')
            txt_out.append(t_out)
            lang.append('es')
            n_es += 1

    stats = {'es': n_es, 'en': n_en, 'none': n_none}

    return txt_out, stats, lang

###################
# Generic functions
###################

def remove_tildes(s):
    """Remove tildes from the input string
    :Param s: Input string (en utf-8)
    :Return: String without tildes (en utf-8)
    """
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

def closest_match(list_str, new_str):
    """Returns the closest match to new_str from the elements in list_str,
    according to the normalized levenshtein distance
    : param list_str : List of of strings in 'utf8' codification
    : param new_str : New string to search closest match (in 'utf8')
    """

    #We start removing accents and other strange symbols

    list_str2 = list(map(remove_tildes, list_str))
    list_str2 = list(map(lambda x: x.lower(), list_str2))
    new_str2 = remove_tildes(new_str).lower()

    #Start by checking if there are any exact matches
    exact_match = list(map(lambda x: new_str2 == x, list_str2))
    if any(exact_match):
        return list_str[exact_match.index(True)]
    else:
        #If not, check if the string is a substring of another one in the list
        contained = list(map(lambda x: new_str2 in x, list_str2))
        if any(contained):
            return list_str[contained.index(True)]
        else:
            #If not, use normalized Levenshtein similarity
            similarities = list(map(lambda x: nleve(x, new_str2), list_str2))
            return list_str[similarities.index(min(similarities))]

def clean_name(nombre):
    """This function cleans some of the most common errors detected
    in the field "NOMBRE" in the researchers table
    """
    nombre = nombre.replace('NO INFORMADO', '')
    nombre = ', '.join([el.strip() for el in nombre.split(',')])
    nombre = nombre.replace(' ND,', ',')
    return nombre

def clean_NIF(NIF):
    """This function cleans some of the most common errors detected 
    in the field "NIF" in the researchers table
    """
    NIF=NIF.strip() #Removes any leading tabs, commas, etc
    NIF=NIF.replace(' ','').replace('-','') #Removes extra spaces, hyphens, etc
    NIF=NIF.lstrip('0') #Removes any heading zeros
    return NIF

def dictionary_replacement(codes, newwords, savefile=''):
    """Create a dictionary with replacements according to the edit
    distance
    :param codes: A list of tuples (word, targetstring)
    :param newwords: A list of words, so that for each word one of the targetstrings
               will be selected
    :param savefile: File where equivalences will be stored (log file)
    :Returns: Dictionary with equivalences of the format {word: targetstring}
    """
    #Add to the dictionary the provided codebook
    dictio = {el[0]:el[1] for el in codes}
    #Now, find new codes for provided list of words (newwords)
    list_codes = list(map(lambda x: x[0], codes))
    for el in newwords:
        dictio[el] = dictio[closest_match(list_codes,el)]
    #Excepciones:
    dictio['ND'] = 'ND'
    dictio['Sin Regionalizar'] = 'ND'
    dictio['SIN REGIONALIZAR'] = 'ND'
    dictio['SIN CLASIFICAR'] = 'ND'
    dictio['Sin clasificar'] = 'ND'
    dictio['Sin Clasificar'] = 'ND'
    if 'La Rioja' in dictio.keys():
        dictio['RIOJA (LA)'] = dictio['La Rioja']
    #Finally, if a savefile is indicated, add dictionary to that file
    if savefile:
        with open(savefile,'a') as f:
            for llave in dictio.keys():
                f.write(dictio[llave]+':'+llave+'\n')
    return dictio

ccaaCodes = [("Andalucía","01"),
             ("Aragón","02"),
             ("Baleares","03"),
             ("Canarias","04"),
             ("Cantabria","05"),
             ("Castilla La Mancha","06"),
             ("Castilla y León", "07"),
             ("Cataluña", "08"),
             ("Ceuta","09"),
             ("Extremadura","10"),
             ("Galicia","11"),
             ("La Rioja","12"),
             ("Madrid","13"),
             ("Melilla","14"),
             ("Murcia","15"),
             ("Navarra","16"),
             ("País Vasco","17"),
             ("Asturias","18"),
             ("C. Valenciana","19")]

provinceCodes = [('Islas Baleares', '07'),
             ('Asturias', '33'),
             ('A Coruña', '15'),
             ('Girona', '17'),
             ('Las Palmas', '35'),
             ('Pontevedra', '36'),
             ('Santa Cruz de Tenerife', '38'),
             ('Cantabria', '39'),
             ('Málaga', '29'),
             ('Almería', '04'),
             ('Murcia', '30'),
             ('Albacete', '02'),
             ('Ávila', '05'),
             ('Álava/Araba', '01'),
             ('Badajoz', '06'),
             ('Alicante/Alacant', '03'),
             ('Ourense', '32'),
             ('Barcelona', '08'),
             ('Burgos', '09'),
             ('Cáceres', '10'),
             ('Cádiz', '11'),
             ('Castellón/Castelló', '12'),
             ('Ciudad Real', '13'),
             ('Jaén', '23'),
             ('Córdoba', '14'),
             ('Cuenca', '16'),
             ('Granada', '18'),
             ('Guadalajara', '19'),
             ('Guipúzcoa/Gipuzkoa', '20'),
             ('Huelva', '21'),
             ('Huesca', '22'),
             ('León', '24'),
             ('Lleida', '25'),
             ('La Rioja', '26'),
             ('Soria', '42'),
             ('Navarra', '31'),
             ('Ceuta', '51'),
             ('Lugo', '27'),
             ('Madrid', '28'),
             ('Palencia', '34'),
             ('Salamanca', '37'),
             ('Segovia', '40'),
             ('Sevilla', '41'),
             ('Toledo', '45'),
             ('Tarragona', '43'),
             ('Teruel', '44'),
             ('Valencia/València', '46'),
             ('Valladolid', '47'),
             ('Vizcaya/Bizkaia', '48'),
             ('Zamora', '49'),
             ('Zaragoza', '50'),
             ('Melilla', '52')]

retoCodes = [('Acción sobre el cambio climático y eficiencia en la utilización de recursos y materias primas','01'),
             ('Cambios e innovaciones sociales','02'),
             ('Economía y sociedad digital','03'),
             ('Energía segura, eficiente y limpia','04'),
             ('Salud, cambio demográfico y bienestar','05'),
             ('Seguridad y calidad alimentarias; actividad agraria productiva y sostenible, recursos naturales, investigación marina y marítima','06'),
             ('Seguridad, protección y defensa','07'),
             ('Transporte inteligente, sostenible e integrado', '08')]

TFECodes = [('Biotecnología','01'),
             ('Materiales avanzados, micro, nanoelectronica, nanotecnología','02'),
             ('Tecnologías de la información y las comunicaciones','03'),
             ('Materiales avanzados','04'),
             ('Micro y nanoelectrónica','05'),
             ('Fotónica','06')]

ANEPareas = [('Agricultura (AGR)','01'),
             ('Biología Fundamental y de Sistemas (BFS)','02'),
             ('Biología Vegetal, Animal y Ecología (BVAE)','03'),
             ('Biomedicina (BMED)','04'),
             ('Ciencia y Tecnología de los Alimentos (TA)','05'),
             ('Ciencia y Tecnología de Materiales (TM)','06'),
             ('Ciencias de la Computación y Tecnología Informática (INF)','07'),
             ('Ciencias de la Educación (EDUC)','08'),
             ('Ciencias de la Tierra (CT)','09'),
             ('Ciencias Sociales (CS)','10'),
             ('Derecho (DER)','11'),
             ('Economía (ECO)','12'),
             ('Filología y Filosofía (FFI)','13'),
             ('Física y Ciencias del Espacio (FI)','14'),
             ('Ganadería y Pesca (GAN)','15'),
             ('Historia y Arte (HA)','16'),
             ('Ingeniería Civil y Arquitectura (ICI)','17'),
             ('Ingeniería Eléctrica, Electrónica y Automática (IEL)','18'),
             ('Ingeniería Mecánica, Naval y Aeronáutica (IME)','19'),
             ('Matemáticas (MTM)','20'),
             ('Medicina Clínica y Epidemiología (MCLI)','21'),
             ('Psicología (PS)','22'),
             ('Química (QMC)','23'),
             ('Tecnología Electrónica y de las Comunicaciones (COM)','24'),
             ('Tecnología Química (TQ)','25'),
             ('Transferencia de Tecnología (IND)','26')]

SectorCodes = [('Empresas','01'),
             ('Administración Pública','02'),
             ('Enseñanza Superior','03'),
             ('Instituciones Privadas sin Fines de Lucro','04'),
             ('Otros','05')]

TentidadCodes = [('Empresa privada (gran empresa)','01'),
             ('Empresa privada (pyme)','02'),
             ('Empresa pública','03'),
             ('Organismo de salud público (hospital incluido)','04'),
             ('Universidad privada','05'),
             ('Centro o institución de las Administraciones Públicas','06'),
             ('Centro público de investigación (no OPI)','07'),
             ('Entidad privada sin ánimo de lucro','08'),
             ('Universidad pública','09'),
             ('Centro de Innovación y Tecnología (CIT)','10'),
             ('Parque científico y tecnológico','11'),
             ('Asociación empresarial sin ánimo de lucro','12'),
             ('Organismo Público de Investigación (OPI)','13'),
             ('Agrupación o asociación de empresas (UTE, ...)','14')]

TITULO_CONV_dictio = {
        'CDTI_FEDER_Innterconecta': 'CDTI_FEDER_ITC',
        'ProyectosIDExcelencia': 'SEIDI_PROYECTOS_EXCELENCIA',
        'Explora': 'SEIDI_EXPLORA',
        'EuropaExcelencia': 'SEIDI_EUROPA_EXCELENCIA',
        'RetosInvestigacion': 'SEIDI_RETOS_INVESTIGACION',
        'APCI': 'SEIDI_APCI',
        'ProyINIA': 'INIA',
        'CDTI_Innternacionaliza': 'CDTI_INNTERNACIONALIZA',
        'CDTI_RetosEmpresa': 'CDTI_I+D_RETOS',
        'CDTI_LineaDirectaInnovacion': 'CDTI_LIC_FACILITADORAS',
        'CDTI_Neotec': 'CDTI_NEOTEC',
        'CDTI_ID_Empresarial': 'CDTI_I+D_EMPRESARIAL',
        'CDTI_Facilitadoras': 'CDTI_I+D_FACILITADORAS',
        'RetosJovenes': 'SEIDI_JOVENES',
        'CDTI_Eurostars': 'CDTI_EUROSTARS',
        'CDTI_Innovación global': 'CDTI_LIG_LIDERAZGO',
        'CDTI_EEA Grants Empresarial': 'CDTI_EEA_GRANTS_EMPRESARIAL',
        'CDTI_EEA Grants Retos': 'CDTI_EEA_GRANTS_RETOS',
        'CDTI_EEA Grants Facilitadoras': 'CDTI_EEA_GRANTS_FACILITADORAS',
        'CDTI_CIEN': 'CDTI_CIEN'
        }

sexo_dictio = {
        'MASCULINO': 'Masculino',
        'FEMENINO': 'Femenino',
        'SIN CLASIFICAR': 'ND'
        }

#This list contains all the fields that will be kept. If a particular PAID file contains additional
#columns, they will be ignored
columns_to_keep = ['REFERENCIA', 'CONVOCATORIA', 'TITULO CONVOCATORIA', 'TIPO DE PROYECTO', 'STATUS',
    'AREA ANEP', 'DURACION', 'CODIGOS NAB 2007', 'PROGRAMA', 'RETO', 'TECNOLOGIA FACILITADORA ESENCIAL',
    'TITULO', 'PALABRA CLAVE LISTA INGLES', 'PALABRA CLAVE LISTA CASTELLANO',
    'RESUMEN DEL PROYECTO EN INGLES', 'RESUMEN DEL PROYECTO EN CASTELLANO',
    'TOTAL SOLICITADO', 'PERSONAL SOLICITADO', 'TOTAL CONCEDIDO', 'PERSONAL CONCEDIDO',
    'CIF', 'ORGANISMO OBJ SOCIAL', 'CENTRO', 'COMUNIDAD REALIZACION', 'PROVINCIA',
    'NIF', 'INVESTIGADOR', 'APELLIDO1', 'APELLIDO2', 'NOMBRE', 'SEXO',
    'YEAR NACIMIENTO', 'IP (S/CO/N)']

def cleandf(df):
    """Homogeneize and clean the dataframe so that
    it can be later incorporated to the database
    :param df: The dataframe to be cleaned
    :Returns: The dataframe after carrying out cleaning tasks
    """
    #1. All text fields should be utf8 encoded
    text_fields = ['REFERENCIA', 'TITULO CONVOCATORIA', 'TIPO DE PROYECTO', 'STATUS',
        'AREA ANEP', 'PROGRAMA', 'RETO', 'TECNOLOGIA FACILITADORA ESENCIAL',
        'TITULO', 'PALABRA CLAVE LISTA INGLES', 'PALABRA CLAVE LISTA CASTELLANO',
        'RESUMEN DEL PROYECTO EN INGLES', 'RESUMEN DEL PROYECTO EN CASTELLANO',
        'CIF', 'ORGANISMO OBJ SOCIAL', 'CENTRO', 'COMUNIDAD REALIZACION', 'PROVINCIA',
        'NIF', 'INVESTIGADOR', 'APELLIDO1', 'APELLIDO2', 'NOMBRE', 'SEXO',
        'IP (S/CO/N)']
    for fldname in text_fields:
        if fldname in df.columns:
            df[fldname] = df[fldname].astype(str)

    #2. Unify fields that can only take values from within a dictionary
    #2.1. Clean the CCAA field. In order to do that, we first create
    #a dictionary of replacements, and later apply replacements to the
    #dataframe
    fldname = 'COMUNIDAD REALIZACION'
    if fldname in df.columns:
        ccaas = list(set(df[fldname]))
        dictio = dictionary_replacement(ccaaCodes, ccaas, './data_Pr_FECYT/eq/CCAAequivalences.txt')
        df[fldname].replace(dictio, inplace=True)

    #2.2. Clean the province field.
    fldname = 'PROVINCIA'
    if fldname in df.columns:
        provinces = list(set(df[fldname]))
        dictio = dictionary_replacement(provinceCodes, provinces, './data_Pr_FECYT/eq/Provincias_equivalences.txt')
        df[fldname].replace(dictio, inplace=True)

    #2.3. Clean the reto field.
    fldname = 'RETO'
    if fldname in df.columns:
        retos = list(set(df[fldname]))
        dictio = dictionary_replacement(retoCodes, retos, './data_Pr_FECYT/eq/Retos_equivalences.txt')
        df[fldname].replace(dictio, inplace=True)

    #2.4. Clean the reto field.
    fldname = 'TECNOLOGIA FACILITADORA ESENCIAL'
    if fldname in df.columns:
        tfes = list(set(df[fldname]))
        dictio = dictionary_replacement(TFECodes, tfes, './data_Pr_FECYT/eq/TFE_equivalences.txt')
        df[fldname].replace(dictio, inplace=True)

    #2.5. Clean the AREA ANEP field.
    fldname = 'AREA ANEP'
    if fldname in df.columns:
        areas = list(set(df[fldname]))
        dictio = dictionary_replacement(ANEPareas, areas, './data_Pr_FECYT/eq/ANEP_equivalences.txt')
        df[fldname].replace(dictio, inplace=True)

    #2.6. Clean the SEXO field
    fldname = 'SEXO'
    if fldname in df.columns:
        df[fldname].replace(sexo_dictio, inplace=True)

    #2.7. Clean the TITULO CONVOCATORIA field
    df['TITULO CONVOCATORIA'].replace(TITULO_CONV_dictio, inplace=True)

    #3.Remove hyphens in fields NIF and CIF
    if 'CIF' in df.columns:
        df['CIF'] = list(map(lambda x: str(x).replace('-',''), df['CIF']))
    if 'NIF' in df.columns:
        df['NIF'] = list(map(lambda x: str(x).replace('-',''), df['NIF']))

    #4.Some fields are converted to 'title' case
    titleflds = ['STATUS', 'ORGANISMO OBJ SOCIAL', 'CENTRO']
    for fldname in titleflds:
        if fldname in df.columns:
            df[fldname] = list(map(lambda x: x.title(), df[fldname]))

    #5. We fix the birth year
    def fix_year(x):
        """This function is used to ensure that the birth year is
        kept within the range 1900 < YEAR NACIMIENTO < 2016
        """
        try:
            if int(x)>1900 and int(x)<2016:
                return int(x)
            elif int(x+1900)>1900 and int(x+1900)<2016:
                return int(x+1900)
            else:
                return 'ND'
        except:
            return 'ND'

    if 'YEAR NACIMIENTO' in df.columns:
        df['YEAR NACIMIENTO'] = list(map(lambda x: fix_year(x), df['YEAR NACIMIENTO']))

    #6. Fix NAB 2007 codes
    def try_int(x):
        try:
            return int(x)
        except:
            return 'ND'

    if 'CODIGOS NAB 2007' in df.columns:
        df['CODIGOS NAB 2007'] = list(map(lambda x: try_int(x), df['CODIGOS NAB 2007']))

    #7. Returns the dataframe after all cleaning tasks
    return df

proyecto_fields = ['REFERENCIA','CONVOCATORIA', 'TITULO CONVOCATORIA', 'TIPO DE PROYECTO', 'STATUS',
                    'CODIGOS NAB 2007',  'PROGRAMA', 'AREA ANEP', 'DURACION', 'RETO', 'TFE',
                    'TITULO', 'KEYWORDS', 'PALABRAS CLAVE', 'RESUMEN', 'ABSTRACT', 'TOTAL CONCEDIDO',
                    'TOTAL SOLICITADO', 'PERSONAL CONCEDIDO', 'PERSONAL SOLICITADO', 'CIFEMPRESA', 'CENTRO',
                    'PROVINCIA', 'CCAA', 'INVEST_FEMENINO', 'INVEST_MASCULINO', 'INVEST_TOTAL',
                    'INVEST_EXTR', 'INVEST_EDADES', 'SEXO IP', 'EDAD IP', 'EXTR IP'
                    ]

#List of fields for table `investigadorproyecto`
investigadorproyecto_fields = ['NIF', 'REFERENCIA', 'ROL']

#List of fields for table `investigador`
investigador_fields = ['NOMBRE', 'NIF', 'YEAR NACIMIENTO', 'SEXO']

#List of fields for table `organismo`
organismo_fields = ['ORGANISMO OBJ SOCIAL', 'CIF', 'TIPO ENTIDAD', 'SECTOR', 'TIPO ENTIDAD']

def project_table_to_sql(dfproyecto):
    """
    Obtain the sequence of SQL commands to incorporate the data in the dataframe
    into the database. If necessary, we generate also queries to remove outdated info
    :param dfproyecto: Dataframe with information to incorporate
    """

    #Variables a devolver. Las inicializamos todas a [] para que 
    #la rutina que la llama sepa qué tablas hay que actualizar 
    pryfields = []
    pryvalues = []
    pryinvvalues_list = []
    inveslist = []
    orglist = []

    #Remove duplicated rows if any
    dfproyecto = dfproyecto.drop_duplicates()

    fields = dfproyecto.columns

    #################################################################################
    #1. Información de proyecto. 
    #################################################################################

    #En primer lugar, hay que determinar la fila del proyecto con la que nos quedamos
    #si tenemos más de una fila. Aplicamos la siguiente regla:
    #   1. Si existe el campo 'ROL', nos quedamos con la primera fila que tenga valor 'S'
    #   2. Si no existe dicho campo, o si dicho campo vale siempre 'N', nos quedamos con la primera
    #      fila del dataframe directamente

    if 'IP (S/CO/N)' in fields:
        dfproyectoIP = dfproyecto[dfproyecto['IP (S/CO/N)']=='S'].reset_index(drop=True)
        if dfproyectoIP.shape[0]>0:
        #Nos quedamos con el primer investigador que lo cumple
            fila_pry = list(dfproyectoIP.head(1).values[0])
        else:
            #Nos quedamos con la primera fila del proyecto
            fila_pry = list(dfproyecto.head(1).values[0])
    else:
        #Nos quedamos directamente con la primera entrada del proyecto
        fila_pry = list(dfproyecto.head(1).values[0])

    pryfields = []
    pryvalues = []

    for fld in proyecto_fields:
        #Campos que tienen distinto nombre en los excel y en la base de datos
        #CIFEMPRESA, KEYWORDS, PALABRAS CLAVE, RESUMEN, ABSTRACT
        if fld=='CIFEMPRESA':
            if 'CIF' in fields:
                CIFidx = fields.tolist().index('CIF')
                if fila_pry[CIFidx]!='ND':
                    pryfields.append(fld)
                    pryvalues.append(fila_pry[CIFidx])
        elif fld=='CCAA':
            if 'COMUNIDAD REALIZACION' in fields:
                fldidx = fields.tolist().index('COMUNIDAD REALIZACION')
                pryfields.append(fld)
                pryvalues.append(fila_pry[fldidx])
        elif fld=='KEYWORDS':
            if 'PALABRA CLAVE LISTA INGLES' in fields:
                fldidx = fields.tolist().index('PALABRA CLAVE LISTA INGLES')
                pryfields.append(fld)
                pryvalues.append(fila_pry[fldidx].replace('\n',' ').replace('"',' '))
        elif fld=='PALABRAS CLAVE':
            if 'PALABRA CLAVE LISTA CASTELLANO' in fields:
                fldidx = fields.tolist().index('PALABRA CLAVE LISTA CASTELLANO')
                pryfields.append(fld)
                pryvalues.append(fila_pry[fldidx].replace('\n',' ').replace('"',' '))
        elif fld=='RESUMEN':
            if 'RESUMEN DEL PROYECTO EN CASTELLANO' in fields:
                fldidx = fields.tolist().index('RESUMEN DEL PROYECTO EN CASTELLANO')
                pryfields.append(fld)
                pryvalues.append(fila_pry[fldidx].replace('\n',' ').replace('"',' '))
        elif fld=='ABSTRACT':
            if 'RESUMEN DEL PROYECTO EN INGLES' in fields:
                fldidx = fields.tolist().index('RESUMEN DEL PROYECTO EN INGLES')
                pryfields.append(fld)
                pryvalues.append(fila_pry[fldidx].replace('\n',' ').replace('"',' '))
        elif fld=='TFE':
            if 'TECNOLOGIA FACILITADORA ESENCIAL' in fields:
                fldidx = fields.tolist().index('TECNOLOGIA FACILITADORA ESENCIAL')
                pryfields.append(fld)
                pryvalues.append(fila_pry[fldidx])
        #Campos que requieren cálculos
        elif fld=='INVEST_FEMENINO':
            if 'SEXO' in fields:
                pryfields.append(fld)
                mujeres = dfproyecto[dfproyecto['SEXO']=='Femenino'].shape[0]
                pryvalues.append(str(mujeres))
        elif fld=='INVEST_MASCULINO':
            if 'SEXO' in fields:
                pryfields.append(fld)
                hombres = dfproyecto[dfproyecto['SEXO']=='Masculino'].shape[0]
                pryvalues.append(str(hombres))
        elif fld=='INVEST_EXTR':
            if 'NIF' in fields:
                pryfields.append(fld)
                nextranj = dfproyecto[list(map(lambda x: not x[0].isdigit() if len(x) else False, dfproyecto['NIF']))].shape[0]
                pryvalues.append(str(nextranj))
        elif fld=='INVEST_TOTAL':
            if ('NIF' in fields) or ('SEXO' in fields) or ('YEAR NACIMIENTO' in fields):
                pryfields.append(fld)
                ninves = dfproyecto.shape[0]
                pryvalues.append(str(ninves))
        elif fld=='INVEST_EDADES':
            if 'YEAR NACIMIENTO' in fields:
                def year_ND(x):
                    try:
                        return str(int(x))
                    except:
                        return 'ND'
                if ','.join([year_ND(el) for el in dfproyecto['YEAR NACIMIENTO']])!='ND':
                    pryfields.append(fld)
                    pryvalues.append(','.join([year_ND(el) for el in dfproyecto['YEAR NACIMIENTO']]))
        elif fld=='SEXO IP':
            if 'IP (S/CO/N)' in fields:
                if dfproyectoIP.shape[0]>0:
                    if 'SEXO' in fields:
                        pryfields.append(fld)
                        SEXOidx = fields.tolist().index('SEXO')
                        pryvalues.append(fila_pry[SEXOidx])
        elif fld=='EDAD IP':
            if 'IP (S/CO/N)' in fields:
                if dfproyectoIP.shape[0]>0:
                    if 'YEAR NACIMIENTO' in fields:
                        YEARidx = fields.tolist().index('YEAR NACIMIENTO')
                        def year_ND(x):
                            try:
                                return str(int(x))
                            except:
                                return 'ND'
                        if year_ND(fila_pry[YEARidx])!='ND':
                            #We only add this field if it is numeric
                            pryfields.append(fld)
                            pryvalues.append(year_ND(fila_pry[YEARidx]) )
        elif fld=='EXTR IP':
            if 'IP (S/CO/N)' in fields:
                if dfproyectoIP.shape[0]>0:
                    if 'NIF' in fields:
                        pryfields.append(fld)
                        NIFidx = fields.tolist().index('NIF')
                        if fila_pry[NIFidx][0].isdigit():
                            pryvalues.append('ESPAÑOL')
                        else:
                            pryvalues.append('EXTRANJERO')
        else:
            #Default behavior. When fld is present in the fields for the project, we simply add the
            #value to the table, with care that we remove any " characters
            if fld in fields:
                idx = fields.tolist().index(fld)
                val = fila_pry[idx]
                if val!='ND':
                    pryfields.append(fld)
                    try:
                        pryvalues.append(val.replace('\n',' ').replace('"',' ') )
                    except:
                        #Campo numerico
                        pryvalues.append(str(val))

    pryfields = list(map(str,pryfields))

    #################################################################################
    # 2. Información de investigador, proyecto.
    #################################################################################

    if 'IP (S/CO/N)' in fields and 'NIF' in fields:
        NIFidx = fields.tolist().index('NIF')
        REFidx = fields.tolist().index('REFERENCIA')
        ROLidx = fields.tolist().index('IP (S/CO/N)')

        for pryinv in dfproyecto.values:
            NIF = clean_NIF(list(pryinv)[NIFidx])
            REF = list(pryinv)[REFidx]
            ROL = list(pryinv)[ROLidx]

            if NIF!='' and NIF is not None and NIF is not np.nan:
                #NIF = NIF.replace(u'\ufffd','')
                pryinvvalues = []
                pryinvvalues.append(NIF)
                pryinvvalues.append(REF)
                pryinvvalues.append(ROL)

                pryinvvalues_list.append(pryinvvalues)

    #################################################################################
    # 3. Listamos los dataframes de investigadores a añadir o actualizar (upsert)
    #################################################################################

    if 'NIF' in fields:
        NIFidx = fields.tolist().index('NIF')

        for inv in dfproyecto.values:
            NIF = clean_NIF(list(inv)[NIFidx])

            if NIF!='' and NIF is not None and NIF is not np.nan:
                
                invfields = []
                invvalues = []

                for fld in investigador_fields:
                    if fld =='NOMBRE':
                        if 'INVESTIGADOR' in fields:
                            invfields.append(fld)
                            idx = fields.tolist().index('INVESTIGADOR')
                            invvalues.append(clean_name(list(inv)[idx]))
                        elif 'APELLIDO1' in fields:
                            #Componemos el nombre
                            ap1_idx = fields.tolist().index('APELLIDO1')
                            ap1 = list(inv)[ap1_idx]
                            ap2_idx = fields.tolist().index('APELLIDO2')
                            ap2 = list(inv)[ap2_idx]
                            name_idx = fields.tolist().index('NOMBRE')
                            name = list(inv)[name_idx]
                            if ap2!='' and ap2 is not None and ap2 is not np.nan:
                                name = ap1 + ' ' + ap2 + ', ' + name
                            else:
                                name = ap1 + ', ' + name

                            if name!='' and name is not None and name is not np.nan:
                                invfields.append(fld)
                                invvalues.append(clean_name(name))
                    elif fld == 'NIF':
                        invfields.append(fld)
                        invvalues.append(NIF)
                    elif fld=='YEAR NACIMIENTO':
                    	if fld in fields:
	                        idx = fields.tolist().index(fld)
	                        val = list(inv)[idx]
	                        def year_ND(x):
	                            try:
	                                return str(int(x))
	                            except:
	                                return 'ND'
	                        if year_ND(val) != 'ND':
	                            invfields.append(fld)
	                            invvalues.append(year_ND(val))
                    else:
                        #Default behavior. When fld is present in the fields for the researcher, we simply add the
                        #value to the table, with care that we remove any " characters
                        if fld in fields:
                            idx = fields.tolist().index(fld)
                            val = list(inv)[idx]
                            if val!='' and val is not None and val is not np.nan:
                                invfields.append(fld)
                                try:
                                    invvalues.append(list(inv)[idx].replace('\n',' ').replace('"',' '))
                                except:
                                   #Campo numerico
                                    invvalues.append(str(list(inv)[idx]))

                inveslist.append(pd.DataFrame.from_records([tuple(invvalues)], columns=invfields))

    #################################################################################
    # 4. Creamos dataframe con la información del organismo
    #################################################################################

    if 'ORGANISMO OBJ SOCIAL' in fields and 'CIF' in fields:
        CIFidx = fields.tolist().index('CIF')
        ORGidx = fields.tolist().index('ORGANISMO OBJ SOCIAL')
        CIF = fila_pry[CIFidx]
        ORG = fila_pry[ORGidx]
        if CIF!='' and CIF is not None and CIF is not np.nan and ORG!='' and ORG is not None and ORG is not np.nan:
            
            organismovalues = []
            organismovalues.append(CIF)
            organismovalues.append(ORG.replace('\n','').replace('"',''))
            orglist.append(pd.DataFrame.from_records([tuple(organismovalues)], columns=['CIF', 'ORGANISMO OBJ SOCIAL']))

    return pryfields,pryvalues,pryinvvalues_list,inveslist,orglist

# convocatorias_structs = {
#     'aaa0000-00000': 7,
#     'aaa-00000000': 8,
#     'aaa0000-00000-C00-00': 7,
#     'aaa0000-00000-R': 7,
#     'aaa0000-00000-P': 7,
#     'aaa0000-00000-C0-0-R': 7,  
#     'aaa0000-00000-Eaa': 7,
#     'aaa0000-00000-Jaa': 7,
#     'aaa-00000000-00': 8,
#     'aaa0000-00000-C0-0-P': 7,
#     'aaa0000-00000-Raaa': 7,
#     'aaaa0000-00000': 8,
#     'aaaa-00000000': 9,
#     'aaaa-0000-000': 9,
#     'aaa-00000000-B-00000000': 8,
#     'aaa-00000000-A-00000000': 8,
#     'aaaa-0000-000-C00-00': 9,
#     'aaaa-00000000-00': 9,
#     'aaa0000-00000-00-00': 7,
#     'aaaaa0000-00000-00-00': 9,
#     'aaaa0000-000-C00-00': 8,
#     'aaaa0000-000': 8,
#     'a-Raa0000-00000-C00-00': 9,
#     'aaa-00000000-F-00000000': 8,
#     'aaaa-00000000-B-00000000': 9,
#     'aaaa-00000000-0': 9,
#     'aaaa0000-A-00000': 8,
#     'aaaa-00000000-A-00000000': 9,
#     'a-Raa0000-00000-00-00': 9,
#     'aaa-00000000-Q-0000000a': 8,
#     'aa00?00000': 4,
#     'aaa00?00000': 5,
#     'aaa-000000-0000-0000': 15
# }

class FECYTmanager(BaseDMsql):
    """
    Specific functions for FECYT database creation
    """

    def __init__(self, db_name, db_connector, path2db=None,
                 db_server=None, db_user=None, db_password=None, db_port=None):
        """
        Initializes the FECYT Manager object

        Args:
            db_name      :Name of the DB
            db_connector :Connector. Available options are mysql or sqlite
            path2db :Path to the project folder (sqlite only)
            db_server    :Server (mysql only)
            db_user      :User (mysql only)
            db_password  :Password (mysql only)
        """

        super(FECYTmanager, self).__init__(
            db_name, db_connector, path2db, db_server, db_user,
            db_password, db_port)

    def createDBtables(self, file_convocatorias):
        """
        Create DB table structure
        """

        sql_cmd = """CREATE TABLE proyectos(

                        REFERENCIA VARCHAR(30) CHARACTER SET utf8 PRIMARY KEY,
                        `TITULO CONVOCATORIA` VARCHAR(45) CHARACTER SET utf8 NOT NULL,
                        CONVOCATORIA SMALLINT UNSIGNED,
                        `TIPO DE PROYECTO` VARCHAR(20) CHARACTER SET utf8,
                        STATUS VARCHAR(10) CHARACTER SET utf8,
                        `CODIGOS NAB 2007` VARCHAR(6) CHARACTER SET utf8,
                        PROGRAMA VARCHAR(3) CHARACTER SET utf8,
                        `AREA ANEP` VARCHAR(2) CHARACTER SET utf8,
                        RETO VARCHAR(2) CHARACTER SET utf8,
                        TFE VARCHAR(2) CHARACTER SET utf8,
                        DURACION FLOAT NOT NULL,

                        NCOORDINADOS SMALLINT UNSIGNED,
                        PCOORDINADOS TEXT CHARACTER SET utf8,

                        TITULO TEXT CHARACTER SET utf8 NOT NULL,
                        KEYWORDS TEXT CHARACTER SET utf8,
                        `PALABRAS CLAVE` TEXT CHARACTER SET utf8,
                        RESUMEN TEXT CHARACTER SET utf8,
                        ABSTRACT TEXT CHARACTER SET utf8,

                        TITULO_ESP TEXT CHARACTER SET utf8,
                        RESUMEN_ESP TEXT CHARACTER SET utf8,
                        PALABRAS_ESP TEXT CHARACTER SET utf8,

                        TITULO_ENG TEXT CHARACTER SET utf8,
                        RESUMEN_ENG TEXT CHARACTER SET utf8,
                        PALABRAS_ENG TEXT CHARACTER SET utf8,

                        LEMAS_SETSI TEXT CHARACTER SET utf8,
                        LEMAS_UC3M TEXT CHARACTER SET utf8,
                        LEMAS_UC3M_ENG TEXT CHARACTER SET utf8,

                        `TOTAL CONCEDIDO` FLOAT NOT NULL,
                        `TOTAL SOLICITADO` FLOAT,
                        `PERSONAL CONCEDIDO` FLOAT,
                        `PERSONAL SOLICITADO` FLOAT,

                        CIFEMPRESA VARCHAR(12) CHARACTER SET utf8,
                        CENTRO VARCHAR(255) CHARACTER SET utf8,
                        PROVINCIA VARCHAR(2) CHARACTER SET utf8,
                        CCAA VARCHAR(2) CHARACTER SET utf8,

                        INVEST_FEMENINO SMALLINT UNSIGNED,
                        INVEST_MASCULINO SMALLINT UNSIGNED,
                        INVEST_TOTAL SMALLINT UNSIGNED,
                        INVEST_EXTR SMALLINT UNSIGNED,
                        INVEST_EDADES TEXT CHARACTER SET utf8,
                        `SEXO IP` VARCHAR(10) CHARACTER SET utf8,
                        `EDAD IP` SMALLINT UNSIGNED,
                        `EXTR IP` VARCHAR(10) CHARACTER SET utf8,

                        Ind2017_BIO VARCHAR(1) CHARACTER SET utf8,
                        Ind2017_TIC VARCHAR(1) CHARACTER SET utf8,
                        Ind2017_ENE VARCHAR(1) CHARACTER SET utf8

                        )"""
        self._c.execute(sql_cmd)

        ####create investigadores table
        sql_cmd = """CREATE TABLE investigadores(
                        NIF VARCHAR(20) CHARACTER SET utf8 PRIMARY KEY,
                        NOMBRE VARCHAR(255) CHARACTER SET utf8,
                        `YEAR NACIMIENTO` SMALLINT UNSIGNED,
                        SEXO VARCHAR(10) CHARACTER SET utf8
                        )"""
        self._c.execute(sql_cmd)

        ####create investigadorproyecto table
        sql_cmd = """CREATE TABLE investigadorproyecto(
                        NIF VARCHAR(20) CHARACTER SET utf8 NOT NULL,
                        REFERENCIA VARCHAR(30) CHARACTER SET utf8 NOT NULL,
                        ROL VARCHAR(5) CHARACTER SET utf8
                        )"""
        self._c.execute(sql_cmd)

        ####create organismos table
        sql_cmd = """CREATE TABLE organismos(
                        CIF VARCHAR(12) CHARACTER SET utf8 PRIMARY KEY,
                        `ORGANISMO OBJ SOCIAL` VARCHAR(512) CHARACTER SET utf8,
                        SECTOR VARCHAR(2) CHARACTER SET utf8,
                        `TIPO ENTIDAD` VARCHAR(2) CHARACTER SET utf8
                        )"""
        self._c.execute(sql_cmd)

        ####create convocatorias table
        sql_cmd = """CREATE TABLE convocatorias(
                        `TITULO CONVOCATORIA` VARCHAR(30) CHARACTER SET utf8 PRIMARY KEY,
                        ACTUACION VARCHAR(130) CHARACTER SET utf8,
                        `PROGRAMA ESTATAL` VARCHAR(75) CHARACTER SET utf8,
                        `SUBPROGRAMA ESTATAL` VARCHAR(160) CHARACTER SET utf8,
                        `ENTIDAD CONVOCANTE` VARCHAR(10) CHARACTER SET utf8,
                        `UNIDAD GESTORA` VARCHAR(20) CHARACTER SET utf8
                        )"""
        self._c.execute(sql_cmd)

        ####create retos table
        sql_cmd = """CREATE TABLE retos(
                        id VARCHAR(2) CHARACTER SET utf8 NOT NULL,
                        RETO VARCHAR(150) CHARACTER SET utf8 PRIMARY KEY
                        )"""
        self._c.execute(sql_cmd)

        ####create TFEs table
        sql_cmd = """CREATE TABLE TFEs(
                        id VARCHAR(2) CHARACTER SET utf8 NOT NULL,
                        TFE VARCHAR(75) CHARACTER SET utf8 PRIMARY KEY
                        )"""
        self._c.execute(sql_cmd)

        ####create areasANEP table
        sql_cmd = """CREATE TABLE areasANEP(
                        id VARCHAR(2) CHARACTER SET utf8 NOT NULL,
                        AREA VARCHAR(70) CHARACTER SET utf8 PRIMARY KEY
                        )"""
        self._c.execute(sql_cmd)

        ####create provincias table
        sql_cmd = """CREATE TABLE provincias(
                        id VARCHAR(2) CHARACTER SET utf8 NOT NULL,
                        PROVINCIA VARCHAR(40) CHARACTER SET utf8 PRIMARY KEY
                        )"""
        self._c.execute(sql_cmd)

        ####create CCAAs table
        sql_cmd = """CREATE TABLE CCAAs(
                        id VARCHAR(2) CHARACTER SET utf8 NOT NULL,
                        CCAA VARCHAR(40) CHARACTER SET utf8 PRIMARY KEY
                        )"""
        self._c.execute(sql_cmd)

        ####create orgsectors table
        sql_cmd = """CREATE TABLE orgsectors(
                        id VARCHAR(2) CHARACTER SET utf8 NOT NULL,
                        SECTOR VARCHAR(50) CHARACTER SET utf8 PRIMARY KEY
                        )"""
        self._c.execute(sql_cmd)

        ####create orgtipos table
        sql_cmd = """CREATE TABLE orgtipos(
                        id VARCHAR(2) CHARACTER SET utf8 NOT NULL,
                        `TIPO ENTIDAD` VARCHAR(70) CHARACTER SET utf8 PRIMARY KEY
                        )"""
        self._c.execute(sql_cmd)

        ####We create an index for the investigadorproyecto table
        sql_cmd = 'CREATE INDEX investproy ON investigadorproyecto(REFERENCIA,NIF)'
        self._c.execute(sql_cmd)

        # ####Fill in convocatorias table with data provided by FECYT
        df = pd.read_excel(file_convocatorias).fillna('ND')
        self.insertInTable('convocatorias', self.getColumnNames('convocatorias'), df.values)

        #### Fill in several Tables with predefined data
        self.insertInTable('provincias',['PROVINCIA','id'],provinceCodes)
        self.insertInTable('CCAAs',['CCAA', 'id'],ccaaCodes)
        self.insertInTable('retos',['RETO','id'],retoCodes)
        self.insertInTable('TFEs',['TFE','id'],TFECodes)
        self.insertInTable('areasANEP',['AREA','id'],ANEPareas)
        self.insertInTable('orgsectors',['SECTOR','id'],SectorCodes)
        self.insertInTable('orgtipos',['TIPO ENTIDAD','id'],TentidadCodes)

        #Commit changes to database
        self._conn.commit()

    def addPAIDdir(self, PAIDdir):
        print('Processing all excel files in directory ' + PAIDdir)
        for root, dirs, files in os.walk(PAIDdir):
            for file in files:
                if file.endswith('.xls') or file.endswith('.xlsx'):
                    if not file.startswith('~'):
                        self.addPAID(os.path.join(root,file))

    def addPAID(self, PAIDfile):
        """This function processes a PAID or AIR file and incorporate
        all information contained on it to the database
        :param PAIDfile: Complete path to file that will be incorporated
        """
        print('Processing file: ' + PAIDfile.split('/')[-1])
        
        # 1. We start by reading the excel file and finding out whether
        # it is a PAID file or an AIR file (old format)
        df = pd.read_excel(PAIDfile,sheet_name=None)
        #For unknown reasons, one of the files contains a (not visible) Macro1 sheet with
        #no information
        sheets = [sh for sh in df.keys() if sh!='Macro1']
        #We store in list dftype1 all dataframes that will be processed
        #In most cases there will be just one element, but one file contains
        #6 different data sheets
        dftype1 = []
        if 'Proyecto' in df.keys() and 'Entidad_Investigador' in sheets:
            #File is PAID
            #Read Proyecto and Entidad_Investigador sheets, fill NaN fields with string 'ND'
            #and keep only relevant columns. Put together both Dataframes
            dfproyectos = df['Proyecto'].fillna('ND')
            fieldsproyectos = [fld for fld in columns_to_keep if fld in dfproyectos.columns]
            dfproyectos = dfproyectos[fieldsproyectos]
            dfinvest = df['Entidad_Investigador'].fillna('ND')
            fieldsinvest = [fld for fld in columns_to_keep if fld in dfinvest.columns]
            dfinvest = dfinvest[fieldsinvest]
            #If there are repeated fields, we keep those of the researchers
            #We keep however the REFERENCIA field, since that is going to be used to relate
            #the project and research information
            repeated_fields = [fld for fld in dfproyectos if fld in dfinvest and fld!='REFERENCIA']
            #print repeated_fields
            dfproyectos = dfproyectos[[fld for fld in dfproyectos.columns if fld not in repeated_fields]]
            dftype1.append(pd.merge(dfproyectos, dfinvest, on='REFERENCIA'))
        else:
            #NOT PAID
            for sheet in sheets:
                dfcompleta = df[sheet].fillna('ND')
                fields = [fld for fld in columns_to_keep if fld in dfcompleta.columns]
                dfcompleta = dfcompleta[fields]
                dftype1.append(dfcompleta)
        #
        #2. Cleaning the dataframes
        #
        dftype1clean = list(map(cleandf, dftype1))

        #3.Next, we process each dataframe and incorporate its fields to the database
        lchunk = 100
        for df in dftype1clean:
            refs_proyecto = list(set(df['REFERENCIA']))
            nproyectos = len(refs_proyecto)
            bar = Bar('Incorporating projects to database', max=1+nproyectos/lchunk)
            for index,ref in enumerate(refs_proyecto):
                if not index%lchunk:
                    #print '\r Processing ' + str(index+1) + ' of ' + str(nproyectos) +' projects',
                    #sys.stdout.flush()
                    bar.next()
                #Obtain subtable with corresponding entries for the project
                dfproyecto = df[df['REFERENCIA']==ref].reset_index(drop=True)

                #La siguiente funcion obtiene las listas de campos a incluir
                #en las tablas (o actualizar)
                pryfields,pryvalues,pryinvvalues_list,inveslist,orglist = project_table_to_sql(dfproyecto)
                
                #Añadimos la entrada del proyecto, que siempre esté disponible
                try:
                    self.insertInTable('proyectos',pryfields,[pryvalues])
                except Exception as e:
                    print(e)
                    print(pryfields)
                    print(pryvalues)
                    exit()
                #Añadimos las entradas de organismos si es que las tenemos
                for dfel in orglist:
                    #self.upsert('organismos','CIF',dfel)
                    #Hacer upsert es lento cuando tenemos muchas entradas en la tabla, porque la función
                    #va a bajar todo el listado de CIFs de la tabla antes de decidir si se hacer insert
                    #o update. Optamos por la siguiente alternativa más rápida:
                    try:
                        self.insertInTable('organismos', dfel.columns.tolist(), dfel.values.tolist())
                    except Exception as e:
                        if e.args[0]!=1062:
                            print(e)
                        else:
                            keyfld = 'CIF'
                            flds = [keyfld] + [x for x in dfel.columns if x != keyfld]
                            dfel = dfel[flds]
                            valueflds = [x for x in dfel.columns if x != keyfld]
                            values = [tuple(dfel.values.tolist()[0])]
                            self.setField('organismos', keyfld, valueflds, values)
                #Añadimos las entradas de investigadroes, si es que las tenemos
                if len(pryinvvalues_list):
                    self.insertInTable('investigadorproyecto',investigadorproyecto_fields,pryinvvalues_list)
                for dfel in inveslist:
                    #self.upsert('investigadores','NIF',dfel)
                    #Hacer upsert es lento cuando tenemos muchas entradas en la tabla, porque la función
                    #va a bajar todo el listado de CIFs de la tabla antes de decidir si se hacer insert
                    #o update. Optamos por la siguiente alternativa más rápida:
                    try:
                        self.insertInTable('investigadores', dfel.columns.tolist(), dfel.values.tolist())
                    except Exception as e:
                        if e.args[0]!=1062:
                            print(e)
                        else:
                            keyfld = 'NIF'
                            flds = [keyfld] + [x for x in dfel.columns if x != keyfld]
                            dfel = dfel[flds]
                            valueflds = [x for x in dfel.columns if x != keyfld]
                            values = [tuple(dfel.values.tolist()[0])]
                            self.setField('investigadores', keyfld, valueflds, values)
            bar.finish()
        #4. Finally, we fix the birth year for some researchers
        sqlcmd='UPDATE investigadores SET `YEAR NACIMIENTO`=NULL WHERE `YEAR NACIMIENTO`=0'
        self._c.execute(sqlcmd)

        #Commit changes
        self._conn.commit()

    def updateprojectdata(self, dataprojectsfile):
        """This function processes an excel with possibly many sheets,
        where each sheet has two columns, the first being project references
        and the second the field with information to be included in the
        projects table
        :param dataprojectsfile: Complete path to excel file
        """
        print ('Processing file: ' + dataprojectsfile.split('/')[-1])
        #
        #We iterate over the sheets in the excel file
        df = pd.read_excel(dataprojectsfile,sheet_name=None)
        cursor = self._c
        for sheetname in df.keys():
            dfprocess = df[sheetname]
            if dfprocess.columns[0]=='REFERENCIA' and dfprocess.columns[1] in self.getColumnNames('proyectos'):
                print ('Processing sheet: ' + sheetname)
                #Name of the field that will be updated
                upd_fld = dfprocess.columns[1]
                #UTF8 encoding of everything
                dfprocess['REFERENCIA'] = dfprocess['REFERENCIA'].astype(str)
                #Make sure that the second column is used as strings
                if upd_fld.startswith('inferred') or upd_fld=='CODIGOS NAB 2007':
                    dfprocess[upd_fld] = dfprocess[upd_fld].astype(str)
                dfprocess[upd_fld] = dfprocess[upd_fld].astype(str)
                #When updating the RETO and TFE fields, we need to make "code" replacements
                if upd_fld=='RETO':
                    retos = list(set(dfprocess[upd_fld]))
                    dictio = dictionary_replacement(retoCodes, retos, '')
                    dfprocess[upd_fld].replace(dictio, inplace=True)
                if upd_fld=='TFE':
                    tfes = list(set(dfprocess[upd_fld]))
                    dictio = dictionary_replacement(TFECodes, tfes, '')
                    dfprocess[upd_fld].replace(dictio, inplace=True)
                values = zip(dfprocess[upd_fld],dfprocess['REFERENCIA'])
                cursor.executemany('UPDATE proyectos SET `' + upd_fld + '`=%s WHERE REFERENCIA=%s', values)

            else:
                print ('Sheet ' + sheetname + ' could not be processed (incorrect column format)')
        self._conn.commit()

    def updateorgdata(self, dataorganizationsfile):
        """This function processes an excel with possibly many sheets,
        where each sheet has two columns, the first being organization CIFs
        and the second the field with information to be included in the
        projects table
        :param dataorganizationsfile: Complete path to excel file
        """
        print ('Processing file: ' + dataorganizationsfile.split('/')[-1])
        #
        #We iterate over the sheets in the excel file
        df = pd.read_excel(dataorganizationsfile,sheet_name=None)
        cursor = self._c
        for sheetname in df.keys():
            dfprocess = df[sheetname]
            if dfprocess.columns[0]=='CIF' and dfprocess.columns[1] in organismo_fields:
                print ('Processing sheet: ' + sheetname)
                #Name of the field that will be updated
                upd_fld = dfprocess.columns[1]
                #UTF8 encoding of everything
                dfprocess['CIF'] = dfprocess['CIF'].astype(str)
                dfprocess[upd_fld] = dfprocess[upd_fld].astype(str)
                #When updating the Sector and tipo de entidad fields, we need to make "code" replacements
                if upd_fld=='SECTOR':
                    sectores = list(set(dfprocess[upd_fld]))
                    dictio = dictionary_replacement(SectorCodes, sectores, '')
                    dfprocess[upd_fld].replace(dictio, inplace=True)
                if upd_fld=='TIPO ENTIDAD':
                    tipoents = list(set(dfprocess[upd_fld]))
                    dictio = dictionary_replacement(TentidadCodes, tipoents, '')
                    dfprocess[upd_fld].replace(dictio, inplace=True)
                #Database updates
                values = zip(dfprocess[upd_fld],dfprocess['CIF'])
                for row in dfprocess.values:
                    sqlcmd = 'UPDATE organismos SET `' + str(upd_fld) + '`="' + row[1] + \
                          '" WHERE CIF="' + str(row[0]).replace('"','') + '"'
                    cursor.execute(sqlcmd)

            else:
                print ('Sheet ' + sheetname + ' could not be processed (incorrect column format)')
        self._conn.commit()

    def updateresearcherdata(self, dataresearchersfile):
        """This function processes an excel with possibly many sheets,
        where each sheet has two columns, the first being researchers NIF
        and the second the field with information to be included in the
        researchers table
        :param dataresearchersfile: Complete path to excel file
        """
        print ('Processing file: ' + dataresearchersfile.split('/')[-1])
        #
        #We iterate over the sheets in the excel file
        df = pd.read_excel(dataresearchersfile,sheet_name=None)
        cursor = self._c
        for sheetname in df.keys():
            dfprocess = df[sheetname]
            if dfprocess.columns[0]=='NIF' and dfprocess.columns[1] in investigador_fields:
                print ('Processing sheet: ' + sheetname)
                #Name of the field that will be updated
                upd_fld = dfprocess.columns[1]
                #UTF8 encoding of everything
                dfprocess['NIF'] = list(map(lambda x: str(x).replace('-',''), dfprocess['NIF']))
                dfprocess['NIF'] = dfprocess['NIF'].astype(str)
                dfprocess[upd_fld] = list(map(lambda x: str(x), dfprocess[upd_fld]))
                dfprocess[upd_fld] = dfprocess[upd_fld].astype(str)
                #Database updates
                values = zip(dfprocess[upd_fld],dfprocess['NIF'])
                cursor.executemany('UPDATE investigadores SET `' + upd_fld + '`=%s WHERE NIF=%s', values)
            else:
                print ('Sheet ' + sheetname + ' could not be processed (incorrect column format)')
        self._conn.commit()

    def cleanResearcherTable(self):
        """This function cleans the database:
            * Removes NIFs that are repetitions of a single character, and those not including any numbers
            * Consolidate researchers using different NIFs
        """
        
        dfinv = self.readDBtable('investigadores', limit=None, selectOptions='*')
        dfinv['isalpha'] = dfinv['NIF'].apply(lambda x: x.isalpha())
        dfinv['nchars'] = dfinv['NIF'].apply(lambda x: len(set(x)))
        dfunicos = dfinv[dfinv['nchars']==1]
        dfisalpha = dfinv[dfinv['isalpha']]
        NIFlist = dfunicos.append(dfisalpha).drop_duplicates()['NIF'].values.tolist()
        NIFlist = ','.join(['"'+el+'"' for el in NIFlist])
        print ('Removing researchers by NIF: ' + NIFlist)

        sqlcmd = 'DELETE from investigadorproyecto where NIF in (' + NIFlist + ')'
        self._c.execute(sqlcmd)
        sqlcmd = 'DELETE from investigadores where NIF in (' + NIFlist + ')'
        self._c.execute(sqlcmd)

        self._conn.commit()

        #Identify researchers which are likely to appear several times though with different NIFS
        dfinv = self.readDBtable('investigadores', limit=None, selectOptions='*')
        dfinvproj = self.readDBtable('investigadorproyecto', limit=None, selectOptions='*')
        dfagg = dfinv.groupby(['NOMBRE', 'YEAR NACIMIENTO', 'SEXO'], as_index=False).aggregate({'NIF': lambda x: ','.join(x)})
        dfagg['NIF'] = dfagg['NIF'].apply(lambda x: x.split(','))
        dfagg['nnif'] = dfagg['NIF'].apply(len)

        dictio_NIFs = {} 
        NIF_to_remove = []
        #We collapse all researchers that have three different NIF (having also same name and birth year)
        dfagg3 = dfagg[dfagg['nnif']>2]
        dfagg3.to_excel('triplicated_researchers.xlsx')
        NIF3 = dfagg3['NIF'].values.tolist()
        sortedNIF = [sorted(x, key=len, reverse=True) for x in NIF3]
        for x in sortedNIF:
            for i,el in enumerate(x):
                if i>0:
                    NIF_to_remove.append(el)
                    dictio_NIFs[el] = x[0]
        #We collapse researchers that have two different NIFx (having also same name and birth year)
        #only if the two available NIFs are not standard SpanishNIFs and their sequence distance is large
        #(if are spanish NIFs but distance is small we assume a typo)
        dfagg2 = dfagg[dfagg['nnif']==2]
        dfagg2 = dfagg2.reset_index()

        def collapse_NIFs(listaNIF):

            colapsa = True
            NIF1 = listaNIF[0]
            NIF2 = listaNIF[1]

            if NIF1[-1].isalpha() and NIF2[-1].isalpha():
                try:
                    valor1 = int(NIF1[:-1])
                    valor2 = int(NIF2[:-1])
                    #If both conversions are correctly carried out
                    #that means that both NIFs are Spanish-like,
                    #therefore the NIFs should not be collapsed
                    colapsa = False
                except:
                    pass
            #In any case, if the distance is very small we should
            #collapse the NIFs
            if leve(NIF1, NIF2)<=2:
                colapsa = True

            return colapsa

        dfagg2['colapsa'] = dfagg2['NIF'].apply(collapse_NIFs)
        dfagg2 = dfagg2[dfagg2['colapsa']]
        dfagg2 = dfagg2.reset_index()
        dfagg2.to_excel('duplicated_researchers.xlsx')
        NIF2 = dfagg2['NIF'].values.tolist()

        for x in NIF2:
            #Count how many times each NIF appears on the table
            count0 = len(dfinvproj[dfinvproj['NIF']==x[0]])
            count1 = len(dfinvproj[dfinvproj['NIF']==x[1]])
            if count0>count1:
                #Keep first NIF
                NIF_to_remove.append(x[1])
                dictio_NIFs[x[1]] = x[0]
            else:
                #Keep second NIF
                NIF_to_remove.append(x[0])
                dictio_NIFs[x[0]] = x[1]

        #Removing duplicates from table "investigadores"
        NIFlist = ','.join(['"'+el+'"' for el in NIF_to_remove])
        print ('Removing duplicated NIFs from researcher table: ' + NIFlist)

        sqlcmd = 'DELETE from investigadores where NIF in (' + NIFlist + ')'
        self._c.execute(sqlcmd)

        self._conn.commit()

        #Updating table "investigadorproyecto"
        print ('Consolidating investigadorproyecto table')
        for i,el in enumerate(dictio_NIFs):
            oldnif = '"' + el + '"'
            newnif = '"' + dictio_NIFs[el] + '"'
            if not(i%100):
                print(i)
            sqlcmd = 'UPDATE investigadorproyecto SET NIF = ' + newnif + ' WHERE NIF = ' + oldnif
            self._c.execute(sqlcmd)
        self._c.commit()


    def run_translator(self):
        """
        This function reads a collection of projects from a given database, extracting
        data from 5 fields:
            - TITULO
            - KEYWORDS
            - PALABRAS CLAVE
            - RESUMEN
            - ABSTRACT

        For each project, it does the following:
            - Detects the language of the text in each field (Spanish or English). It
              does not care about the field name, because the database may be not
              consistent, and the text of the field may be in other language thant that
              suggested by the field name.
            - Selects one Spanish version for the Title, the Keywords and the Abstract.
            - If there is no Spanish version, an English version is translated.
            - The results are stores in the database, in fiels TITULO_ESP,
              PALABRAS_ESP, RESUMEN_ESP.
        """

        # String variables for easy editing the readTable command
        tokREF = '`REFERENCIA`'
        tokKEYes = '`PALABRAS CLAVE`'
        tokKEYen = '`KEYWORDS`'
        tokTIT = '`TITULO`'
        tokRESes = '`RESUMEN`'
        tokRESen = '`ABSTRACT`'
        selectOptions = ", ".join(
            (tokREF, tokTIT, tokKEYes, tokKEYen, tokRESes, tokRESen))
        filterOptions = '`TITULO_ESP` is NULL'  # AND

        # Read only  those projects that have no Spanish version in the output
        # field.
        rawData = self.readDBtable('proyectos', limit=None, selectOptions=selectOptions,
                    filterOptions=filterOptions, orderOptions=None)
        rawData = rawData.values.tolist()

        # Mnemonic variables for fancy indexing...
        idREF = 0
        idTIT = 1
        idKEYes = 2
        idKEYen = 3
        idRESes = 4
        idRESen = 5

        ###################
        # Data pre-analysis
        ###################

        # Some stats about the number of fields containing data.
        isKEYes = np.array([0 if d[idKEYes] == '' or d[idKEYes] is None
                            else 1 for d in rawData])
        isKEYen = np.array([0 if d[idKEYen] == '' or d[idKEYen] is None
                            else 1 for d in rawData])
        isKEYesen = isKEYes * isKEYen
        isRESes = np.array([0 if d[idRESes] == '' or d[idRESes] is None
                            else 1 for d in rawData])
        isRESen = np.array([0 if d[idRESen] == '' or d[idRESen] is None
                            else 1 for d in rawData])
        isRESesen = isRESes * isRESen

        nKEY_es = np.count_nonzero(isKEYes)
        nKEY_en = np.count_nonzero(isKEYen)
        nKEY_esen = np.count_nonzero(isKEYesen)
        nRES_es = np.count_nonzero(isRESes)
        nRES_en = np.count_nonzero(isRESen)
        nRES_esen = np.count_nonzero(isRESesen)

        n_items = len(rawData)

        print ("=========================")
        print ("Language project analysis")
        print ('\n')
        print (" -- Total no. of projects: {0}".format(n_items))
        print (" -- -- Keywords in the Spanish cells: {0}".format(nKEY_es))
        print (" -- -- Keywords in the English cells: {0}".format(nKEY_en))
        print (" -- -- Keywords in both Spanish and English cells: {0}".format(
                    nKEY_esen))
        print ('\n')
        print (" -- -- Abstracts in the Spanish cells: {0}".format(nRES_es))
        print (" -- -- Abstracts in the English cells: {0}".format(nRES_en))
        print (" -- -- Abstracts in both Spanish and English cells: {0}".format(
                    nRES_esen))

        # ####################
        # Extract text sources
        # ####################

        print ("===========================")
        print ("Extracting spanish versions")

        # In this section we collect a spanish version of keywords, titles and
        # abstract. When there is no Spanish version, an English one is selected
        # for translation.

        # Convert all titles to lowercase, and get a Spanish version:
        print ("--- Titles")
        txt_es = list(map(lambda d: d[idTIT].lower(), rawData))
        txt_en = [''] * len(txt_es)

        tit_out_es, statsTIT, langTIT = getSpanishVersion(txt_es, txt_en, translate_service=None)

        # Convert all keywords to lowercase, and get a Spanish version
        print ("--- Keywords")
        txt_es = list(map(lambda d: '' if d[idKEYes] is None else d[idKEYes].lower(),
                     rawData))
        txt_en = list(map(lambda d: '' if d[idKEYen] is None else d[idKEYen].lower(),
                     rawData))
        k_out_es, statsKEY, langKEY = getSpanishVersion(txt_es, txt_en, translate_service=None)

        # Convert all abstracts to lowercase, and get a Spanish version
        print ("--- Abstracts")
        txt_es = list(map(lambda d: '' if d[idRESes] is None else d[idRESes].lower(),
                     rawData))
        txt_en = list(map(lambda d: '' if d[idRESen] is None else d[idRESen].lower(),
                     rawData))
        res_out_es, statsRES, langRES = getSpanishVersion(txt_es, txt_en, translate_service=None)

        # #############
        # Join all data
        # #############

        # The only motivation to join keywords, titles and abstracts into a single
        # dataset it the need to shuffle all sentences so as to send them to
        # an external translation service without violating any confidentiality
        # restrictions.
        print ("Extracting english sentences")

        # Combine titles, keywords and abstracts
        textALL = tit_out_es + k_out_es + res_out_es
        # Preserve language indicators
        langALL = langTIT + langKEY + langRES
        # Preserve indicator of the data source
        origALL = (['TIT'] * len(tit_out_es) + ['KEY'] * len(k_out_es) +
                   ['RES'] * len(res_out_es))

        # Create corpus of sentences by fragmenting texts in English
        corpus_en = []
        corpus_ids = []
        for n, text in enumerate(textALL):
            # Split only texts in english
            if langALL[n] == 'en':
                if text is None:
                    s = ''
                else:
                    s = text.split('. ')
                len_s = len(s)
                corpus_en += s
                corpus_ids += [n] * len_s

        # Index the list of sentences in random order
        n_en = len(corpus_en)
        translateOrder = np.random.permutation(n_en)

        # ############
        # Translation
        # ###########
        corpus_es = [''] * n_en
        shuffled_corpus = [corpus_en[i] for i in translateOrder]

        # Translation with Google Service
        start = time.clock()
        corpus_out = google_tr(shuffled_corpus, translate_service='google', target_language='es')
        print("Translated in {0} seconds".format(str(time.clock() - start)))

        for i, s in enumerate(corpus_out):
            corpus_es[translateOrder[i]] = s

        # Replace text in textALL by its translations.
        for n, text in enumerate(textALL):

            if langALL[n] == 'en':
                # Join all sentences from the n-th text
                x = [c[1] for c in zip(corpus_ids, corpus_es) if c[0] == n]
                textALL[n] = '. '.join(x)

        # Recover keys, titles and abstracts (now in Spanish)
        k_out_es = [t[1] for t in zip(origALL, textALL) if t[0] == 'KEY']
        tit_out_es = [t[1] for t in zip(origALL, textALL) if t[0] == 'TIT']
        res_out_es = [t[1] for t in zip(origALL, textALL) if t[0] == 'RES']

        # Print some stats:
        print ('\n')
        print (" -- Total no. of projects: {0}".format(n_items))
        print (" -- -- Titles in Spanish: {0}".format(statsTIT['es']))
        print (" -- -- Titles in English: {0}".format(statsTIT['en']))
        print (" -- -- No titles: {0}".format(statsTIT['none']))
        print ('\n')
        print (" -- -- Keywords in Spanish: {0}".format(statsKEY['es']))
        print (" -- -- Keywords in English only: {0}".format(statsKEY['en']))
        print (" -- -- No Keywords: {0}".format(statsKEY['none']))
        print ('\n')
        print (" -- -- Abstracts in Spanish: {0}".format(statsRES['es']))
        print (" -- -- Abstracts in English only: {0}".format(statsRES['en']))
        print (" -- -- No abstract: {0}".format(statsRES['none']))

        listREFS = list(map(lambda x: x[idREF], rawData))
        key_ref = [el for el in zip(listREFS, k_out_es)]
        tit_ref = [el for el in zip(listREFS, tit_out_es)]
        res_ref = [el for el in zip(listREFS, res_out_es)]
        print ("Saving titles to database...")
        self.setField('proyectos', 'REFERENCIA', 'TITULO_ESP', tit_ref)
        print ("Saving keywords to database...")
        self.setField('proyectos', 'REFERENCIA', 'PALABRAS_ESP', key_ref)
        print ("Saving abstracts to database...")
        self.setField('proyectos', 'REFERENCIA', 'RESUMEN_ESP', res_ref)

    def run_translator_ENG(self, convocatoria):
        """
        This function reads a collection of projects from a given database, extracting
        data from 5 fields:
            - TITULO
            - KEYWORDS
            - PALABRAS CLAVE
            - RESUMEN
            - ABSTRACT

        For each project, it does the following:
            - Detects the language of the text in each field (Spanish or English). It
              does not care about the field name, because the database may be not
              consistent, and the text of the field may be in other language thant that
              suggested by the field name.
            - Selects one English version for the Title, the Keywords and the Abstract.
            - If there is no English version, an Spanish version is translated.
            - The results are stores in the database, in fiels TITULO_ENG,
              PALABRAS_ENG, RESUMEN_ENG.
        """

        # String variables for easy editing the readTable command
        tokREF = '`REFERENCIA`'
        tokKEYes = '`PALABRAS CLAVE`'
        tokKEYen = '`KEYWORDS`'
        tokTIT = '`TITULO`'
        tokRESes = '`RESUMEN`'
        tokRESen = '`ABSTRACT`'
        selectOptions = ", ".join(
            (tokREF, tokTIT, tokKEYes, tokKEYen, tokRESes, tokRESen))
        filterOptions = '`TITULO_ENG` is NULL'# and CONVOCATORIA='+convocatoria  # AND
        print(convocatoria)

        # Read only  those projects that have no Spanish version in the output
        # field.
        rawData = self.readDBtable('proyectos', limit=5200, selectOptions=selectOptions,
                    filterOptions=filterOptions, orderOptions=None)
        rawData = rawData.values.tolist()

        # Mnemonic variables for fancy indexing...
        idREF = 0
        idTIT = 1
        idKEYes = 2
        idKEYen = 3
        idRESes = 4
        idRESen = 5

        ###################
        # Data pre-analysis
        ###################

        # Some stats about the number of fields containing data.
        isKEYes = np.array([0 if d[idKEYes] == '' or d[idKEYes] is None
                            else 1 for d in rawData])
        isKEYen = np.array([0 if d[idKEYen] == '' or d[idKEYen] is None
                            else 1 for d in rawData])
        isKEYesen = isKEYes * isKEYen
        isRESes = np.array([0 if d[idRESes] == '' or d[idRESes] is None
                            else 1 for d in rawData])
        isRESen = np.array([0 if d[idRESen] == '' or d[idRESen] is None
                            else 1 for d in rawData])
        isRESesen = isRESes * isRESen

        nKEY_es = np.count_nonzero(isKEYes)
        nKEY_en = np.count_nonzero(isKEYen)
        nKEY_esen = np.count_nonzero(isKEYesen)
        nRES_es = np.count_nonzero(isRESes)
        nRES_en = np.count_nonzero(isRESen)
        nRES_esen = np.count_nonzero(isRESesen)

        n_items = len(rawData)

        print ("=========================")
        print ("Language project analysis")
        print ('\n')
        print (" -- Total no. of projects: {0}".format(n_items))
        print (" -- -- Keywords in the Spanish cells: {0}".format(nKEY_es))
        print (" -- -- Keywords in the English cells: {0}".format(nKEY_en))
        print (" -- -- Keywords in both Spanish and English cells: {0}".format(
                    nKEY_esen))
        print ('\n')
        print (" -- -- Abstracts in the Spanish cells: {0}".format(nRES_es))
        print (" -- -- Abstracts in the English cells: {0}".format(nRES_en))
        print (" -- -- Abstracts in both Spanish and English cells: {0}".format(
                    nRES_esen))

        # ####################
        # Extract text sources
        # ####################

        print ("===========================")
        print ("Extracting English versions")

        # In this section we collect a English version of keywords, titles and
        # abstract. When there is no English version, a Spanish one is selected
        # for translation.

        # Convert all titles to lowercase, and get a Spanish version:
        print ("--- Titles")
        txt_es = list(map(lambda d: d[idTIT].lower(), rawData))
        txt_en = [''] * len(txt_es)

        tit_out_en, statsTIT, langTIT = getEnglishVersion(txt_es, txt_en, translate_service=None)

        # Convert all keywords to lowercase, and get a Spanish version
        print ("--- Keywords")
        txt_es = list(map(lambda d: '' if d[idKEYes] is None else d[idKEYes].lower(),
                     rawData))
        txt_en = list(map(lambda d: '' if d[idKEYen] is None else d[idKEYen].lower(),
                     rawData))
        k_out_en, statsKEY, langKEY = getEnglishVersion(txt_es, txt_en, translate_service=None)

        # Convert all abstracts to lowercase, and get a Spanish version
        print ("--- Abstracts")
        txt_es = list(map(lambda d: '' if d[idRESes] is None else d[idRESes].lower(),
                     rawData))
        txt_en = list(map(lambda d: '' if d[idRESen] is None else d[idRESen].lower(),
                     rawData))
        res_out_en, statsRES, langRES = getEnglishVersion(txt_es, txt_en, translate_service=None)

        ##############
        # Traducciones NUEVA VERSION
        ##############

        tit_out_en_save = tit_out_en
        k_out_en_save = k_out_en
        res_out_en_save = res_out_en

        titulos_es = [el[0] for el in zip(tit_out_en,langTIT) if el[1]=='es']
        titulos_unicos_es = list(set(titulos_es))
        print('Traduciendo títulos:', len(titulos_unicos_es), 'de', len(titulos_es))
        titulos_trans = google_tr(titulos_unicos_es, translate_service='deepl', target_language='en')

        for k in range(len(langTIT)):
            if langTIT[k]=='es':
                idx = titulos_unicos_es.index(tit_out_en[k])
                tit_out_en[k] = titulos_trans[idx]

        key_es = [el[0] for el in zip(k_out_en,langKEY) if el[1]=='es']
        key_unicos_es = list(set(key_es))
        print('Traduciendo Keywords:', len(key_unicos_es), 'de', len(key_es))
        key_trans = google_tr(key_unicos_es, translate_service='deepl', target_language='en')

        for k in range(len(langKEY)):
            if langKEY[k]=='es':
                idx = key_unicos_es.index(k_out_en[k])
                k_out_en[k] = key_trans[idx]

        res_es = [el[0] for el in zip(res_out_en,langRES) if el[1]=='es']
        res_unicos_es = list(set(res_es))
        print('Traduciendo Resumenes:', len(res_unicos_es), 'de', len(res_es))
        res_trans = google_tr(res_unicos_es, translate_service='deepl', target_language='en')

        for k in range(len(langRES)):
            if langRES[k]=='es':
                idx = res_unicos_es.index(res_out_en[k])
                res_out_en[k]=res_trans[idx]

        # # #############
        # # Join all data
        # # #############

        # # The only motivation to join keywords, titles and abstracts into a single
        # # dataset is the need to shuffle all sentences so as to send them to
        # # an external translation service without violating any confidentiality
        # # restrictions.
        # print ("Extracting Spanish sentences")

        # # Combine titles, keywords and abstracts
        # textALL = tit_out_en + k_out_en + res_out_en
        # # Preserve language indicators
        # langALL = langTIT + langKEY + langRES
        # # Preserve indicator of the data source
        # origALL = (['TIT'] * len(tit_out_en) + ['KEY'] * len(k_out_en) +
        #            ['RES'] * len(res_out_en))

        # # Create corpus of sentences by fragmenting texts in Spanish
        # corpus_es = []
        # corpus_ids = []
        # for n, text in enumerate(textALL):
        #     # Split only texts in Spanish
        #     if langALL[n] == 'es':
        #         if text is None:
        #             s = ''
        #         else:
        #             s = text.split('. ')
        #         len_s = len(s)
        #         corpus_es += s
        #         corpus_ids += [n] * len_s

        # # Index the list of sentences in random order
        # n_es = len(corpus_es)
        # translateOrder = np.random.permutation(n_es)

        # # ############
        # # Translation
        # # ###########
        # corpus_en = [''] * n_es
        # shuffled_corpus = [corpus_es[i] for i in translateOrder]

        # # Translation with Google Service
        # start = time.time()
        # #corpus_out = google_tr(shuffled_corpus, translate_service='google', target_language='en')
        # #corpus_out = google_tr(shuffled_corpus, translate_service='deepl', target_language='en')
        # #Como suele haber frases repetidas vamos a proceder de la siguietne manera:
        # #1.Creamos un corpus sin repeticiones
        # #2.Traducimos dicho corpus
        # #3.Reconstruimos corpus_out buscando los índices de las traducciones
        # unique_corpus = list(set(shuffled_corpus))
        # print(len(shuffled_corpus), len(unique_corpus))
        # #ipdb.set_trace()
        # unique_translated = google_tr(unique_corpus, translate_service='deepl', target_language='en')
        # print("Translated in {0} seconds".format(str(time.time() - start)))
        # corpus_out = []
        # for s in shuffled_corpus:
        #     #Índice de la frase en unique corpus
        #     idx = unique_corpus.index(s)
        #     #Añadimos la frase traducida correspondiente
        #     corpus_out.append(unique_translated[idx])

        # for i, s in enumerate(corpus_out):
        #     corpus_en[translateOrder[i]] = s

        # # Replace text in textALL by its translations.
        # for n, text in enumerate(textALL):

        #     if langALL[n] == 'es':
        #         # Join all sentences from the n-th text
        #         x = [c[1] for c in zip(corpus_ids, corpus_en) if c[0] == n]
        #         textALL[n] = '. '.join(x)

        # # Recover keys, titles and abstracts (now in English)
        # k_out_en = [t[1] for t in zip(origALL, textALL) if t[0] == 'KEY']
        # tit_out_en = [t[1] for t in zip(origALL, textALL) if t[0] == 'TIT']
        # res_out_en = [t[1] for t in zip(origALL, textALL) if t[0] == 'RES']

        # # Print some stats:
        # print ('\n')
        # print (" -- Total no. of projects: {0}".format(n_items))
        # print (" -- -- Titles in Spanish: {0}".format(statsTIT['es']))
        # print (" -- -- Titles in English: {0}".format(statsTIT['en']))
        # print (" -- -- No titles: {0}".format(statsTIT['none']))
        # print ('\n')
        # print (" -- -- Keywords in Spanish: {0}".format(statsKEY['es']))
        # print (" -- -- Keywords in English only: {0}".format(statsKEY['en']))
        # print (" -- -- No Keywords: {0}".format(statsKEY['none']))
        # print ('\n')
        # print (" -- -- Abstracts in Spanish: {0}".format(statsRES['es']))
        # print (" -- -- Abstracts in English only: {0}".format(statsRES['en']))
        # print (" -- -- No abstract: {0}".format(statsRES['none']))

        listREFS = list(map(lambda x: x[idREF], rawData))
        key_ref = [el for el in zip(listREFS, k_out_en)]
        tit_ref = [el for el in zip(listREFS, tit_out_en)]
        res_ref = [el for el in zip(listREFS, res_out_en)]

        print ("Saving titles to database...")
        self.setField('proyectos', 'REFERENCIA', 'TITULO_ENG', tit_ref)
        print ("Saving keywords to database...")
        self.setField('proyectos', 'REFERENCIA', 'PALABRAS_ENG', key_ref)
        print ("Saving abstracts to database...")
        self.setField('proyectos', 'REFERENCIA', 'RESUMEN_ENG', res_ref)
        
    def detecta_coord(self, coordinatedfile):
        """Detection of coordinated projects"""
        
        dfprojects = self.readDBtable('proyectos', limit=None, 
                    selectOptions='`TITULO CONVOCATORIA`, `TITULO`, `REFERENCIA`, '\
                    '`CONVOCATORIA`, `STATUS`, `TIPO DE PROYECTO`')

        coord = Tambourine (dfprojects, verbose=True)
        coord.calculaCoordinados ()
        coord.save_to_excel(coordinatedfile)
        


















    # def detecta_coord_old(self):
    #     """Detection of coordinated projects"""

    #     df = self.readDBtable('proyectos', limit=None, 
    #                 selectOptions='REFERENCIA, TITULO_ESP',
    #                 filterOptions='TITULO_ESP IS NOT NULL AND LENGTH(TITULO_ESP)')

    #     def find_structure(ref):

    #         structure = ''
    #         for i,el in enumerate(ref):
    #             if el.isalpha():
    #                 if i>0 and structure[-1]=='-':
    #                     structure+=el
    #                 else:
    #                     structure+='a'
    #             elif el.isnumeric():
    #                 structure+='0'
    #             elif el=='-':
    #                 structure+='-'
    #             else:
    #                 structure+='?'

    #         return structure

    #     ref_str_tit_res = list(map(lambda x: (x[0], find_structure(x[0]), x[1], len(x[1])), df.values))
    #     #refs_str = list(map(lambda x: x[1], ref_str_tit_res))
    #     #count = Counter(refs_str)

    #     #print(count)
    #     th = 50
    #     min_len = 10
    #     th_leve = 0.9

    #     coord_list = []

    #     n = 0

    #     #We start by filtering out all projects that do not have sufficient length in the 
    #     #title to be considered as coordinated
    #     ref_str_tit_res = [el for el in ref_str_tit_res if el[-1]>min_len]

    #     for struct in convocatorias_structs:
    #         #Retain projects with the right structure
    #         projects_struct = list(filter(lambda x: x[1]==struct, ref_str_tit_res))
    #         #Find all possible subsets, number of same initial chars depend on reference structure
    #         allcodes = set(list(map(lambda x: x[0][:convocatorias_structs[struct]],projects_struct)))
    #         for code in allcodes:
    #             projects_code = [el for el in projects_struct if el[0].startswith(code)]
    #             if len(projects_code)>1:
    #                 n = n+1
    #                 print(n,code,len(projects_code))
    #                 for i, sel1 in enumerate(projects_code)


    #     ipdb.set_trace()










    # def _importLemmas(self, data_path='import', filename='corpus_lemmas.xlsx'):

    #     # Load lemmas from file
    #     print('---- ---- Reading lemmas from file')
    #     df = pd.read_excel(os.path.join(data_path, filename))

    #     # Add new columns to database if they do not exist
    #     if self.connector == 'mysql':
    #         self.addTableColumn('proyectos', 'Resumen_lang', 'VARCHAR(5)')
    #         self.addTableColumn('proyectos', 'Titulo_lang', 'VARCHAR(5)')
    #     else:
    #         self.addTableColumn('proyectos', 'Resumen_lang', 'TEXT')
    #         self.addTableColumn('proyectos', 'Titulo_lang', 'TEXT')
    #     self.addTableColumn('proyectos', 'Resumen_lemas', 'TEXT')
    #     self.addTableColumn('proyectos', 'Titulo_lemas', 'TEXT')

    #     # Save lemmas in database.
    #     print('---- ---- Saving lemmas into database')
    #     self.upsert('proyectos', 'Referencia', df)

    #     return

    # def _importCProjects(self, data_path='import', fname='c_projects.xlsx'):
    #     '''
    #     Imports the groups of coordinated projects.
    #     Each group will be identified by the name of one of its members (the
    #     first in the list)
    #     A new column of table proyectos (named GroupRef) is added, assigning
    #     each project to its corresponding group.
    #     '''

    #     # Load lemmas from file
    #     print('---- ---- Reading coordination data from files')
    #     df = pd.read_excel(os.path.join(data_path, fname))

    #     # Add new columns to database if they do not exist
    #     if 'GroupRef' not in self.getColumnNames('proyectos'):
    #         self.addTableColumn('proyectos', 'GroupRef', 'TEXT')
    #     else:
    #         print('---- ---- Column GroupRef already exist. Current values ' +
    #               'will be overwritten.')

    #     # Save lemmas in database.
    #     print('---- ---- Saving cordination data into database')
    #     self.upsert('proyectos', 'Referencia', df)

    #     # ## OLD VERSION
    #     # ## This is the old version, based on code by Jeronimo Arenas.
    #     # ## It is no longer used because imporing from xlsx files exported
    #     # ## from the database is simpler.
    #     #
    #     # # Read the file containing the list of grups of coordinated projects
    #     # fpath = os.path.join(data_path, filename)
    #     # with open(fpath, 'rb') as f:
    #     #     coords = pickle.load(f)

    #     # # By default, we assign each project to itself
    #     # refs = self.readDBtable('proyectos', selectOptions='Referencia')
    #     # ref2coord = dict(zip(refs, refs))

    #     # # Assign each project in a group to its group representative
    #     # for group in coords:
    #     #     for p in group[0]:
    #     #         # Each group is represented by the first project in the list
    #     #         ref2coord[p] = group[0][0]
    #     # refcoord = list(ref2coord.items())

    #     # # Create column in table if it does not exist and fill it
    #     # if 'GroupRef' not in self.getColumnNames('proyectos'):
    #     #     self.addTableColumn('proyectos', 'GroupRef', 'TEXT')
    #     # self.setField('proyectos', 'Referencia', 'GroupRef', refcoord)

    #     return

