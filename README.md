# DBimport

This repository provides code for importing different open datasets into corresponding MySQL databases. These databases have been used in several projects under the [Language Technologies Plan](https://www.plantl.gob.es/) of the [Spain State Secretary for Digital Advancement](https://avancedigital.gob.es).

Each dataset can be imported using a different python script with specific options that are intrinsic to the structure of the database as described below.

Note that we do not provide the original data for any of the datasets. Therefore, users will need to obtain the data from the source providers, and it will be their responsability to comply with any copyright or use limitations imposed by the data owners.


# Quick Start

1. Apart from forking the repository, make sure that the `dbmanager` submodule code is retrieved by executing

```
git submodule init
```

2. Most of the datasets provide an option to lemmatize the textual fields, which is a necessary step before training other machine learning models based either on Bags of Words or TFIDF. Inside this project we use the [librAIry](https://github.com/librairy/nlp) lemmatization service. If you are planning to use this service, you will need to deploy it using the instructions provided in that repository.

3. Other python libraries that are used in the repository are listed in the `requirements.txt` file

4. Configuration file: Rename file `config.cf.default` to `config.cf`, and update some variables as needed. The configuration file includes several sections:
   * [DB]: MySQL server address and authentication credentials
   * [S2]: Specific settings for the Semantic Scholar importer
   * [FIS]: Specific settings for the FIS project importer
   * [Lemmatizer]: Address of the librAIry REST API, and configuration settings for the lemmatization


# Datasets

## Semantic Scholar

Semantic Scholar (S2) is a database of more than 45 M scientific publications, including abstract, author information, recognized entities and some additional bibliographic metadata. Apart from a REST API service, S2 provides an [open data](https://api.semanticscholar.org/corpus/) collection in JSON format. 

In order to import the S2 data you need to run the `importS2.py` script with one or several of the following options

   * resetDB: If activated, the database will be reset and the schema will be regenerated
   * importPapers: Import paper metadata
   * importCitations: Import Citation data
   * importAuthors: Import author metadata
   * importFields: Fill in paper vs (journals/volumes/fieldOfStudy) tables
   * importEntities: Fill in paper vs Entities table
   * lemmatize: lemmatize database
   * lemmas_query: Use this flag followed by an SQL query to select the paper abstracts that will be lemmatized. E.g.: 
   
   ```>> python importS2.py --lemmatize --lemmas_query "isDBLP=1 and LEMAS is NULL" ```

Detailed information about the database structure and some statistical analysis can be found in the [database documentation](https://github.com/PlanTL-INTELCOMP/DBimport/blob/master/documentation/Pu_S2_description.docx).

## FIS (Instituto de Salud Carlos III)

FIS (Fondo de Investigación en Salud) is a database of 2607 projects funded by Instituto de Salud Carlos III, including abstract, author information, keywords, duration, budget. Project information is available at [Portal FIS] (https://portalfis.isciii.es/es/Paginas/inicio.aspx). The code provided in this repository is prepared to download project information from the website.

In order to import the FIS data you need to run the `importFIS.py` script with one or several of the following options

   * download: If activated, project information will be downloaded from the FIS portal
   * resetDB: If activated, the database will be reset and the schema will be regenerated
   * importData: Import project metadata
   
   ```>> python importFIS.py --download --resetDB --importData ```

Detailed information about the database structure and some statistical analysis can be found in the project Wiki.

