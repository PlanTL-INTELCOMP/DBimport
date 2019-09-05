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


# Configuration file


# Datasets

## Semantic Scholar

Semantic Scholar (S2) is a database of more than 45 M scientific publications, including abstract, author information, recognized entities and some additional bibliographic metadata. Apart from a REST API service, S2 provides an [open data](https://api.semanticscholar.org/corpus/) collection in JSON format. 

In order to import the S2 data you need to run the `importSScholar.py` script with one or several of the following options

   * resetDB: If activated, the database will be reset and the schema will be regenerated
   * importData: Import author and paper metadata
   * importCitations: import citation data
   * importAuthorship: Fill in a paper vs author table
   * importEntities: Fill in a paper vs entity table
   * lemmatize: lemmatize database
   * lemmas_query: Use this flag followed by an SQL query to select the paper abstracts that will be lemmatized. E.g.: 
   
    ```   >> python importSScholar.py --lemmatize --lemmas_query "DBLP=1 and LEMAS is NULL" ```
