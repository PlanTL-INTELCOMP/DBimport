# DBimport

This repository provides code for importing different open datasets into corresponding MySQL databases. These databases have been used in several projects under the [Language Technologies Plan](https://www.plantl.gob.es/) of the [Spain State Secretary for Digital Advancement](https://avancedigital.gob.es).

Each dataset can be imported using a different python script with specific options that are intrinsic to the structure of the database as described below.


# Quick Installation instructions

1. Apart from forking the repository make sure the `dbmanager` submodule code is retrieved by executing

```
git submodule init
```

2. Most of the datasets provide an option to lemmatize the textual fields, which is a necessary step before training other machine learning models based either on Bags of Words or TFIDF. Inside this project we use the [librAIry](https://github.com/librairy/nlp) lemmatization service. If you are planning to use this service, you will need to deploy it using the instructions provided in that repository.

3. Other python libraries that are used in the repository are listed in the requirements.txt file
