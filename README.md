# Scientific database
Automated database that stores information on scientific articles from Scopus and PubMed, based on searches by keywords

## Create database
Use the script _[create_database.sql](./create_database.sql)_    to create the database in MySQL

## Database configuration file
You must properly configure the _[config_db.py](./config_db.py)_ file
```
    DATABASE_CONFIG = {'host': '',
                       'user': 'lbmcf',
                       'password': '',
                       'db': 'publication_db'}
```
- **host**: Host or IP
- **user**: Database user
- **password**: Database user password
- **db**: Name of the database

## Application configuration file (Back-end)
You must properly configure the _[config_app.py](./config_app.py)_ file
```
    APP_CONFIG = {'python_path': '',
                  'app_path': '',
                  'query_scopus': '',
                  'query_pubmed': '',
                  'api_key_scopus': '',
                  'api_key_pubmed': '',
                  'crossref': False,
                  'first_time': False}
```
- **python_path**: Absolute path of the Python 3 executable
- **app_path**: Absolute path of the application folder (scientific_database path)
- **query_scopus**: Keywords to search in Scopus
- **query_pubmed**: Keywords to search PubMed
- **api_key_scopus**: Scopus API Key. [Here](https://dev.elsevier.com), more information to get your key. (**Required**)
- **api_key_pubmed**: E-utilities API Key. [Here](https://ncbiinsights.ncbi.nlm.nih.gov/2017/11/02/new-api-keys-for-the-e-utilities), more information to get your key. (Default: '')
- **crossref**: Flag (True or False) that allows complementary data searches from [Crossref](https://www.crossref.org). (Default: False)
- **first_time**: Flag (True or False) that indicates if it is the first time the application will be executed (Back-end). (Default: False)

## Running the application
```
  $ python3 main.py
```

## Author

* [Glen Jasper](https://github.com/glenjasper)

## Organization
* [Molecular and Computational Biology of Fungi Laboratory](http://lbmcf.pythonanywhere.com) (LBMCF, ICB - **UFMG**, Belo Horizonte, Brazil).

## License

This project is licensed under the MIT License - see the [LICENSE](./LICENSE) file for details.
