# Scientific database
Automated database that stores information on scientific articles from Scopus and PubMed, based on searches by keywords

## Create database
Use the script _create_database.sql_ to create the database in MySQL

## Database configuration file
You must properly configure the _config_app.py_ file
```bash
    DATABASE_CONFIG = {'host': '127.0.0.1',
                       'user': 'lbmcf',
                       'password': 'P@55w0rd',
                       'db': 'publication_db'}
```
```
    DATABASE_CONFIG = {'host': '127.0.0.1',
                       'user': 'lbmcf',
                       'password': 'P@55w0rd',
                       'db': 'publication_db'}
```

## Application configuration file (Back-end)


    APP_CONFIG = {'python_path': '',
                  'app_path': '',
                  'query_scopus': '',
                  'query_pubmed': '',
                  'crossref': False,
                  'first_time': False}

