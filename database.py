#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import mysql.connector as mysql
from config_db import Config_DB

class Database:

    def __init__(self):
        # https://pynative.com/python-mysql-database-connection

        # PubMed
        # Table
        self.TABLE_PUBLICATION_PUBMED = 'publication_pubmed'

        # Columns
        self.DB_PUBMED_ID = 'pubmed_id'
        self.DB_PUBMED_DOI = 'doi'
        self.DB_PUBMED_TITLE = 'title'
        self.DB_PUBMED_AUTHOR = 'author'
        self.DB_PUBMED_JOURNAL_NAME = 'journal_name'
        self.DB_PUBMED_PUBLICATION_DATE = 'publication_date'
        self.DB_PUBMED_COUNTRY = 'country'
        self.DB_PUBMED_ABSTRACT = 'abstract'
        self.DB_PUBMED_LANGUAGE = 'language'
        self.DB_PUBMED_PUBLICATION_TYPE = 'publication_type'

        # Scopus
        # Table
        self.TABLE_PUBLICATION_SCOPUS = 'publication_scopus'

        # Columns
        self.DB_SCOPUS_ID = 'scopus_id'
        self.DB_SCOPUS_PMID = 'pubmed_id'
        self.DB_SCOPUS_DOI = 'doi'
        self.DB_SCOPUS_TITLE = 'title'
        self.DB_SCOPUS_AUTHOR = 'author'
        self.DB_SCOPUS_JOURNAL_NAME = 'journal_name'
        self.DB_SCOPUS_PUBLICATION_DATE = 'publication_date'
        self.DB_SCOPUS_PUBLICATION_TYPE = 'publication_type'
        self.DB_SCOPUS_DOCUMENT_CODE = 'document_code'
        self.DB_SCOPUS_DOCUMENT_TYPE = 'document_type'
        self.DB_SCOPUS_CITED_BY = 'cited_by'
        self.DB_SCOPUS_OPEN_ACCESS = 'open_access'
        self.DB_SCOPUS_AFFILIATION = 'affiliation'

        # Table
        self.TABLE_PUBLICATION_TODAY = 'publication_today'

        # Columns
        self.DB_PUBLICATION_TODAY_SCOPUS_ID = 'scopus_id'
        self.DB_PUBLICATION_TODAY_PUBMED_ID = 'pubmed_id'
        self.DB_PUBLICATION_TODAY_DOI = 'doi'
        self.DB_PUBLICATION_TODAY_TITLE = 'title'
        self.DB_PUBLICATION_TODAY_AUTHOR = 'author'
        self.DB_PUBLICATION_TODAY_JOURNAL_NAME = 'journal_name'
        self.DB_PUBLICATION_TODAY_PUBLICATION_DATE = 'publication_date'
        self.DB_PUBLICATION_TODAY_PUBLICATION_TYPE = 'publication_type'
        self.DB_PUBLICATION_TODAY_DOCUMENT_TYPE = 'document_type'
        self.DB_PUBLICATION_TODAY_CITED_BY = 'cited_by'
        self.DB_PUBLICATION_TODAY_AFFILIATION = 'affiliation'
        self.DB_PUBLICATION_TODAY_ABSTRACT = 'abstract'
        self.DB_PUBLICATION_TODAY_LANGUAGE = 'language'
        self.DB_PUBLICATION_TODAY_COUNTRY = 'country'
        self.DB_PUBLICATION_TODAY_REPOSITORY = 'repository'

        # Table
        self.TABLE_PUBLICATION_UPDATED = 'publication_updated'

        # Columns
        self.DB_PUBLICATION_UPDATED_SCOPUS_ID = 'scopus_id'
        self.DB_PUBLICATION_UPDATED_PUBMED_ID = 'pubmed_id'
        self.DB_PUBLICATION_UPDATED_DOI = 'doi'
        self.DB_PUBLICATION_UPDATED_TITLE = 'title'
        self.DB_PUBLICATION_UPDATED_AUTHOR = 'author'
        self.DB_PUBLICATION_UPDATED_JOURNAL_NAME = 'journal_name'
        self.DB_PUBLICATION_UPDATED_PUBLICATION_DATE = 'publication_date'
        self.DB_PUBLICATION_UPDATED_PUBLICATION_TYPE = 'publication_type'
        self.DB_PUBLICATION_UPDATED_DOCUMENT_TYPE = 'document_type'
        self.DB_PUBLICATION_UPDATED_CITED_BY = 'cited_by'
        self.DB_PUBLICATION_UPDATED_AFFILIATION = 'affiliation'
        self.DB_PUBLICATION_UPDATED_ABSTRACT = 'abstract'
        self.DB_PUBLICATION_UPDATED_LANGUAGE = 'language'
        self.DB_PUBLICATION_UPDATED_COUNTRY = 'country'
        self.DB_PUBLICATION_UPDATED_REPOSITORY = 'repository'
        self.DB_PUBLICATION_UPDATED_PUBLISHER = 'publisher'
        self.DB_PUBLICATION_UPDATED_URL = 'url'

        # Table
        self.TABLE_PUBLICATION_LOG = 'publication_log'

        # Columns
        self.DB_PUBLICATION_LOG_ID = 'id'
        self.DB_PUBLICATION_LOG_PROCESS = 'process'
        self.DB_PUBLICATION_LOG_STATUS = 'status'
        self.DB_PUBLICATION_LOG_MESSAGE = 'message'
        self.DB_PUBLICATION_LOG_DATE_START = 'date_start'
        self.DB_PUBLICATION_LOG_DATE_FINISH = 'date_finish'

        # Constants
        self.DB_CONST_SCOPUS = 'Scopus'
        self.DB_CONST_PUBMED = 'PubMed'

        self.DB_CONST_STATUS_OK = 'Ok'
        self.DB_CONST_STATUS_RUNNING = 'Running'
        self.DB_CONST_STATUS_FAILED = 'Failed'

        self.DB_CONST_PROCESS_PUBMED = 'Search in PubMed'
        self.DB_CONST_PROCESS_SCOPUS = 'Search in Scopus'
        self.DB_CONST_PROCESS_CONSENSUS = 'Consolidating scientific publications'

    def connect_db(self, autocommit = False, dictionary = True):
        _connection = mysql.connect(host = Config_DB.DATABASE_CONFIG['host'],
                                    user = Config_DB.DATABASE_CONFIG['user'],
                                    passwd = Config_DB.DATABASE_CONFIG['password'],
                                    database = Config_DB.DATABASE_CONFIG['db'],
                                    auth_plugin = 'mysql_native_password',
                                    autocommit = autocommit)
        _cursor = _connection.cursor(dictionary = dictionary)

        return _connection, _cursor

    def close_db(self, connection, cursor):
        if connection.is_connected():
            cursor.close()
            connection.close()

    def test(self):
        connection, cursor = self.connect_db()
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        for table in tables:
            print(table)

    def truncate_table(self, table):
        msg = ''
        try:
            connection, cursor = self.connect_db()

            query = 'TRUNCATE %s' % table
            cursor.execute(query)

            connection.commit()
            msg = 'Successful truncate'
        except mysql.connector.Error as error:
            msg = 'Failed to Delete all records from database table: {}'.format(error)
        finally:
            self.close_db(connection, cursor)

        return msg

    def insert_publication_pubmed_individual(self, data_item):
        try:
            connection, cursor = self.connect_db()

            query = 'INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)' % \
                    (self.TABLE_PUBLICATION_PUBMED,
                     self.DB_PUBMED_ID,
                     self.DB_PUBMED_DOI,
                     self.DB_PUBMED_TITLE,
                     self.DB_PUBMED_AUTHOR,
                     self.DB_PUBMED_JOURNAL_NAME,
                     self.DB_PUBMED_PUBLICATION_DATE,
                     self.DB_PUBMED_COUNTRY,
                     self.DB_PUBMED_ABSTRACT,
                     self.DB_PUBMED_LANGUAGE,
                     self.DB_PUBMED_PUBLICATION_TYPE)
            query += ' VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

            record = (data_item[self.DB_PUBMED_ID],
                      data_item[self.DB_PUBMED_DOI],
                      data_item[self.DB_PUBMED_TITLE],
                      data_item[self.DB_PUBMED_AUTHOR],
                      data_item[self.DB_PUBMED_JOURNAL_NAME],
                      data_item[self.DB_PUBMED_PUBLICATION_DATE],
                      data_item[self.DB_PUBMED_COUNTRY],
                      data_item[self.DB_PUBMED_ABSTRACT],
                      data_item[self.DB_PUBMED_LANGUAGE],
                      data_item[self.DB_PUBMED_PUBLICATION_TYPE])

            cursor.execute(query, record)
            connection.commit()
            # print("Record inserted successfully into Laptop table")
        except mysql.connector.Error as error:
            print("Failed to insert record into MySQL table: {}".format(error))
        finally:
            self.close_db(connection, cursor)

    def insert_publication_pubmed(self, cursor, data_item):
        query = 'INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)' % \
                (self.TABLE_PUBLICATION_PUBMED,
                 self.DB_PUBMED_ID,
                 self.DB_PUBMED_DOI,
                 self.DB_PUBMED_TITLE,
                 self.DB_PUBMED_AUTHOR,
                 self.DB_PUBMED_JOURNAL_NAME,
                 self.DB_PUBMED_PUBLICATION_DATE,
                 self.DB_PUBMED_COUNTRY,
                 self.DB_PUBMED_ABSTRACT,
                 self.DB_PUBMED_LANGUAGE,
                 self.DB_PUBMED_PUBLICATION_TYPE)
        query += ' VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

        record = (data_item[self.DB_PUBMED_ID],
                  data_item[self.DB_PUBMED_DOI],
                  data_item[self.DB_PUBMED_TITLE],
                  data_item[self.DB_PUBMED_AUTHOR],
                  data_item[self.DB_PUBMED_JOURNAL_NAME],
                  data_item[self.DB_PUBMED_PUBLICATION_DATE],
                  data_item[self.DB_PUBMED_COUNTRY],
                  data_item[self.DB_PUBMED_ABSTRACT],
                  data_item[self.DB_PUBMED_LANGUAGE],
                  data_item[self.DB_PUBMED_PUBLICATION_TYPE])

        cursor.execute(query, record)

    def insert_publication_pubmed_transactional(self, data_list, data_log):
        msg = ''
        try:
            connection, cursor = self.connect_db()
            connection.start_transaction()

            for _, item in data_list.items():
                self.insert_publication_pubmed(cursor, item)

            self.update_publication_log(data_log, transactional = True, cursor = cursor)

            connection.commit()
            msg = 'Successful transaction'
        except mysql.connector.Error as error:
            msg = 'Failed to insert record to database rollback: {}'.format(error)
            connection.rollback()
        finally:
            self.close_db(connection, cursor)
        return msg

    def insert_publication_scopus(self, cursor, data_item):
        query = 'INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)' % \
                (self.TABLE_PUBLICATION_SCOPUS,
                 self.DB_SCOPUS_ID,
                 self.DB_SCOPUS_PMID,
                 self.DB_SCOPUS_DOI,
                 self.DB_SCOPUS_TITLE,
                 self.DB_SCOPUS_AUTHOR,
                 self.DB_SCOPUS_JOURNAL_NAME,
                 self.DB_SCOPUS_PUBLICATION_DATE,
                 self.DB_SCOPUS_PUBLICATION_TYPE,
                 self.DB_SCOPUS_DOCUMENT_CODE,
                 self.DB_SCOPUS_DOCUMENT_TYPE,
                 self.DB_SCOPUS_CITED_BY,
                 self.DB_SCOPUS_OPEN_ACCESS,
                 self.DB_SCOPUS_AFFILIATION)
        query += ' VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

        record = (data_item[self.DB_SCOPUS_ID],
                  data_item[self.DB_SCOPUS_PMID],
                  data_item[self.DB_SCOPUS_DOI],
                  data_item[self.DB_SCOPUS_TITLE],
                  data_item[self.DB_SCOPUS_AUTHOR],
                  data_item[self.DB_SCOPUS_JOURNAL_NAME],
                  data_item[self.DB_SCOPUS_PUBLICATION_DATE],
                  data_item[self.DB_SCOPUS_PUBLICATION_TYPE],
                  data_item[self.DB_SCOPUS_DOCUMENT_CODE],
                  data_item[self.DB_SCOPUS_DOCUMENT_TYPE],
                  data_item[self.DB_SCOPUS_CITED_BY],
                  data_item[self.DB_SCOPUS_OPEN_ACCESS],
                  data_item[self.DB_SCOPUS_AFFILIATION])

        cursor.execute(query, record)

    def insert_publication_scopus_transactional(self, data_list, data_log):
        msg = ''
        try:
            connection, cursor = self.connect_db()
            connection.start_transaction()

            for _, item in data_list.items():
                self.insert_publication_scopus(cursor, item)

            self.update_publication_log(data_log, transactional = True, cursor = cursor)

            connection.commit()
            msg = 'Successful transaction'
        except mysql.connector.Error as error:
            msg = 'Failed to insert record to database rollback: {}'.format(error)
            connection.rollback()
        finally:
            self.close_db(connection, cursor)

        return msg

    def insert_publication_today(self, cursor, data_item):
        query = 'INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)' % \
                (self.TABLE_PUBLICATION_TODAY,
                 self.DB_PUBLICATION_TODAY_SCOPUS_ID,
                 self.DB_PUBLICATION_TODAY_PUBMED_ID,
                 self.DB_PUBLICATION_TODAY_DOI,
                 self.DB_PUBLICATION_TODAY_TITLE,
                 self.DB_PUBLICATION_TODAY_AUTHOR,
                 self.DB_PUBLICATION_TODAY_JOURNAL_NAME,
                 self.DB_PUBLICATION_TODAY_PUBLICATION_DATE,
                 self.DB_PUBLICATION_TODAY_PUBLICATION_TYPE,
                 self.DB_PUBLICATION_TODAY_DOCUMENT_TYPE,
                 self.DB_PUBLICATION_TODAY_CITED_BY,
                 self.DB_PUBLICATION_TODAY_AFFILIATION,
                 self.DB_PUBLICATION_TODAY_ABSTRACT,
                 self.DB_PUBLICATION_TODAY_LANGUAGE,
                 self.DB_PUBLICATION_TODAY_COUNTRY,
                 self.DB_PUBLICATION_TODAY_REPOSITORY)
        query += ' VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

        record = (data_item[self.DB_PUBLICATION_TODAY_SCOPUS_ID],
                  data_item[self.DB_PUBLICATION_TODAY_PUBMED_ID],
                  data_item[self.DB_PUBLICATION_TODAY_DOI],
                  data_item[self.DB_PUBLICATION_TODAY_TITLE],
                  data_item[self.DB_PUBLICATION_TODAY_AUTHOR],
                  data_item[self.DB_PUBLICATION_TODAY_JOURNAL_NAME],
                  data_item[self.DB_PUBLICATION_TODAY_PUBLICATION_DATE],
                  data_item[self.DB_PUBLICATION_TODAY_PUBLICATION_TYPE],
                  data_item[self.DB_PUBLICATION_TODAY_DOCUMENT_TYPE],
                  data_item[self.DB_PUBLICATION_TODAY_CITED_BY],
                  data_item[self.DB_PUBLICATION_TODAY_AFFILIATION],
                  data_item[self.DB_PUBLICATION_TODAY_ABSTRACT],
                  data_item[self.DB_PUBLICATION_TODAY_LANGUAGE],
                  data_item[self.DB_PUBLICATION_TODAY_COUNTRY],
                  ', '.join(data_item[self.DB_PUBLICATION_TODAY_REPOSITORY]))

        cursor.execute(query, record)

    def insert_publication_today_transactional(self, data_list):
        msg = ''
        try:
            connection, cursor = self.connect_db()
            connection.start_transaction()

            for _, item in data_list.items():
                self.insert_publication_today(cursor, item)

            connection.commit()
            msg = 'Successful transaction'
        except mysql.connector.Error as error:
            msg = 'Failed to insert record to database rollback: {}'.format(error)
            connection.rollback()
        finally:
            self.close_db(connection, cursor)

        return msg

    def insert_publication_updated(self, cursor, data_item):
        query = 'INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)' % \
                (self.TABLE_PUBLICATION_UPDATED,
                 self.DB_PUBLICATION_UPDATED_SCOPUS_ID,
                 self.DB_PUBLICATION_UPDATED_PUBMED_ID,
                 self.DB_PUBLICATION_UPDATED_DOI,
                 self.DB_PUBLICATION_UPDATED_TITLE,
                 self.DB_PUBLICATION_UPDATED_AUTHOR,
                 self.DB_PUBLICATION_UPDATED_JOURNAL_NAME,
                 self.DB_PUBLICATION_UPDATED_PUBLICATION_DATE,
                 self.DB_PUBLICATION_UPDATED_PUBLICATION_TYPE,
                 self.DB_PUBLICATION_UPDATED_DOCUMENT_TYPE,
                 self.DB_PUBLICATION_UPDATED_CITED_BY,
                 self.DB_PUBLICATION_UPDATED_AFFILIATION,
                 self.DB_PUBLICATION_UPDATED_ABSTRACT,
                 self.DB_PUBLICATION_UPDATED_LANGUAGE,
                 self.DB_PUBLICATION_UPDATED_COUNTRY,
                 self.DB_PUBLICATION_UPDATED_REPOSITORY,
                 self.DB_PUBLICATION_UPDATED_PUBLISHER,
                 self.DB_PUBLICATION_UPDATED_URL)
        query += ' VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

        record = (data_item[self.DB_PUBLICATION_UPDATED_SCOPUS_ID],
                  data_item[self.DB_PUBLICATION_UPDATED_PUBMED_ID],
                  data_item[self.DB_PUBLICATION_UPDATED_DOI],
                  data_item[self.DB_PUBLICATION_UPDATED_TITLE],
                  data_item[self.DB_PUBLICATION_UPDATED_AUTHOR],
                  data_item[self.DB_PUBLICATION_UPDATED_JOURNAL_NAME],
                  data_item[self.DB_PUBLICATION_UPDATED_PUBLICATION_DATE],
                  data_item[self.DB_PUBLICATION_UPDATED_PUBLICATION_TYPE],
                  data_item[self.DB_PUBLICATION_UPDATED_DOCUMENT_TYPE],
                  data_item[self.DB_PUBLICATION_UPDATED_CITED_BY],
                  data_item[self.DB_PUBLICATION_UPDATED_AFFILIATION],
                  data_item[self.DB_PUBLICATION_UPDATED_ABSTRACT],
                  data_item[self.DB_PUBLICATION_UPDATED_LANGUAGE],
                  data_item[self.DB_PUBLICATION_UPDATED_COUNTRY],
                  data_item[self.DB_PUBLICATION_UPDATED_REPOSITORY],
                  data_item[self.DB_PUBLICATION_UPDATED_PUBLISHER],
                  data_item[self.DB_PUBLICATION_UPDATED_URL])

        cursor.execute(query, record)

    def insert_publication_updated_transactional(self, data_list, data_log):
        msg = ''
        try:
            connection, cursor = self.connect_db()
            connection.start_transaction()

            for _, item in data_list.items():
                self.insert_publication_updated(cursor, item)

            self.update_publication_log(data_log, transactional = True, cursor = cursor)

            connection.commit()
            msg = 'Successful transaction'
        except mysql.connector.Error as error:
            msg = 'Failed to insert record to database rollback: {}'.format(error)
            connection.rollback()
        finally:
            self.close_db(connection, cursor)

        return msg

    def insert_publication_log(self, data_item):
        msg = ''
        try:
            connection, cursor = self.connect_db()

            query = 'INSERT INTO %s (%s, %s, %s, %s)' % \
                    (self.TABLE_PUBLICATION_LOG,
                     self.DB_PUBLICATION_LOG_ID,
                     self.DB_PUBLICATION_LOG_PROCESS,
                     self.DB_PUBLICATION_LOG_MESSAGE,
                     self.DB_PUBLICATION_LOG_STATUS)
            query += ' VALUES(%s, %s, %s, %s)'

            record = (data_item[self.DB_PUBLICATION_LOG_ID],
                      data_item[self.DB_PUBLICATION_LOG_PROCESS],
                      data_item[self.DB_PUBLICATION_LOG_MESSAGE],
                      data_item[self.DB_PUBLICATION_LOG_STATUS])

            cursor.execute(query, record)
            connection.commit()
            msg = 'Successful insertion'
        except mysql.connector.Error as error:
            msg = 'Failed to delete record to table: {}'.format(error)
        finally:
            self.close_db(connection, cursor)

        return msg

    def update_publication_updated(self, data_item):
        msg = ''
        try:
            connection, cursor = self.connect_db()
            connection.start_transaction()

            _doi = data_item[self.DB_PUBLICATION_UPDATED_DOI]
            if _doi:
                # With DOI
                _exist = self.select_publication_individual(cursor, doi = _doi)
                if _exist['exist'] == 1:
                    # Check update
                    _rowcount = self.update_publication_updated_individual(cursor, data_item, doi = True)
                    if _rowcount == 1:
                        msg = 'Record updated successfully: %s' % _doi
                    elif _rowcount == 0:
                        msg = None # 'Record not updated'
                else:
                    # Insert
                    self.insert_publication_updated(cursor, data_item)
                    msg = 'Record inserted successfully: %s' % _doi
            else:
                # Without DOI
                _title = data_item[self.DB_PUBLICATION_UPDATED_TITLE]
                _exist = self.select_publication_individual(cursor, title = _title)
                if _exist['exist'] == 1:
                    # Check update
                    _rowcount = self.update_publication_updated_individual(cursor, data_item, title = True)
                    if _rowcount == 1:
                        msg = 'Record updated successfully: %s' % _title
                    elif _rowcount == 0:
                        msg = None # 'Record not updated'
                else:
                    # Insert
                    self.insert_publication_updated(cursor, data_item)
                    msg = 'Record inserted successfully: %s' % _title

            connection.commit()
        except mysql.connector.Error as error:
            msg = 'Failed to update record to table rollback: {}'.format(error)
            connection.rollback()
        finally:
            self.close_db(connection, cursor)

        return msg

    def select_publication_individual(self, cursor, doi = None, title = None):
        record = None
        try:
            query = ''
            param = []
            if doi:
                query = 'SELECT COUNT(1) as exist FROM ' + self.TABLE_PUBLICATION_UPDATED + ' WHERE ' + self.DB_PUBLICATION_UPDATED_DOI + ' = %s'
                param = [doi]
            elif title:
                query = 'SELECT COUNT(1) as exist FROM ' + self.TABLE_PUBLICATION_UPDATED + ' WHERE ' + self.DB_PUBLICATION_UPDATED_TITLE + ' = %s'
                param = [title]
            
            cursor.execute(query, param)

            record = cursor.fetchone()
        except mysql.connector.Error as error:
            msg = 'Error reading data from MySQL table: {}'.format(error)

        return record

    def update_publication_updated_individual(self, cursor, data_item, doi = False, title = False):

        def update_row(cursor, column_id, column_update, data_item, flag):
            query = 'UPDATE ' + self.TABLE_PUBLICATION_UPDATED + \
                    '   SET ' + column_update + ' = %s' + \
                    ' WHERE ' + column_id + ' = %s' + \
                    '   AND ' + column_update + ' IS NULL'

            update_data = [data_item[column_update],
                           data_item[column_id]]
            cursor.execute(query, update_data)
            _rowcount = cursor.rowcount

            flag = True if _rowcount > 0 else flag

            return flag

        update_transac = False

        column_id = ''
        if doi:
            column_id = self.DB_PUBLICATION_UPDATED_DOI
        elif title:
            column_id = self.DB_PUBLICATION_UPDATED_TITLE

        # Update Cited by
        query = 'UPDATE ' + self.TABLE_PUBLICATION_UPDATED + \
                '   SET ' + self.DB_PUBLICATION_UPDATED_CITED_BY + ' = %s' + \
                ' WHERE ' + column_id + ' = %s' + \
                '   AND IFNULL(' + self.DB_PUBLICATION_UPDATED_CITED_BY + ', 0) < %s'

        update_data = [data_item[self.DB_PUBLICATION_UPDATED_CITED_BY],
                       data_item[column_id],
                       data_item[self.DB_PUBLICATION_UPDATED_CITED_BY]]
        cursor.execute(query, update_data)
        _rowcount = cursor.rowcount

        update_transac = True if _rowcount > 0 else update_transac

        # Update Abstract
        update_transac = update_row(cursor, column_id, self.DB_PUBLICATION_UPDATED_ABSTRACT, data_item, update_transac)

        # Update Document type
        update_transac = update_row(cursor, column_id, self.DB_PUBLICATION_UPDATED_DOCUMENT_TYPE, data_item, update_transac)

        # Update Date
        update_transac = update_row(cursor, column_id, self.DB_PUBLICATION_UPDATED_PUBLICATION_DATE, data_item, update_transac)

        # Update Author
        update_transac = update_row(cursor, column_id, self.DB_PUBLICATION_UPDATED_AUTHOR, data_item, update_transac)

        # Update Language
        update_transac = update_row(cursor, column_id, self.DB_PUBLICATION_UPDATED_LANGUAGE, data_item, update_transac)

        # Update Publisher
        update_transac = update_row(cursor, column_id, self.DB_PUBLICATION_UPDATED_PUBLISHER, data_item, update_transac)

        _rowcount_transac = 1 if update_transac else 0

        return _rowcount_transac

    def update_publication_log(self, data_item, transactional = False, cursor = None):
        if transactional:
            query = 'UPDATE ' + self.TABLE_PUBLICATION_LOG + \
                    '   SET ' + self.DB_PUBLICATION_LOG_STATUS + ' = %s,' + \
                          ' ' + self.DB_PUBLICATION_LOG_MESSAGE + ' = %s' + \
                    ' WHERE ' + self.DB_PUBLICATION_LOG_ID + ' = %s' + \
                    '   AND ' + self.DB_PUBLICATION_LOG_PROCESS + ' = %s'

            update_data = (data_item[self.DB_PUBLICATION_LOG_STATUS],
                           data_item[self.DB_PUBLICATION_LOG_MESSAGE],
                           data_item[self.DB_PUBLICATION_LOG_ID],
                           data_item[self.DB_PUBLICATION_LOG_PROCESS])

            cursor.execute(query, update_data)
        else:
            msg = ''
            try:
                connection, cursor = self.connect_db()

                query = 'UPDATE ' + self.TABLE_PUBLICATION_LOG + \
                        '   SET ' + self.DB_PUBLICATION_LOG_STATUS + ' = %s,' + \
                              ' ' + self.DB_PUBLICATION_LOG_MESSAGE + ' = %s' + \
                        ' WHERE ' + self.DB_PUBLICATION_LOG_ID + ' = %s' + \
                        '   AND ' + self.DB_PUBLICATION_LOG_PROCESS + ' = %s'

                update_data = (data_item[self.DB_PUBLICATION_LOG_STATUS],
                               data_item[self.DB_PUBLICATION_LOG_MESSAGE],
                               data_item[self.DB_PUBLICATION_LOG_ID],
                               data_item[self.DB_PUBLICATION_LOG_PROCESS])

                cursor.execute(query, update_data)
                connection.commit()
                msg = 'Successful update'
            except mysql.connector.Error as error:
                msg = 'Failed to update record to table: {}'.format(error)
            finally:
                self.close_db(connection, cursor)

            return msg

    def delete_publication_log(self, data_item):
        msg = ''
        try:
            connection, cursor = self.connect_db()

            query = 'DELETE FROM ' + self.TABLE_PUBLICATION_LOG + \
                    ' WHERE ' + self.DB_PUBLICATION_LOG_ID + ' = %s' + \
                    '   AND ' + self.DB_PUBLICATION_LOG_PROCESS + ' = %s'

            delete_data = (data_item[self.DB_PUBLICATION_LOG_ID],
                           data_item[self.DB_PUBLICATION_LOG_PROCESS])

            cursor.execute(query, delete_data)
            connection.commit()
            msg = 'Successful deletion'
        except mysql.connector.Error as error:
            msg = 'Failed to delete record to table: {}'.format(error)
        finally:
            self.close_db(connection, cursor)

        return msg

    def select_publication_pubmed(self):
        records = None
        try:
            connection, cursor = self.connect_db()

            query = 'SELECT ' + self.DB_PUBMED_ID + ', ' \
                              + self.DB_PUBMED_DOI + ', ' \
                              + self.DB_PUBMED_TITLE + ', ' \
                              + self.DB_PUBMED_AUTHOR + ', ' \
                              + self.DB_PUBMED_JOURNAL_NAME + ', ' \
                              + self.DB_PUBMED_PUBLICATION_DATE + ', ' \
                              + self.DB_PUBMED_COUNTRY + ', ' \
                              + self.DB_PUBMED_ABSTRACT + ', ' \
                              + self.DB_PUBMED_LANGUAGE + ', ' \
                              + self.DB_PUBMED_PUBLICATION_TYPE + \
                    '  FROM ' + self.TABLE_PUBLICATION_PUBMED

            cursor.execute(query)
            records = cursor.fetchall()
        except mysql.connector.Error as error:
            msg = 'Error reading data from MySQL table: {}'.format(error)
        finally:
            self.close_db(connection, cursor)

        return records

    def select_publication_scopus(self):
        records = None
        try:
            connection, cursor = self.connect_db()

            query = 'SELECT ' + self.DB_SCOPUS_ID + ', ' \
                              + self.DB_SCOPUS_PMID + ', ' \
                              + self.DB_SCOPUS_DOI + ', ' \
                              + self.DB_SCOPUS_TITLE + ', ' \
                              + self.DB_SCOPUS_AUTHOR + ', ' \
                              + self.DB_SCOPUS_JOURNAL_NAME + ', ' \
                              + self.DB_SCOPUS_PUBLICATION_DATE + ', ' \
                              + self.DB_SCOPUS_PUBLICATION_TYPE + ', ' \
                              + self.DB_SCOPUS_DOCUMENT_CODE + ', ' \
                              + self.DB_SCOPUS_DOCUMENT_TYPE + ', ' \
                              + self.DB_SCOPUS_CITED_BY + ', ' \
                              + self.DB_SCOPUS_OPEN_ACCESS + ', ' \
                              + self.DB_SCOPUS_AFFILIATION + \
                    '  FROM ' + self.TABLE_PUBLICATION_SCOPUS

            cursor.execute(query)
            records = cursor.fetchall()
        except mysql.connector.Error as error:
            msg = 'Error reading data from MySQL table: {}'.format(error)
        finally:
            self.close_db(connection, cursor)

        return records

    def select_publication_today(self):
        records = None
        try:
            connection, cursor = self.connect_db()

            query = 'SELECT ' + self.DB_PUBLICATION_TODAY_SCOPUS_ID + ', ' \
                              + self.DB_PUBLICATION_TODAY_PUBMED_ID + ', ' \
                              + self.DB_PUBLICATION_TODAY_DOI + ', ' \
                              + self.DB_PUBLICATION_TODAY_TITLE + ', ' \
                              + self.DB_PUBLICATION_TODAY_AUTHOR + ', ' \
                              + self.DB_PUBLICATION_TODAY_JOURNAL_NAME + ', ' \
                              + self.DB_PUBLICATION_TODAY_PUBLICATION_DATE + ', ' \
                              + self.DB_PUBLICATION_TODAY_PUBLICATION_TYPE + ', ' \
                              + self.DB_PUBLICATION_TODAY_DOCUMENT_TYPE + ', ' \
                              + self.DB_PUBLICATION_TODAY_CITED_BY + ', ' \
                              + self.DB_PUBLICATION_TODAY_AFFILIATION + ', ' \
                              + self.DB_PUBLICATION_TODAY_ABSTRACT + ', ' \
                              + self.DB_PUBLICATION_TODAY_LANGUAGE + ', ' \
                              + self.DB_PUBLICATION_TODAY_COUNTRY + ', ' \
                              + self.DB_PUBLICATION_TODAY_REPOSITORY + \
                    '  FROM ' + self.TABLE_PUBLICATION_TODAY

            cursor.execute(query)
            records = cursor.fetchall()
        except mysql.connector.Error as error:
            msg = 'Error reading data from MySQL table: {}'.format(error)
        finally:
            self.close_db(connection, cursor)

        return records

    def select_publication_log(self, data_item):
        record = None
        try:
            connection, cursor = self.connect_db()

            query = 'SELECT ' + self.DB_PUBLICATION_LOG_STATUS + ', ' \
                              + self.DB_PUBLICATION_LOG_MESSAGE + \
                    '  FROM ' + self.TABLE_PUBLICATION_LOG + \
                    ' WHERE ' + self.DB_PUBLICATION_LOG_ID + ' = %s' \
                    '   AND ' + self.DB_PUBLICATION_LOG_PROCESS + ' = %s' \

            param = [data_item[self.DB_PUBLICATION_LOG_ID],
                     data_item[self.DB_PUBLICATION_LOG_PROCESS]]

            cursor.execute(query, param)

            record = cursor.fetchone()
        except mysql.connector.Error as error:
            msg = 'Error reading data from MySQL table: {}'.format(error)
        finally:
            self.close_db(connection, cursor)

        return record

'''
if __name__ == '__main__':
    odb = Database()
    odb.test()
'''
