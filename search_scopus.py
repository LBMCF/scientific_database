#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import json
import time
import requests
import traceback
from tqdm import tqdm
from urllib.parse import quote
from requests.exceptions import Timeout
from colorama import init
init()
from pprint import pprint
from config_app import Config_App
from database import Database

class Scopus:

    def __init__(self):
        self.ROOT = os.path.dirname(os.path.realpath(__file__))

        # Log
        self.LOG_NAME = "log_%s_%s.log" % (os.path.splitext(os.path.basename(__file__))[0], time.strftime('%Y%m%d'))
        self.LOG_FILE = None # os.path.join(self.ROOT, self.LOG_NAME)

        self.OUTPUT_PATH = os.path.join(self.ROOT, 'log_app')

        # Scopus
        self.API_KEY = '2d6a947f171edb59b9c211db7430d8a4'
        self.APPLICATION_TYPE = 'application/json'
        self.REQUEST_COUNT = 25
        self.REQUEST_START = 0
        self.TIMEOUT = 60 # 120: 2 min
        self.BASE_SCOPUS_URL = 'https://api.elsevier.com/content/search/scopus?query=<QUERY>&httpAccept=<APPLICATION_TYPE>&count=<COUNT>&start=<START>&apiKey=<API_KEY>'

        # Fields
        self.SCOPUS_RESULTS = 'search-results'
        self.SCOPUS_TOTAL_RESULTS = 'opensearch:totalResults'
        self.SCOPUS_ITEMS = 'entry'
        self.SCOPUS_AFFILIATION = 'affiliation'

        self.SCOPUS_IDENTIFIER = 'dc:identifier'
        self.SCOPUS_PUBMED_ID = 'pubmed-id'
        self.SCOPUS_DOI = 'prism:doi'
        self.SCOPUS_TITLE = 'dc:title'
        self.SCOPUS_CREATOR = 'dc:creator'
        self.SCOPUS_PUBLICATION_NAME = 'prism:publicationName'
        self.SCOPUS_OPEN_ACCESS = 'openaccess'
        self.SCOPUS_CITED_BY = 'citedby-count'
        self.SCOPUS_AGGREGATION_TYPE = 'prism:aggregationType'
        self.SCOPUS_SUB_TYPE = 'subtype'
        self.SCOPUS_SUB_TYPE_DESC = 'subtypeDescription'
        self.SCOPUS_COVER_DATE = 'prism:coverDate'
        self.SCOPUS_AFFILIATION_COUNTRY = 'affiliation-country'
        self.SCOPUS_AFFILIATION_NAME = 'affilname'

        # Keys
        self.KEY_SCOPUS_AFFILIATION_COUNTRY = 'affiliation-country'
        self.KEY_SCOPUS_AFFILIATION_NAME = 'affiliation-name'

        self.KEY_OUTPUTMESSAGE = 'OutputMessage'

        self.STATUS_TIMEOUT = 'Timeout'

        # Fonts
        self.RED = '\033[31m'
        self.YELLOW = '\033[33m'
        self.GREEN = '\033[32m'
        self.BIRED = '\033[1;91m'
        self.BIGREEN = '\033[1;92m'
        self.END = '\033[0m'

        self.odb = Database()

    def show_print(self, message, logs = None, showdate = True, font = None):
        msg_print = message
        msg_write = message

        if font is not None:
            msg_print = "%s%s%s" % (font, msg_print, self.END)

        if showdate is True:
            _time = time.strftime('%Y-%m-%d %H:%M:%S')
            msg_print = "%s %s" % (_time, msg_print)
            msg_write = "%s %s" % (_time, message)

        print(msg_print)
        if logs is not None:
            for log in logs:
                if log is not None:
                    with open(log, 'a', encoding = 'utf-8') as f:
                        f.write("%s\n" % msg_write)
                        f.close()

    def start_time(self):
        return time.time()

    def finish_time(self, start, message = None):
        finish = time.time()
        runtime = time.strftime("%H:%M:%S", time.gmtime(finish - start))
        if message is None:
            return runtime
        else:
            return "%s: %s" % (message, runtime)

    def create_directory(self, path):
        output = True
        try:
            if len(path) > 0 and not os.path.exists(path):
                os.makedirs(path)
        except Exception as e:
            output = False
        return output

    def get_scopus_search(self, query):

        def get_attribute(data, field):
            _text = ''
            if field in data:
                _text = data[field]
            return _text

        self.show_print("Search for scientific publications in Scopus", [self.LOG_FILE], font = self.YELLOW)
        self.show_print("  Query: %s" % query, [self.LOG_FILE])
        self.show_print("  Block size: %s" % self.REQUEST_COUNT, [self.LOG_FILE])
        self.show_print("", [self.LOG_FILE])

        requestURL = self.BASE_SCOPUS_URL.replace('<QUERY>', quote(query))
        requestURL = requestURL.replace('<API_KEY>', self.API_KEY)
        requestURL = requestURL.replace('<APPLICATION_TYPE>', quote(self.APPLICATION_TYPE))
        requestURL = requestURL.replace('<COUNT>', str(self.REQUEST_COUNT))
        requestURL = requestURL.replace('<START>', str(self.REQUEST_START))
        # print(requestURL)

        dict_publications = {}
        dict_responses = {}
        try:
            # Insert log
            dict_log = {self.odb.DB_PUBLICATION_LOG_ID: time.strftime('%Y%m%d'),
                        self.odb.DB_PUBLICATION_LOG_PROCESS: self.odb.DB_CONST_PROCESS_SCOPUS,
                        self.odb.DB_PUBLICATION_LOG_MESSAGE: None,
                        self.odb.DB_PUBLICATION_LOG_STATUS: self.odb.DB_CONST_STATUS_RUNNING}
            self.odb.delete_publication_log(dict_log)
            self.odb.insert_publication_log(dict_log)

            # First search
            response = requests.get(requestURL, headers = {"Accept": "application/json"}, timeout = self.TIMEOUT)
            # print(response.ok)

            if response.ok:
                try:
                    data = json.loads(response.text)
                    data_results = data[self.SCOPUS_RESULTS]
                    totalResults = data_results[self.SCOPUS_TOTAL_RESULTS]
                    # print(totalResults)
                    self.show_print("Number of scientific publications found: %s" % totalResults, [self.LOG_FILE], font = self.YELLOW)
                    self.show_print("", [self.LOG_FILE])

                    if int(totalResults) < self.REQUEST_COUNT:
                        # First (One)
                        dict_responses.update({0: response})
                    else:
                        # First
                        dict_responses.update({0: response})

                        # Iteration
                        groups = int(totalResults)/self.REQUEST_COUNT
                        if not groups.is_integer():
                            groups += 1
                        groups = int(groups)

                        self.show_print("Extracting metadata by blocks (n = %s)" % groups, [self.LOG_FILE])

                        with tqdm(total = groups) as pbar:
                            # First
                            pbar.update(1)

                            # Iteration
                            for group in range(1, groups):
                                try:
                                    requestURL = self.BASE_SCOPUS_URL.replace('<QUERY>', quote(query))
                                    requestURL = requestURL.replace('<API_KEY>', self.API_KEY)
                                    requestURL = requestURL.replace('<APPLICATION_TYPE>', quote(self.APPLICATION_TYPE))
                                    requestURL = requestURL.replace('<COUNT>', str(self.REQUEST_COUNT))
                                    requestURL = requestURL.replace('<START>', str(group * self.REQUEST_COUNT))
                                    # print(requestURL)

                                    response = requests.get(requestURL, headers = {"Accept": "application/json"}, timeout = self.TIMEOUT)
                                    # print(response.ok)

                                    if response.ok:
                                        try:
                                            dict_responses.update({group: response})
                                        except Exception as e:
                                            dict_responses.update({group: None})
                                except Timeout:
                                    dict_responses.update({group: self.STATUS_TIMEOUT})

                                pbar.update(1)
                    # pprint(dict_responses)

                    for index_i, _response in dict_responses.items():
                        _data = json.loads(_response.text)
                        _data_results = _data[self.SCOPUS_RESULTS]
                        # _totalResults = _data_results[self.SCOPUS_TOTAL_RESULTS]

                        _publications = _data_results[self.SCOPUS_ITEMS]
                        # pprint(_publications)

                        for index_j, item in enumerate(_publications, start = 1):
                            # pprint(item)

                            dict_affiliation = {}
                            if self.SCOPUS_AFFILIATION in item:
                                for i, affil in enumerate(item[self.SCOPUS_AFFILIATION]):
                                    current_aff = {}
                                    current_aff.update({self.KEY_SCOPUS_AFFILIATION_COUNTRY: affil[self.SCOPUS_AFFILIATION_COUNTRY],
                                                        self.KEY_SCOPUS_AFFILIATION_NAME: affil[self.SCOPUS_AFFILIATION_NAME]})
                                    dict_affiliation.update({i: current_aff})

                            arr_affiliation = []
                            for _, item_aff in dict_affiliation.items():
                                _affiliation_name = item_aff[self.KEY_SCOPUS_AFFILIATION_NAME]
                                _affiliation_country = item_aff[self.KEY_SCOPUS_AFFILIATION_COUNTRY]

                                _affiliation = ''
                                if _affiliation_name and not _affiliation_country:
                                    _affiliation = _affiliation_name
                                elif not _affiliation_name and _affiliation_country:
                                    _affiliation = _affiliation_country
                                elif _affiliation_name and _affiliation_country:
                                    _affiliation = _affiliation_name + '|' + _affiliation_country

                                if _affiliation not in arr_affiliation:
                                    arr_affiliation.append(_affiliation)
                            # print(arr_affiliation)

                            _identifier = get_attribute(item, self.SCOPUS_IDENTIFIER)
                            _pubmed_id = get_attribute(item, self.SCOPUS_PUBMED_ID)
                            _doi = get_attribute(item, self.SCOPUS_DOI)
                            _title = get_attribute(item, self.SCOPUS_TITLE)
                            _author = get_attribute(item, self.SCOPUS_CREATOR)
                            _journal_name = get_attribute(item, self.SCOPUS_PUBLICATION_NAME)
                            _open_access = get_attribute(item, self.SCOPUS_OPEN_ACCESS)
                            _cited_by = get_attribute(item, self.SCOPUS_CITED_BY)
                            _publication_type = get_attribute(item, self.SCOPUS_AGGREGATION_TYPE)
                            _sub_type = get_attribute(item, self.SCOPUS_SUB_TYPE)
                            _sub_type_desc = get_attribute(item, self.SCOPUS_SUB_TYPE_DESC)
                            _date = get_attribute(item, self.SCOPUS_COVER_DATE)
                            _date = _date.replace('-', '') if _date else _date

                            current = {}
                            current.update({self.odb.DB_SCOPUS_ID: _identifier,
                                            self.odb.DB_SCOPUS_PMID: _pubmed_id,
                                            self.odb.DB_SCOPUS_DOI: _doi,
                                            self.odb.DB_SCOPUS_TITLE: _title,
                                            self.odb.DB_SCOPUS_AUTHOR: _author,
                                            self.odb.DB_SCOPUS_JOURNAL_NAME: _journal_name,
                                            self.odb.DB_SCOPUS_OPEN_ACCESS: _open_access,
                                            self.odb.DB_SCOPUS_CITED_BY: _cited_by,
                                            self.odb.DB_SCOPUS_PUBLICATION_TYPE: _publication_type,
                                            self.odb.DB_SCOPUS_DOCUMENT_CODE: _sub_type,
                                            self.odb.DB_SCOPUS_DOCUMENT_TYPE: _sub_type_desc,
                                            self.odb.DB_SCOPUS_PUBLICATION_DATE: _date,
                                            self.odb.DB_SCOPUS_AFFILIATION: ', '.join(arr_affiliation)})

                            dict_publications.update({(index_i * self.REQUEST_COUNT) + index_j: current})

                    self.show_print("", [self.LOG_FILE])
                    self.show_print("Number of retrieved scientific publications: %s" % len(dict_publications), [self.LOG_FILE], font = self.YELLOW)
                    self.show_print("", [self.LOG_FILE])
                except Exception as e:
                    warning_message = '[ERROR]: %s' % traceback.format_exc()
                    dict_publications.update({self.KEY_OUTPUTMESSAGE: warning_message})

                    # Update log failed
                    dict_log = {self.odb.DB_PUBLICATION_LOG_ID: time.strftime('%Y%m%d'),
                                self.odb.DB_PUBLICATION_LOG_PROCESS: self.odb.DB_CONST_PROCESS_SCOPUS,
                                self.odb.DB_PUBLICATION_LOG_MESSAGE: warning_message,
                                self.odb.DB_PUBLICATION_LOG_STATUS: self.odb.DB_CONST_STATUS_FAILED}
                    self.odb.update_publication_log(dict_log)
        except Timeout:
            warning_message = '[ERROR]: [Timeout]: %s' % traceback.format_exc()
            dict_publications.update({self.KEY_OUTPUTMESSAGE: warning_message})         

            # Update log failed
            dict_log = {self.odb.DB_PUBLICATION_LOG_ID: time.strftime('%Y%m%d'),
                        self.odb.DB_PUBLICATION_LOG_PROCESS: self.odb.DB_CONST_PROCESS_SCOPUS,
                        self.odb.DB_PUBLICATION_LOG_MESSAGE: warning_message,
                        self.odb.DB_PUBLICATION_LOG_STATUS: self.odb.DB_CONST_STATUS_FAILED}
            self.odb.update_publication_log(dict_log)

        return dict_publications

    def insert_data_scopus(self, dict_data):
        self.show_print("Deleting data from table: %s" % self.odb.TABLE_PUBLICATION_SCOPUS, [self.LOG_FILE], font = self.YELLOW)
        msg = self.odb.truncate_table(self.odb.TABLE_PUBLICATION_SCOPUS)
        self.show_print(msg, [self.LOG_FILE], font = self.GREEN)

        self.show_print("Inserting data into table: %s" % self.odb.TABLE_PUBLICATION_SCOPUS, [self.LOG_FILE], font = self.YELLOW)

        dict_log = {self.odb.DB_PUBLICATION_LOG_ID: time.strftime('%Y%m%d'),
                    self.odb.DB_PUBLICATION_LOG_PROCESS: self.odb.DB_CONST_PROCESS_SCOPUS,
                    self.odb.DB_PUBLICATION_LOG_MESSAGE: None,
                    self.odb.DB_PUBLICATION_LOG_STATUS: self.odb.DB_CONST_STATUS_OK}
        msg = self.odb.insert_publication_scopus_transactional(dict_data, dict_log)
        self.show_print(msg, [self.LOG_FILE], font = self.GREEN)

        self.show_print("", [self.LOG_FILE])

def main(args):
    try:
        start = opaper.start_time()
        opaper.create_directory(opaper.OUTPUT_PATH)
        opaper.LOG_FILE = os.path.join(opaper.OUTPUT_PATH, opaper.LOG_NAME)

        opaper.show_print("###########################################################", [opaper.LOG_FILE], font = opaper.BIGREEN)
        opaper.show_print("########################### RUN ###########################", [opaper.LOG_FILE], font = opaper.BIGREEN)
        opaper.show_print("###########################################################", [opaper.LOG_FILE], font = opaper.BIGREEN)

        query = Config_App.APP_CONFIG['query_scopus']

        data = opaper.get_scopus_search(query)
        if opaper.KEY_OUTPUTMESSAGE not in data:
            opaper.insert_data_scopus(data)
        else:
            opaper.show_print("Failed: %s" % data[opaper.KEY_OUTPUTMESSAGE], [opaper.LOG_FILE], font = opaper.RED)
            opaper.show_print("", [opaper.LOG_FILE])

        opaper.show_print(opaper.finish_time(start, "Elapsed time"), [opaper.LOG_FILE])
        opaper.show_print("Done.", [opaper.LOG_FILE])
    except Exception as e:
        warning_message = '[ERROR]: %s' % traceback.format_exc()

        # Update log failed
        dict_log = {opaper.odb.DB_PUBLICATION_LOG_ID: time.strftime('%Y%m%d'),
                    opaper.odb.DB_PUBLICATION_LOG_PROCESS: opaper.odb.DB_CONST_PROCESS_SCOPUS,
                    opaper.odb.DB_PUBLICATION_LOG_MESSAGE: warning_message,
                    opaper.odb.DB_PUBLICATION_LOG_STATUS: opaper.odb.DB_CONST_STATUS_FAILED}
        opaper.odb.update_publication_log(dict_log)

        opaper.show_print("\n%s" % traceback.format_exc(), [opaper.LOG_FILE], font = opaper.RED)
        opaper.show_print(opaper.finish_time(start, "Elapsed time"), [opaper.LOG_FILE])
        opaper.show_print("Done.", [opaper.LOG_FILE])

if __name__ == '__main__':
    opaper = Scopus()
    main(sys.argv)
