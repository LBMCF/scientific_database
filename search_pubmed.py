#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import time
import traceback
from Bio import Entrez
from Bio import Medline
from colorama import init
from config_app import Config_App
from database import Database
init()

class Pubmed:

    def __init__(self):
        self.ROOT = os.path.dirname(os.path.realpath(__file__))

        # Log
        self.LOG_NAME = "log_%s_%s.log" % (os.path.splitext(os.path.basename(__file__))[0], time.strftime('%Y%m%d'))
        self.LOG_FILE = None # os.path.join(self.ROOT, self.LOG_NAME)

        self.OUTPUT_PATH = os.path.join(self.ROOT, 'log_app')

        # Entrez
        self.EMAIL = 'youremail@domain.com'
        self.DATABASE = 'pubmed'
        self.API_KEY = Config_App.APP_CONFIG['api_key_pubmed']

        # Fields
        self.PUBMED_PUBMED_ID = 'PMID'
        self.PUBMED_DOI = 'AID'
        self.PUBMED_TITLE = 'TI'
        self.PUBMED_AUTHOR = 'AU'
        self.PUBMED_PUBLICATION_NAME = 'TA'
        self.PUBMED_DATE_PUBLICATION = 'DP'
        self.PUBMED_COUNTRY = 'PL'
        self.PUBMED_ABSTRACT = 'AB'
        self.PUBMED_LANGUAGE = 'LA'
        self.PUBMED_PUBLICATION_TYPE = 'PT'

        self.KEY_OUTPUTMESSAGE = 'OutputMessage'

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

    def get_language(self, code):
        # https://www.nlm.nih.gov/bsd/language_table.html
        hash_data = {
            'afr': 'Afrikaans',
            'alb': 'Albanian',
            'amh': 'Amharic',
            'ara': 'Arabic',
            'arm': 'Armenian',
            'aze': 'Azerbaijani',
            'ben': 'Bengali',
            'bos': 'Bosnian',
            'bul': 'Bulgarian',
            'cat': 'Catalan',
            'chi': 'Chinese',
            'cze': 'Czech',
            'dan': 'Danish',
            'dut': 'Dutch',
            'eng': 'English',
            'epo': 'Esperanto',
            'est': 'Estonian',
            'fin': 'Finnish',
            'fre': 'French',
            'geo': 'Georgian',
            'ger': 'German',
            'gla': 'Scottish Gaelic',
            'gre': 'Greek, Modern',
            'heb': 'Hebrew',
            'hin': 'Hindi',
            'hrv': 'Croatian',
            'hun': 'Hungarian',
            'ice': 'Icelandic',
            'ind': 'Indonesian',
            'ita': 'Italian',
            'jpn': 'Japanese',
            'kin': 'Kinyarwanda',
            'kor': 'Korean',
            'lat': 'Latin',
            'lav': 'Latvian',
            'lit': 'Lithuanian',
            'mac': 'Macedonian',
            'mal': 'Malayalam',
            'mao': 'Maori',
            'may': 'Malay',
            'mul': 'Multiple languages',
            'nor': 'Norwegian',
            'per': 'Persian, Iranian',
            'pol': 'Polish',
            'por': 'Portuguese',
            'pus': 'Pushto',
            'rum': 'Romanian, Rumanian, Moldovan',
            'rus': 'Russian',
            'san': 'Sanskrit',
            'slo': 'Slovak',
            'slv': 'Slovenian',
            'spa': 'Spanish',
            'srp': 'Serbian',
            'swe': 'Swedish',
            'tha': 'Thai',
            'tur': 'Turkish',
            'ukr': 'Ukrainian',
            'und': 'Undetermined',
            'urd': 'Urdu',
            'vie': 'Vietnamese',
            'wel': 'Welsh'
        }

        r = 'Unknown'
        if code in hash_data:
            r = hash_data[code]

        return r

    def get_pubmed_search(self, query):
        # https://eutils.ncbi.nlm.nih.gov/gquery?term=biopython&retmode=xml

        self.show_print("Search for scientific publications in PubMed", [self.LOG_FILE], font = self.YELLOW)
        self.show_print("  Query: %s" % query, [self.LOG_FILE])
        self.show_print("", [self.LOG_FILE])

        dict_publications = {}
        try:
            # Insert log
            dict_log = {self.odb.DB_PUBLICATION_LOG_ID: time.strftime('%Y%m%d'),
                        self.odb.DB_PUBLICATION_LOG_PROCESS: self.odb.DB_CONST_PROCESS_PUBMED,
                        self.odb.DB_PUBLICATION_LOG_MESSAGE: None,
                        self.odb.DB_PUBLICATION_LOG_STATUS: self.odb.DB_CONST_STATUS_RUNNING}
            self.odb.delete_publication_log(dict_log)
            self.odb.insert_publication_log(dict_log)

            # Get total results
            Entrez.email = self.EMAIL
            Entrez.api_key = self.API_KEY

            handle = Entrez.egquery(term = query)
            record = Entrez.read(handle)
            handle.close()

            totalResults = 0
            for row in record['eGQueryResult']:
                if row['DbName'] == self.DATABASE:
                    totalResults = int(row['Count'])
                    break
            # print(totalResults)

            self.show_print("Number of scientific publications found: %s" % totalResults, [self.LOG_FILE], font = self.YELLOW)
            self.show_print("", [self.LOG_FILE])

            if totalResults > 0:
                # Get the PubMed IDs
                handle = Entrez.esearch(db = self.DATABASE, term = query, retmax = totalResults)
                record = Entrez.read(handle)
                handle.close()
                idlist = record["IdList"]
                # print(idlist)
                # print(len(idlist))

                self.show_print("Obtaining details of scientific publications from MEDLINE", [self.LOG_FILE], font = self.YELLOW)

                # Get the detail of scientific publications
                handle = Entrez.efetch(db = self.DATABASE, id = idlist, rettype = 'medline', retmode = 'text', retmax = len(idlist))
                records = Medline.parse(handle)
                #self.show_print("[3]", [self.LOG_FILE])
                records = list(records)
                #self.show_print("[4]", [self.LOG_FILE])
                handle.close()
                self.show_print("", [self.LOG_FILE])

                self.show_print("Number of retrieved scientific publications: %s" % len(records), [self.LOG_FILE], font = self.YELLOW)

                # print(records)
                # print(len(records))

                for index, record in enumerate(records, start = 1):
                    #print(index)
                    current = {}
                    _pubmed_id = record.get(self.PUBMED_PUBMED_ID, '')
                    _arr_doi = record.get(self.PUBMED_DOI, '')
                    _title = record.get(self.PUBMED_TITLE, '')
                    _author = record.get(self.PUBMED_AUTHOR, '')
                    _journal_name = record.get(self.PUBMED_PUBLICATION_NAME, '')
                    _publication_type = record.get(self.PUBMED_PUBLICATION_TYPE, '')
                    _abstract = record.get(self.PUBMED_ABSTRACT, '')

                    _language_raw = record.get(self.PUBMED_LANGUAGE, '')
                    _language = []
                    for code in _language_raw:
                        _language.append(self.get_language(code))

                    _country = record.get(self.PUBMED_COUNTRY, '')
                    _date = record.get(self.PUBMED_DATE_PUBLICATION, '')

                    _date = _date.split()
                    _date_format = ''
                    if _date:
                        _year = ''
                        _month = '12'
                        _day = '01'
                        flag_m = False
                        for part in _date:
                            if len(part) == 4:
                                _year = part
                            elif len(part) == 3:
                                _month = ''
                                if part in ['January', 'Jan']:
                                    _month = '01'
                                    _day = '31'
                                elif part in ['February', 'Feb']:
                                    _month = '02'
                                    _day = '28'
                                elif part in ['March', 'Mar']:
                                    _month = '03'
                                    _day = '31'
                                elif part in ['April', 'Apr']:
                                    _month = '04'
                                    _day = '30'
                                elif part in ['May']:
                                    _month = '05'
                                    _day = '31'
                                elif part in ['June', 'Jun']:
                                    _month = '06'
                                    _day = '30'
                                elif part in ['July', 'Jul']:
                                    _month = '07'
                                    _day = '31'
                                elif part in ['August', 'Aug']:
                                    _month = '08'
                                    _day = '31'
                                elif part in ['September', 'Sep']:
                                    _month = '09'
                                    _day = '30'
                                elif part in ['October', 'Oct']:
                                    _month = '10'
                                    _day = '31'
                                elif part in ['November', 'Nov']:
                                    _month = '11'
                                    _day = '30'
                                elif part in ['December', 'Dec']:
                                    _month = '12'
                                    _day = '31'
                                flag_m = True
                            elif len(part) == 2:
                                _day = part

                        if not flag_m:
                            _day = '31'

                        _date_format = _year + _month + _day

                    _doi = ''
                    if _arr_doi:
                        for i in _arr_doi:
                            if '[doi]' in i:
                                _doi = i.replace('[doi]', '').strip()
                                break

                    current.update({self.odb.DB_PUBMED_ID: _pubmed_id,
                                    self.odb.DB_PUBMED_DOI: _doi,
                                    self.odb.DB_PUBMED_TITLE: _title,
                                    self.odb.DB_PUBMED_AUTHOR: ', '.join(_author),
                                    self.odb.DB_PUBMED_JOURNAL_NAME: _journal_name,
                                    self.odb.DB_PUBMED_PUBLICATION_TYPE: ', '.join(_publication_type),
                                    self.odb.DB_PUBMED_ABSTRACT: _abstract,
                                    self.odb.DB_PUBMED_LANGUAGE: ', '.join(_language),
                                    self.odb.DB_PUBMED_COUNTRY: _country,
                                    self.odb.DB_PUBMED_PUBLICATION_DATE: _date_format})
                    dict_publications.update({index: current})

                self.show_print("", [self.LOG_FILE])
        except Exception as e:
            warning_message = '[ERROR]: %s' % traceback.format_exc()
            dict_publications.update({self.KEY_OUTPUTMESSAGE: warning_message})

            # Update log failed
            dict_log = {self.odb.DB_PUBLICATION_LOG_ID: time.strftime('%Y%m%d'),
                        self.odb.DB_PUBLICATION_LOG_PROCESS: self.odb.DB_CONST_PROCESS_PUBMED,
                        self.odb.DB_PUBLICATION_LOG_MESSAGE: warning_message,
                        self.odb.DB_PUBLICATION_LOG_STATUS: self.odb.DB_CONST_STATUS_FAILED}
            self.odb.update_publication_log(dict_log)

        return dict_publications

    def insert_data_pubmed(self, dict_data):
        self.show_print("Deleting data from table: %s" % self.odb.TABLE_PUBLICATION_PUBMED, [self.LOG_FILE], font = self.YELLOW)
        msg = self.odb.truncate_table(self.odb.TABLE_PUBLICATION_PUBMED)
        self.show_print(msg, [self.LOG_FILE], font = self.GREEN)

        self.show_print("Inserting data into table: %s" % self.odb.TABLE_PUBLICATION_PUBMED, [self.LOG_FILE], font = self.YELLOW)

        dict_log = {self.odb.DB_PUBLICATION_LOG_ID: time.strftime('%Y%m%d'),
                    self.odb.DB_PUBLICATION_LOG_PROCESS: self.odb.DB_CONST_PROCESS_PUBMED,
                    self.odb.DB_PUBLICATION_LOG_MESSAGE: None,
                    self.odb.DB_PUBLICATION_LOG_STATUS: self.odb.DB_CONST_STATUS_OK}
        msg = self.odb.insert_publication_pubmed_transactional(dict_data, dict_log)
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

        query = Config_App.APP_CONFIG['query_pubmed']

        data = opaper.get_pubmed_search(query)
        if opaper.KEY_OUTPUTMESSAGE not in data:
            opaper.insert_data_pubmed(data)
        else:
            opaper.show_print("Failed: %s" % data[opaper.KEY_OUTPUTMESSAGE], [opaper.LOG_FILE], font = opaper.RED)
            opaper.show_print("", [opaper.LOG_FILE])

        opaper.show_print(opaper.finish_time(start, "Elapsed time"), [opaper.LOG_FILE])
        opaper.show_print("Done.", [opaper.LOG_FILE])
    except Exception as e:
        warning_message = '[ERROR]: %s' % traceback.format_exc()

        # Update log failed
        dict_log = {opaper.odb.DB_PUBLICATION_LOG_ID: time.strftime('%Y%m%d'),
                    opaper.odb.DB_PUBLICATION_LOG_PROCESS: opaper.odb.DB_CONST_PROCESS_PUBMED,
                    opaper.odb.DB_PUBLICATION_LOG_MESSAGE: warning_message,
                    opaper.odb.DB_PUBLICATION_LOG_STATUS: opaper.odb.DB_CONST_STATUS_FAILED}
        opaper.odb.update_publication_log(dict_log)

        opaper.show_print("\n%s" % traceback.format_exc(), [opaper.LOG_FILE], font = opaper.RED)
        opaper.show_print(opaper.finish_time(start, "Elapsed time"), [opaper.LOG_FILE])
        opaper.show_print("Done.", [opaper.LOG_FILE])

if __name__ == '__main__':
    opaper = Pubmed()
    main(sys.argv)
