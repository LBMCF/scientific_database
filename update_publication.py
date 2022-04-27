#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import time
import traceback
from tqdm import tqdm
from Bio import Entrez
from Bio import Medline
from colorama import init
from crossref.restful import Works
from config_app import Config_App
from database import Database
init()
from pprint import pprint

class Publication:

    def __init__(self):
        self.ROOT = os.path.dirname(os.path.realpath(__file__))

        # Log
        self.LOG_NAME = "log_%s_%s.log" % (os.path.splitext(os.path.basename(__file__))[0], time.strftime('%Y%m%d'))
        self.LOG_FILE = None # os.path.join(self.ROOT, self.LOG_NAME)

        self.OUTPUT_PATH = os.path.join(self.ROOT, 'log_app')

        # Crossref API
        self.crossref_author = 'author'
        self.crossref_author_given = 'given'
        self.crossref_author_family = 'family'
        self.crossref_abstract = 'abstract'
        self.crossref_language = 'language'
        self.crossref_publisher = 'publisher'
        self.crossref_cited_by = 'is-referenced-by-count'
        self.crossref_type = 'type'
        self.crossref_deposited = 'deposited'
        self.crossref_deposited_date_parts = 'date-parts'

        # Get data
        self.crossref_get_abstract = 'abstract'
        self.crossref_get_document_type = 'document_type'
        self.crossref_get_cited_by = 'cited_by'
        self.crossref_get_publisher = 'publisher'
        self.crossref_get_language = 'language'
        self.crossref_get_author = 'author'
        self.crossref_get_deposited_date = 'deposited_date'

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

    def get_data_pubmed(self):
        self.show_print("Get data from table: %s" % self.odb.TABLE_PUBLICATION_PUBMED, [self.LOG_FILE], font = self.YELLOW)
        results = self.odb.select_publication_pubmed()
        _results = {}
        for index, item in enumerate(results, start = 1):
            _results.update({index: item})

        return _results

    def get_data_scopus(self):
        self.show_print("Get data from table: %s" % self.odb.TABLE_PUBLICATION_SCOPUS, [self.LOG_FILE], font = self.YELLOW)
        results = self.odb.select_publication_scopus()
        _results = {}
        for index, item in enumerate(results, start = 1):
            _results.update({index: item})

        return _results

    def join_all_data(self):

        def remove_endpoint(text):
            _text = text.strip()

            while(_text[-1] == '.'):
                _text = _text[0:len(_text) - 1]
                _text = _text.strip()

            return _text

        # Insert log
        dict_log = {self.odb.DB_PUBLICATION_LOG_ID: time.strftime('%Y%m%d'),
                    self.odb.DB_PUBLICATION_LOG_PROCESS: self.odb.DB_CONST_PROCESS_CONSENSUS,
                    self.odb.DB_PUBLICATION_LOG_MESSAGE: None,
                    self.odb.DB_PUBLICATION_LOG_STATUS: self.odb.DB_CONST_STATUS_RUNNING}
        self.odb.delete_publication_log(dict_log)
        self.odb.insert_publication_log(dict_log)

        records_scopus = self.get_data_scopus()
        records_pubmed = self.get_data_pubmed()
        self.show_print("", [self.LOG_FILE])

        records_scopus_format = {}
        for index, item in records_scopus.items():
            current = {self.odb.DB_PUBLICATION_TODAY_SCOPUS_ID: None,
                       self.odb.DB_PUBLICATION_TODAY_PUBMED_ID: None,
                       self.odb.DB_PUBLICATION_TODAY_DOI: None,
                       self.odb.DB_PUBLICATION_TODAY_TITLE: None,
                       self.odb.DB_PUBLICATION_TODAY_AUTHOR: None,
                       self.odb.DB_PUBLICATION_TODAY_JOURNAL_NAME: None,
                       self.odb.DB_PUBLICATION_TODAY_PUBLICATION_DATE: None,
                       self.odb.DB_PUBLICATION_TODAY_PUBLICATION_TYPE: None,
                       self.odb.DB_PUBLICATION_TODAY_DOCUMENT_TYPE: None,
                       self.odb.DB_PUBLICATION_TODAY_CITED_BY: None,
                       self.odb.DB_PUBLICATION_TODAY_AFFILIATION: None,
                       self.odb.DB_PUBLICATION_TODAY_ABSTRACT: None,
                       self.odb.DB_PUBLICATION_TODAY_LANGUAGE: None,
                       self.odb.DB_PUBLICATION_TODAY_COUNTRY: None,
                       self.odb.DB_PUBLICATION_TODAY_REPOSITORY: [self.odb.DB_CONST_SCOPUS]}

            current[self.odb.DB_PUBLICATION_TODAY_SCOPUS_ID] = item[self.odb.DB_SCOPUS_ID]
            current[self.odb.DB_PUBLICATION_TODAY_PUBMED_ID] = item[self.odb.DB_SCOPUS_PMID]
            current[self.odb.DB_PUBLICATION_TODAY_DOI] = item[self.odb.DB_SCOPUS_DOI]
            current[self.odb.DB_PUBLICATION_TODAY_TITLE] = item[self.odb.DB_SCOPUS_TITLE]
            current[self.odb.DB_PUBLICATION_TODAY_AUTHOR] = item[self.odb.DB_SCOPUS_AUTHOR]
            current[self.odb.DB_PUBLICATION_TODAY_JOURNAL_NAME] = item[self.odb.DB_SCOPUS_JOURNAL_NAME]
            current[self.odb.DB_PUBLICATION_TODAY_PUBLICATION_DATE] = item[self.odb.DB_SCOPUS_PUBLICATION_DATE]
            current[self.odb.DB_PUBLICATION_TODAY_PUBLICATION_TYPE] = item[self.odb.DB_SCOPUS_PUBLICATION_TYPE]
            current[self.odb.DB_PUBLICATION_TODAY_DOCUMENT_TYPE] = item[self.odb.DB_SCOPUS_DOCUMENT_TYPE]
            current[self.odb.DB_PUBLICATION_TODAY_CITED_BY] = item[self.odb.DB_SCOPUS_CITED_BY]
            current[self.odb.DB_PUBLICATION_TODAY_AFFILIATION] = item[self.odb.DB_SCOPUS_AFFILIATION]

            records_scopus_format.update({index: current})

        records_pubmed_format = {}
        for index, item in records_pubmed.items():
            current = {self.odb.DB_PUBLICATION_TODAY_SCOPUS_ID: None,
                       self.odb.DB_PUBLICATION_TODAY_PUBMED_ID: None,
                       self.odb.DB_PUBLICATION_TODAY_DOI: None,
                       self.odb.DB_PUBLICATION_TODAY_TITLE: None,
                       self.odb.DB_PUBLICATION_TODAY_AUTHOR: None,
                       self.odb.DB_PUBLICATION_TODAY_JOURNAL_NAME: None,
                       self.odb.DB_PUBLICATION_TODAY_PUBLICATION_DATE: None,
                       self.odb.DB_PUBLICATION_TODAY_PUBLICATION_TYPE: None,
                       self.odb.DB_PUBLICATION_TODAY_DOCUMENT_TYPE: None,
                       self.odb.DB_PUBLICATION_TODAY_CITED_BY: None,
                       self.odb.DB_PUBLICATION_TODAY_AFFILIATION: None,
                       self.odb.DB_PUBLICATION_TODAY_ABSTRACT: None,
                       self.odb.DB_PUBLICATION_TODAY_LANGUAGE: None,
                       self.odb.DB_PUBLICATION_TODAY_COUNTRY: None,
                       self.odb.DB_PUBLICATION_TODAY_REPOSITORY: [self.odb.DB_CONST_PUBMED]}

            current[self.odb.DB_PUBLICATION_TODAY_PUBMED_ID] = item[self.odb.DB_PUBMED_ID]
            current[self.odb.DB_PUBLICATION_TODAY_DOI] = item[self.odb.DB_PUBMED_DOI]
            current[self.odb.DB_PUBLICATION_TODAY_TITLE] = item[self.odb.DB_PUBMED_TITLE]
            current[self.odb.DB_PUBLICATION_TODAY_AUTHOR] = item[self.odb.DB_PUBMED_AUTHOR]
            current[self.odb.DB_PUBLICATION_TODAY_JOURNAL_NAME] = item[self.odb.DB_PUBMED_JOURNAL_NAME]
            current[self.odb.DB_PUBLICATION_TODAY_PUBLICATION_DATE] = item[self.odb.DB_PUBMED_PUBLICATION_DATE]
            current[self.odb.DB_PUBLICATION_TODAY_PUBLICATION_TYPE] = item[self.odb.DB_PUBMED_PUBLICATION_TYPE]
            current[self.odb.DB_PUBLICATION_TODAY_ABSTRACT] = item[self.odb.DB_PUBMED_ABSTRACT]
            current[self.odb.DB_PUBLICATION_TODAY_LANGUAGE] = item[self.odb.DB_PUBMED_LANGUAGE]
            current[self.odb.DB_PUBLICATION_TODAY_COUNTRY] = item[self.odb.DB_PUBMED_COUNTRY]

            records_pubmed_format.update({index: current})

        # Articles Without DOI / With DOI
        titles_without_doi = []
        records_without_doi = {}
        records_with_doi = {}
        index_wo = 1
        index_w = 1
        for _, item_scopus in records_scopus_format.items():
            doi = item_scopus[self.odb.DB_PUBLICATION_TODAY_DOI]
            if not doi:
                title = item_scopus[self.odb.DB_PUBLICATION_TODAY_TITLE]
                _title = remove_endpoint(title.lower())

                if _title not in titles_without_doi:
                    titles_without_doi.append(_title)

                    item_scopus[self.odb.DB_PUBLICATION_TODAY_TITLE] = remove_endpoint(title)
                    records_without_doi.update({index_wo: item_scopus})
                    index_wo += 1
            else:
                doi = doi.lower()
                if doi not in records_with_doi:
                    item_scopus[self.odb.DB_PUBLICATION_TODAY_TITLE] = remove_endpoint(item_scopus[self.odb.DB_PUBLICATION_TODAY_TITLE])
                    records_with_doi.update({doi: item_scopus})

        index_wo = len(records_without_doi) + 1
        for _, item_pubmed in records_pubmed_format.items():
            doi = item_pubmed[self.odb.DB_PUBLICATION_TODAY_DOI]
            if not doi:
                title = item_pubmed[self.odb.DB_PUBLICATION_TODAY_TITLE]
                _title = remove_endpoint(title.lower())

                if _title not in titles_without_doi:
                    titles_without_doi.append(_title)

                    item_pubmed[self.odb.DB_PUBLICATION_TODAY_TITLE] = remove_endpoint(title)
                    records_without_doi.update({index_wo: item_pubmed})
                    index_wo += 1
            else:
                doi = doi.lower()
                if doi not in records_with_doi:
                    item_pubmed[self.odb.DB_PUBLICATION_TODAY_TITLE] = remove_endpoint(item_pubmed[self.odb.DB_PUBLICATION_TODAY_TITLE])
                    records_with_doi.update({doi: item_pubmed})
                else:
                    record_updated = records_with_doi[doi].copy()
                    record_updated[self.odb.DB_PUBLICATION_TODAY_REPOSITORY].append(self.odb.DB_CONST_PUBMED)
                    record_updated[self.odb.DB_PUBLICATION_TODAY_PUBMED_ID] = item_pubmed[self.odb.DB_PUBLICATION_TODAY_PUBMED_ID]
                    record_updated[self.odb.DB_PUBLICATION_TODAY_ABSTRACT] = item_pubmed[self.odb.DB_PUBLICATION_TODAY_ABSTRACT]
                    record_updated[self.odb.DB_PUBLICATION_TODAY_AUTHOR] = item_pubmed[self.odb.DB_PUBLICATION_TODAY_AUTHOR]
                    record_updated[self.odb.DB_PUBLICATION_TODAY_COUNTRY] = item_pubmed[self.odb.DB_PUBLICATION_TODAY_COUNTRY]
                    record_updated[self.odb.DB_PUBLICATION_TODAY_LANGUAGE] = item_pubmed[self.odb.DB_PUBLICATION_TODAY_LANGUAGE]
                    records_with_doi.update({doi: record_updated})

        # pprint(records_without_doi)
        # pprint(records_with_doi)

        self.show_print("Publications with DOI: %s" % len(records_with_doi), [self.LOG_FILE], font = self.YELLOW)
        self.show_print("Publications without DOI: %s" % len(records_without_doi), [self.LOG_FILE], font = self.YELLOW)
        self.show_print("", [self.LOG_FILE])

        return records_with_doi, records_without_doi

    def insert_data_publication_today(self, dict_data1, dict_data2):
        self.show_print("Deleting data from table: %s" % self.odb.TABLE_PUBLICATION_TODAY, [self.LOG_FILE], font = self.YELLOW)
        msg = self.odb.truncate_table(self.odb.TABLE_PUBLICATION_TODAY)
        self.show_print(msg, [self.LOG_FILE], font = self.GREEN)

        self.show_print("Inserting data into table: %s" % self.odb.TABLE_PUBLICATION_TODAY, [self.LOG_FILE], font = self.YELLOW)
        msg = self.odb.insert_publication_today_transactional(dict_data1)
        msg = self.odb.insert_publication_today_transactional(dict_data2)
        self.show_print(msg, [self.LOG_FILE], font = self.GREEN)

        self.show_print("", [self.LOG_FILE])

    def get_data_publication_today(self):
        self.show_print("Get data from table: %s" % self.odb.TABLE_PUBLICATION_TODAY, [self.LOG_FILE], font = self.YELLOW)
        results = self.odb.select_publication_today()
        _results = {}
        for index, item in enumerate(results, start = 1):
            _results.update({index: item})

        self.show_print("", [self.LOG_FILE])

        return _results

    def get_language(self, code):
        hash_data = {
            'ab': 'Abkhazian',
            'aa': 'Afar',
            'af': 'Afrikaans',
            'ak': 'Akan',
            'sq': 'Albanian',
            'am': 'Amharic',
            'ar': 'Arabic',
            'an': 'Aragonese',
            'hy': 'Armenian',
            'as': 'Assamese',
            'av': 'Avaric',
            'ae': 'Avestan',
            'ay': 'Aymara',
            'az': 'Azerbaijani',
            'bm': 'Bambara',
            'ba': 'Bashkir',
            'eu': 'Basque',
            'be': 'Belarusian',
            'bn': 'Bengali',
            'bi': 'Bislama',
            'bs': 'Bosnian',
            'br': 'Breton',
            'bg': 'Bulgarian',
            'my': 'Burmese',
            'ca': 'Catalan',
            'km': 'Central Khmer',
            'ch': 'Chamorro',
            'ce': 'Chechen',
            'zh': 'Chinese',
            'cu': 'Church Slavic',
            'cv': 'Chuvash',
            'kw': 'Cornish',
            'co': 'Corsican',
            'cr': 'Cree',
            'hr': 'Croatian',
            'cs': 'Czech',
            'da': 'Danish',
            'dv': 'Divehi',
            'nl': 'Dutch',
            'dz': 'Dzongkha',
            'en': 'English',
            'eo': 'Esperanto',
            'et': 'Estonian',
            'ee': 'Ewe',
            'fo': 'Faroese',
            'fj': 'Fijian',
            'fi': 'Finnish',
            'fr': 'French',
            'ff': 'Fulah',
            'gd': 'Gaelic',
            'gl': 'Galician',
            'lg': 'Ganda',
            'ka': 'Georgian',
            'de': 'German',
            'el': 'Greek',
            'gn': 'Guarani',
            'gu': 'Gujarati',
            'ht': 'Haitian',
            'ha': 'Hausa',
            'he': 'Hebrew',
            'hz': 'Herero',
            'hi': 'Hindi',
            'ho': 'Hiri Motu',
            'hu': 'Hungarian',
            'is': 'Icelandic',
            'io': 'Ido',
            'ig': 'Igbo',
            'id': 'Indonesian',
            'ia': 'Interlingua',
            'ie': 'Interlingue',
            'iu': 'Inuktitut',
            'ik': 'Inupiaq',
            'ga': 'Irish',
            'it': 'Italian',
            'ja': 'Japanese',
            'jv': 'Javanese',
            'kl': 'Kalaallisut',
            'kn': 'Kannada',
            'kr': 'Kanuri',
            'ks': 'Kashmiri',
            'kk': 'Kazakh',
            'ki': 'Kikuyu',
            'rw': 'Kinyarwanda',
            'ky': 'Kirghiz',
            'kv': 'Komi',
            'kg': 'Kongo',
            'ko': 'Korean',
            'kj': 'Kuanyama',
            'ku': 'Kurdish',
            'lo': 'Lao',
            'la': 'Latin',
            'lv': 'Latvian',
            'li': 'Limburgan',
            'ln': 'Lingala',
            'lt': 'Lithuanian',
            'lu': 'Luba-Katanga',
            'lb': 'Luxembourgish',
            'mk': 'Macedonian',
            'mg': 'Malagasy',
            'ms': 'Malay',
            'ml': 'Malayalam',
            'mt': 'Maltese',
            'gv': 'Manx',
            'mi': 'Maori',
            'mr': 'Marathi',
            'mh': 'Marshallese',
            'mn': 'Mongolian',
            'na': 'Nauru',
            'nv': 'Navajo',
            'ng': 'Ndonga',
            'ne': 'Nepali',
            'nd': 'North Ndebele',
            'se': 'Northern Sami',
            'no': 'Norwegian',
            'nb': 'Norwegian Bokmal',
            'nn': 'Norwegian Nynorsk',
            'ny': 'Nyanja',
            'oc': 'Occitan',
            'oj': 'Ojibwa',
            'or': 'Oriya',
            'om': 'Oromo',
            'os': 'Ossetian',
            'pi': 'Pali',
            'ps': 'Pashto',
            'fa': 'Persian',
            'pl': 'Polish',
            'pt': 'Portuguese',
            'pa': 'Punjabi',
            'qu': 'Quechua',
            'ro': 'Romanian',
            'rm': 'Romansh',
            'rn': 'Rundi',
            'ru': 'Russian',
            'sm': 'Samoan',
            'sg': 'Sango',
            'sa': 'Sanskrit',
            'sc': 'Sardinian',
            'sr': 'Serbian',
            'sn': 'Shona',
            'ii': 'Sichuan Yi',
            'sd': 'Sindhi',
            'si': 'Sinhala',
            'sk': 'Slovak',
            'sl': 'Slovenian',
            'so': 'Somali',
            'nr': 'South Ndebele',
            'st': 'Southern Sotho',
            'es': 'Spanish',
            'su': 'Sundanese',
            'sw': 'Swahili',
            'ss': 'Swati',
            'sv': 'Swedish',
            'tl': 'Tagalog',
            'ty': 'Tahitian',
            'tg': 'Tajik',
            'ta': 'Tamil',
            'tt': 'Tatar',
            'te': 'Telugu',
            'th': 'Thai',
            'bo': 'Tibetan',
            'ti': 'Tigrinya',
            'to': 'Tonga',
            'ts': 'Tsonga',
            'tn': 'Tswana',
            'tr': 'Turkish',
            'tk': 'Turkmen',
            'tw': 'Twi',
            'ug': 'Uighur',
            'uk': 'Ukrainian',
            'ur': 'Urdu',
            'uz': 'Uzbek',
            've': 'Venda',
            'vi': 'Vietnamese',
            'vo': 'Volapük',
            'wa': 'Walloon',
            'cy': 'Welsh',
            'fy': 'Western Frisian',
            'wo': 'Wolof',
            'xh': 'Xhosa',
            'yi': 'Yiddish',
            'yo': 'Yoruba',
            'za': 'Zhuang',
            'zu': 'Zulu'
        }

        r = 'Unknown'
        if code in hash_data:
            r = hash_data[code]

        return r

    def get_complement(self, doi):
        '''
                 'assertion': [{'label': 'This article is maintained by',
                                'name': 'publisher',
                                'value': 'Elsevier'},
                               {'label': 'Content Type',
                                'name': 'content_type',
                                'value': 'article'},
                               {'label': 'CrossRef DOI link to publisher maintained version',
                                'name': 'articlelink',
                                'value': 'https://doi.org/10.1016/j.ecss.2022.107791'},

                 'author': [{'affiliation': [],
                             'family': 'Fernandes',
                             'given': 'Sheryl Oliveira',
                             'sequence': 'first'},
                            {'ORCID': 'http://orcid.org/0000-0001-9074-2902',
                             'affiliation': [],
                             'authenticated-orcid': False,
                             'family': 'Gonsalves',
                             'given': 'Maria Judith',
                             'sequence': 'additional'},

                 'language': 'en',
                 'publisher': 'Elsevier BV',
                 'is-referenced-by-count': 0,
                 'type': 'journal-article',

                 'deposited': {'date-parts': [[2022, 4, 23]],
                               'date-time': '2022-04-23T14:14:59Z',
                               'timestamp': 1650723299000},
                 'indexed': {'date-parts': [[2022, 4, 23]],
                             'date-time': '2022-04-23T14:41:01Z',
                             'timestamp': 1650724861856},
        '''
        complement = {}
        try:
            works = Works()
            response = works.doi(doi)
            # pprint(response)

            if response:
                # response['reference'] = ''
                # pprint(response)

                complement = {self.crossref_get_abstract: None,
                              self.crossref_get_document_type: None,
                              self.crossref_get_cited_by: None,
                              self.crossref_get_publisher: None,
                              self.crossref_get_language: None,
                              self.crossref_get_author: None,
                              self.crossref_get_deposited_date: None}

                _abstract = None
                if self.crossref_abstract in response:
                    _abstract = response[self.crossref_abstract]
                complement.update({self.crossref_get_abstract: _abstract})

                _document_type = None
                if self.crossref_type in response:
                    _document_type = response[self.crossref_type]
                complement.update({self.crossref_get_document_type: _document_type})

                _cited_by = None
                if self.crossref_cited_by in response:
                    _cited_by = response[self.crossref_cited_by]
                complement.update({self.crossref_get_cited_by: _cited_by})

                _publisher = None
                if self.crossref_publisher in response:
                    _publisher = response[self.crossref_publisher]
                complement.update({self.crossref_get_publisher: _publisher})

                _language = None
                if self.crossref_language in response:
                    _language = self.get_language(response[self.crossref_language])
                complement.update({self.crossref_get_language: _language})

                _author = None
                if self.crossref_author in response:
                    _names = []
                    for item in response[self.crossref_author]:
                        _first_name = ''
                        if self.crossref_author_given in item:
                            _first_name = item[self.crossref_author_given]
                        _last_name = ''
                        if self.crossref_author_family in item:
                            _last_name = item[self.crossref_author_family]
                        _name = (_first_name + ' ' + _last_name).strip()
                        _names.append(_name)
                    _author = ', '.join(_names)
                complement.update({self.crossref_get_author: _author})

                _deposited = None
                if self.crossref_deposited in response:
                    _date = response[self.crossref_deposited][self.crossref_deposited_date_parts]
                    _year = str(_date[0][0])
                    _month = str(_date[0][1]).zfill(2)
                    _day = str(_date[0][2])
                    _deposited = _year + _month + _day
                complement.update({self.crossref_get_deposited_date: _deposited})
        except Exception as e:
            msg = '[ERROR]: [%s] %s' % (doi, traceback.format_exc())
            raise msg

        return complement

    def get_publication_complement(self):
        data_publication = self.get_data_publication_today()

        _crossref = Config_App.APP_CONFIG['crossref']
        if _crossref:
            self.show_print("Collecting complementary data from Crossref", [self.LOG_FILE], font = self.YELLOW)

            with tqdm(total = len(data_publication)) as pbar:
                for _, publication in data_publication.items():
                    # New fields
                    publication[self.odb.DB_PUBLICATION_UPDATED_PUBLISHER] = None
                    publication[self.odb.DB_PUBLICATION_UPDATED_URL] = None

                    _doi = publication[self.odb.DB_PUBLICATION_TODAY_DOI]
                    if _doi:
                        publication[self.odb.DB_PUBLICATION_UPDATED_URL] = 'https://doi.org/%s' % _doi

                        _complement = self.get_complement(_doi)
                        if _complement:
                            # pprint(_complement)

                            _abstract = _complement[self.crossref_get_abstract]
                            if _abstract:
                                if not publication[self.odb.DB_PUBLICATION_TODAY_ABSTRACT]:
                                    publication[self.odb.DB_PUBLICATION_TODAY_ABSTRACT] = _abstract

                            _document_type = _complement[self.crossref_get_document_type]
                            if _document_type:
                                if not publication[self.odb.DB_PUBLICATION_TODAY_DOCUMENT_TYPE]:
                                    publication[self.odb.DB_PUBLICATION_TODAY_DOCUMENT_TYPE] = _document_type

                            _deposited_date = _complement[self.crossref_get_deposited_date]
                            if _deposited_date:
                                if not publication[self.odb.DB_PUBLICATION_TODAY_PUBLICATION_DATE]:
                                    publication[self.odb.DB_PUBLICATION_TODAY_PUBLICATION_DATE] = _deposited_date

                            _language = _complement[self.crossref_get_language]
                            if _language:
                                if not publication[self.odb.DB_PUBLICATION_TODAY_LANGUAGE]:
                                    publication[self.odb.DB_PUBLICATION_TODAY_LANGUAGE] = _language

                            _cited_by = _complement[self.crossref_get_cited_by]
                            if _cited_by:
                                publication[self.odb.DB_PUBLICATION_TODAY_CITED_BY] = _cited_by

                            _author = _complement[self.crossref_get_author]
                            if _author:
                                publication[self.odb.DB_PUBLICATION_TODAY_AUTHOR] = _author

                            _publisher = _complement[self.crossref_get_publisher]
                            if _publisher:
                                publication[self.odb.DB_PUBLICATION_UPDATED_PUBLISHER] = _publisher

                    pbar.update(1)
        else:
            self.show_print("Companion data collection disabled [crossref = False]", [self.LOG_FILE], font = self.YELLOW)

            for _, publication in data_publication.items():
                # New fields
                publication[self.odb.DB_PUBLICATION_UPDATED_PUBLISHER] = None
                publication[self.odb.DB_PUBLICATION_UPDATED_URL] = None

                _doi = publication[self.odb.DB_PUBLICATION_TODAY_DOI]
                if _doi:
                    publication[self.odb.DB_PUBLICATION_UPDATED_URL] = 'https://doi.org/%s' % _doi

        self.show_print("", [self.LOG_FILE])

        return data_publication

    def insert_data_publication_updated(self, dict_data):
        self.show_print("Deleting data from table: %s" % self.odb.TABLE_PUBLICATION_UPDATED, [self.LOG_FILE], font = self.YELLOW)
        msg = self.odb.truncate_table(self.odb.TABLE_PUBLICATION_UPDATED)
        self.show_print(msg, [self.LOG_FILE], font = self.GREEN)

        self.show_print("Inserting data into table: %s" % self.odb.TABLE_PUBLICATION_UPDATED, [self.LOG_FILE], font = self.YELLOW)

        dict_log = {self.odb.DB_PUBLICATION_LOG_ID: time.strftime('%Y%m%d'),
                    self.odb.DB_PUBLICATION_LOG_PROCESS: self.odb.DB_CONST_PROCESS_CONSENSUS,
                    self.odb.DB_PUBLICATION_LOG_MESSAGE: None,
                    self.odb.DB_PUBLICATION_LOG_STATUS: self.odb.DB_CONST_STATUS_OK}
        msg = self.odb.insert_publication_updated_transactional(dict_data, dict_log)
        self.show_print(msg, [self.LOG_FILE], font = self.GREEN)

        self.show_print("", [self.LOG_FILE])

    def update_data_publication_updated(self, dict_data):
        self.show_print("Updating data from table: %s" % self.odb.TABLE_PUBLICATION_UPDATED, [self.LOG_FILE], font = self.YELLOW)

        flag_update = False
        for index, item in dict_data.items():
            msg = self.odb.update_publication_updated(item)
            if msg:
                self.show_print(msg, [self.LOG_FILE], font = self.GREEN)
                flag_update = True

        # Update log
        dict_log = {self.odb.DB_PUBLICATION_LOG_ID: time.strftime('%Y%m%d'),
                    self.odb.DB_PUBLICATION_LOG_PROCESS: self.odb.DB_CONST_PROCESS_CONSENSUS,
                    self.odb.DB_PUBLICATION_LOG_MESSAGE: None,
                    self.odb.DB_PUBLICATION_LOG_STATUS: self.odb.DB_CONST_STATUS_OK}
        self.odb.update_publication_log(dict_log)

        if not flag_update:
            self.show_print("Updated database. Nothing to be updated", [self.LOG_FILE], font = self.GREEN)

        self.show_print("", [self.LOG_FILE])

def main(args):
    try:
        start = opublication.start_time()
        opublication.create_directory(opublication.OUTPUT_PATH)
        opublication.LOG_FILE = os.path.join(opublication.OUTPUT_PATH, opublication.LOG_NAME)

        opublication.show_print("###########################################################", [opublication.LOG_FILE], font = opublication.BIGREEN)
        opublication.show_print("########################### RUN ###########################", [opublication.LOG_FILE], font = opublication.BIGREEN)
        opublication.show_print("###########################################################", [opublication.LOG_FILE], font = opublication.BIGREEN)

        first_time = Config_App.APP_CONFIG['first_time']

        records_with_doi, records_without_doi = opublication.join_all_data()
        opublication.insert_data_publication_today(records_with_doi, records_without_doi)

        records_updated = opublication.get_publication_complement()
        if first_time:
            opublication.insert_data_publication_updated(records_updated)
        else:
            opublication.update_data_publication_updated(records_updated)

        opublication.show_print(opublication.finish_time(start, "Elapsed time"), [opublication.LOG_FILE])
        opublication.show_print("Done.", [opublication.LOG_FILE])
    except Exception as e:
        warning_message = '[ERROR]: %s' % traceback.format_exc()

        # Update log failed
        dict_log = {opublication.odb.DB_PUBLICATION_LOG_ID: time.strftime('%Y%m%d'),
                    opublication.odb.DB_PUBLICATION_LOG_PROCESS: opublication.odb.DB_CONST_PROCESS_CONSENSUS,
                    opublication.odb.DB_PUBLICATION_LOG_MESSAGE: warning_message,
                    opublication.odb.DB_PUBLICATION_LOG_STATUS: opublication.odb.DB_CONST_STATUS_FAILED}
        opublication.odb.update_publication_log(dict_log)

        opublication.show_print("\n%s" % traceback.format_exc(), [opublication.LOG_FILE], font = opublication.RED)
        opublication.show_print(opublication.finish_time(start, "Elapsed time"), [opublication.LOG_FILE])
        opublication.show_print("Done.", [opublication.LOG_FILE])

if __name__ == '__main__':
    opublication = Publication()
    main(sys.argv)
