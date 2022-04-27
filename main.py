#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import time
from config_app import Config_App
from database import Database

class Principal:

    def __init__(self):
        self.odb = Database()

    def start_time(self):
        return time.time()

    def finish_time(self, start, message = None):
        finish = time.time()
        runtime = time.strftime("%H:%M:%S", time.gmtime(finish - start))
        if message is None:
            return runtime
        else:
            return "%s: %s" % (message, runtime)

    def run_script(self, script):
        _python_path = Config_App.APP_CONFIG['python_path']
        _app_path = Config_App.APP_CONFIG['app_path']
        _script = os.path.join(_app_path, script)

        _command = '%s %s' % (_python_path, _script)
        os.system(_command)

    def check_status(self, process):
        item_log = {self.odb.DB_PUBLICATION_LOG_ID: time.strftime('%Y%m%d'),
                    self.odb.DB_PUBLICATION_LOG_PROCESS: process}
        result = self.odb.select_publication_log(item_log)
        pubmed_status = False
        pubmed_message = result[self.odb.DB_PUBLICATION_LOG_MESSAGE]
        if result[self.odb.DB_PUBLICATION_LOG_STATUS] == self.odb.DB_CONST_STATUS_OK:
            pubmed_status = True

        return pubmed_status, pubmed_message

def main(args):
    start = omain.start_time()
    print('[main|%s] %s' % (time.strftime('%Y%m%d'), 'Run Back-end'))
    print('[main|%s]' % time.strftime('%Y%m%d'))

    print('[main|%s] (1/3) Run %s' % (time.strftime('%Y%m%d'), omain.odb.DB_CONST_PROCESS_PUBMED))
    omain.run_script('search_pubmed.py')
    _status, _message = omain.check_status(omain.odb.DB_CONST_PROCESS_PUBMED)

    if _status:
        print('[main|%s] (1/3) %s' % (time.strftime('%Y%m%d'), 'Successful execution'))
        print('[main|%s]' % time.strftime('%Y%m%d'))

        print('[main|%s] (2/3) Run %s' % (time.strftime('%Y%m%d'), omain.odb.DB_CONST_PROCESS_SCOPUS))
        omain.run_script('search_scopus.py')
        _status, _message = omain.check_status(omain.odb.DB_CONST_PROCESS_SCOPUS)

        if _status:
            print('[main|%s] (2/3) %s' % (time.strftime('%Y%m%d'), 'Successful execution'))
            print('[main|%s]' % time.strftime('%Y%m%d'))

            print('[main|%s] (3/3) Run %s' % (time.strftime('%Y%m%d'), omain.odb.DB_CONST_PROCESS_CONSENSUS))
            omain.run_script('update_publication.py')
            _status, _message = omain.check_status(omain.odb.DB_CONST_PROCESS_CONSENSUS)

            if _status:
                print('[main|%s] (3/3) %s' % (time.strftime('%Y%m%d'), 'Successful execution'))
                print('[main|%s]' % time.strftime('%Y%m%d'))
            else:
                print('[main|%s] (3/3) %s' % (time.strftime('%Y%m%d'), 'Processing failed'))
                print('[main|%s] (3/3) Message: %s' % (time.strftime('%Y%m%d'), _message))
                print('[main|%s]' % time.strftime('%Y%m%d'))
        else:
            print('[main|%s] (2/3) %s' % (time.strftime('%Y%m%d'), 'Processing failed'))
            print('[main|%s] (2/3) Message: %s' % (time.strftime('%Y%m%d'), _message))
            print('[main|%s]' % time.strftime('%Y%m%d'))
    else:
        print('[main|%s] (1/3) %s' % (time.strftime('%Y%m%d'), 'Processing failed'))
        print('[main|%s] (1/3) Message: %s' % (time.strftime('%Y%m%d'), _message))
        print('[main|%s]' % time.strftime('%Y%m%d'))

    print('[main|%s] %s' % (time.strftime('%Y%m%d'), omain.finish_time(start, 'Elapsed time')))
    print('[main|%s] %s' % (time.strftime('%Y%m%d'), 'Done.'))

if __name__ == '__main__':
    omain = Principal()
    main(sys.argv)
