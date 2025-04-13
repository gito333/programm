#!/usr/bin/env python
# coding: utf8

import sys
import time

import requests
from urllib.request import urlopen
from urllib.error import HTTPError
from urllib.error import URLError
import urllib3


class ProcessRequest(object):
    def __init__(self):
        self.session = requests.Session()

    # msg script abruptly terminated        
    def script_terminated(self, msg):
        print("___________________________________________________________________________________")
        print("Number of failed attempts. Script abruptly terminated...due to the following error:")
        print("___________________________________________________________________________________")
        print(msg)
        sys.exit()

    # retry 
    def retry(self, n):
        N_ATTEMPS = 10
        WAIT_TIME = 3
        print("reconnecting...", flush=True)
        time.sleep(WAIT_TIME)
        return False if n == N_ATTEMPS else True

    # set request
    def set_request(self, url, params=None, headers=None, verify=None, stream=None):
        count = 0
        loop = True
        while loop:
            try:
                if params is None:
                    response = self.session.get(url, timeout=30, headers=headers, stream=stream)
                else:
                    response = self.session.post(url, data=params, timeout=30, headers=headers)
                response.raise_for_status()
                loop = (response.status_code != 200)
            except requests.exceptions.HTTPError as httpErr: 
                msg = "Http Error: ", httpErr
                if response.status_code == 404 or response.status_code == 500:
                    return False
            except requests.exceptions.ConnectionError as connErr: 
                msg = "Error Connecting: ", connErr
            except requests.exceptions.Timeout as timeOutErr: 
                msg = "Timeout Error: ", timeOutErr
                #return None
            except requests.exceptions.RequestException as reqErr: 
                msg = "Something Else: ", reqErr 
            if loop:
                print(msg)
                count = count + 1
                if not self.retry(count):
                    self.script_terminated(msg)

        return response