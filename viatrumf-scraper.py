# coding=utf-8
runningLocally = True

from os import getenv
from os import path
from os import makedirs

import pytz
import re
import scrapy.http.request
import scrapy.spiders
import scrapy.crawler as crawler
from collections import OrderedDict
from bs4 import BeautifulSoup
from urllib.parse import urlencode, urljoin
if runningLocally == False:
    from google.cloud import storage
import json
from datetime import datetime
from twisted.internet import reactor

class Nettbutikk:
    def __init__(self, ahref):
        attrs = ahref.attrs
        self.namn = attrs['data-name']
        self.verdi = attrs['data-percentage']
        self.popularitet = attrs['data-popularity']
        self.href = attrs['href']

class ClutterTrimmer:
    def trimAwayClutter(self, body):
        soup = BeautifulSoup(body, 'html.parser')
        butikkar = []
        for nettbutikk in soup.find_all('a', class_='shop-button'):
            if not nettbutikk.attrs['data-name'] == 'zzzz':
                butikkar.append(Nettbutikk(nettbutikk))
        return butikkar

class Persister:
    def __init__(self, target_username):
        self.target_username = target_username

    def __upload_blob(self, bucket_name, blob_text, destination_blob_name):
        """Uploads a file to the bucket."""
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_string(blob_text)

        int('File uploaded to {}.'.format(destination_blob_name))

    def __saveToLocalFile(self, name, nettbutikk):
        folder = self.__getFolder()
        if not (path.exists(folder)):
            makedirs(folder)
        with open(folder + name, 'w', encoding='utf-8') as outfile:
            json.dump(nettbutikk, outfile, ensure_ascii=False)

    def writeToFile(self, nettbutikk):
        name = nettbutikk['namn'].replace(' ', '_') + "_" + self.__getToday().strftime('%Y%m%d_%H%M%S') + '.json'
        if (runningLocally):
            self.__saveToLocalFile(name, nettbutikk)
        else:
            self.__upload_blob('viatrumf', json.dumps(nettbutikk, ensure_ascii=False), self.__getFolder() + name)
    
    def __getFolder(self):
        return  'nettbutikkar/' + self.__getToday().strftime('%Y%m%d') +'/'

    def __getToday(self):
        return datetime.now(pytz.timezone('Europe/Oslo'))

class ViatrumfSpider(scrapy.Spider):
    name = 'viatrumf'
    allowed_domains = ['viatrumf.no']
    top_url = 'https://viatrumf.no/kategori'
    start_urls = ( top_url, )

    def __init__(self, target_username):
        self.target_username = target_username 

    def parse(self, response):
        try: 
            url = '{top_url}/{username}'.format(top_url=self.top_url, username=self.target_username)
            return scrapy.Request(url, callback=self._parseAndPersist)
        except Exception as e:
            print(e)

    def _parseAndPersist(self, response):
        body = response.body.decode('unicode_escape')
        nettbutikkar = ClutterTrimmer().trimAwayClutter(body)
        
        persister = Persister(self.target_username)
        for nettbutikk in nettbutikkar:
            nettbutikkPersistable = self.__toPersistable(nettbutikk)
            persister.writeToFile(nettbutikkPersistable)

    def __toPersistable(self, nettbutikk):
        persistable = {}
        persistable['namn'] = nettbutikk.namn
        persistable['verdi'] = nettbutikk.verdi
        persistable['href'] = nettbutikk.href
        persistable['popularitet'] = nettbutikk.popularitet
        return persistable

def fetch():
    runner = crawler.CrawlerRunner({
        'USER_AGENT': 'Mozilla/5.0 (Linux; U; Android 4.0.3; ko-kr; LG-L160L Build/IML74K) AppleWebkit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30'
    })
    
    kategoriar = ['reise', 'mote', 'sport', 'elektronikk', 'bolig', 'velvære', 'underholdning', 'barn', 'tjenester']

    for kategori in kategoriar:
        runner.crawl(ViatrumfSpider, target_username=kategori)

    d = runner.join()
    d.addBoth(lambda _: reactor.stop())
    reactor.run()

def run(d, f):
    fetch()

def runSingleParam(d):
    run(d, None)

if runningLocally:
    try:
        run(None, None)
    except Exception as e:
        print(e)