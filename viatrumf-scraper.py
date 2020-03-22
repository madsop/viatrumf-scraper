# coding=utf-8
runningLocally = True

from os import getenv
from os import path
from os import makedirs

import pytz
import scrapy.http.request
import scrapy.spiders
import scrapy.crawler as crawler
from bs4 import BeautifulSoup
from datetime import datetime
from twisted.internet import reactor

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

if runningLocally:
    cred = credentials.Certificate('viatrumf-scraper-271913-1e8fcedf7e5b.json')
    firebase_admin.initialize_app(cred)
else:
    cred = credentials.ApplicationDefault()
    firebase_admin.initialize_app()

db = firestore.client()


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
    def __init__(self):
        self.tidspunkt = self.__getToday().strftime('%Y%m%d_%H%M%S')

    def save(self, nettbutikk):
        name = nettbutikk['namn'].replace(' ', '_') + "_" + self.tidspunkt + '.json'
        doc_ref = db.collection('viatrumf-scraper').document(name)
        doc_ref.set(nettbutikk)

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
        nettbutikkar = ClutterTrimmer().trimAwayClutter(response.body.decode('unicode_escape'))
        
        persister = Persister()
        for nettbutikk in nettbutikkar:
            persister.save(self.__toPersistable(nettbutikk))

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

if runningLocally:
    try:
        run(None, None)
    except Exception as e:
        print(e)