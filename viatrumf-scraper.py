# coding=utf-8
from os import getenv

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

envValue = getenv('runningLocally', True)
runningLocally = True == envValue

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

class ViatrumfSpider(scrapy.Spider):
    name = 'viatrumf'
    allowed_domains = ['viatrumf.no']
    top_url = 'https://viatrumf.no/'
    start_urls = ( top_url, )

    def __init__(self, kategori):
        self.kategori = kategori 

    def parse(self, response):
        try: 
            url = '{top_url}/category/paged/{kategori}/100'.format(top_url=self.top_url, kategori=self.kategori)
            return scrapy.Request(url, callback=self.__parseAndPersist)
        except Exception as e:
            print(e)

    def __trimAwayClutter(self, body):
        soup = BeautifulSoup(body, 'html.parser')
        butikkar = []
        nettbutikkar = soup.find_all('a', class_='shop-button')
        for nettbutikk in nettbutikkar:
            if not nettbutikk.attrs['data-name'] == 'zzzz':
                butikkar.append(Nettbutikk(nettbutikk))
        return butikkar

    def __parseAndPersist(self, response):
        nettbutikkar = self.__trimAwayClutter(response.body.decode('unicode_escape'))
        
        self.tidspunkt = datetime.now(pytz.timezone('Europe/Oslo')).strftime('%Y%m%dT%H%M%SZ')
        for nettbutikk in nettbutikkar:
            self.__save(self.__toPersistable(nettbutikk))

    def __toPersistable(self, nettbutikk):
        persistable = {}
        persistable['namn'] = nettbutikk.namn
        persistable['verdi'] = nettbutikk.verdi
        persistable['href'] = nettbutikk.href
        persistable['popularitet'] = nettbutikk.popularitet
        persistable['timestamp'] = self.tidspunkt
        persistable['kategori'] = self.kategori
        return persistable
    
    def __save(self, nettbutikk):
        namn = nettbutikk['namn'].replace(' ', '_').replace('\'', '')
        name = namn + "_" + self.tidspunkt + '.json'
        doc_ref = db.collection('viatrumf-scraper2').document(namn).collection('innslag').document(name)
        doc_ref.set(nettbutikk)

def run(d, f):
    runner = crawler.CrawlerRunner({
        'USER_AGENT': 'Mozilla/5.0 (Linux; U; Android 4.0.3; ko-kr; LG-L160L Build/IML74K) AppleWebkit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30'
    })
    
    kategoriar = ['reise', 'mote-klær', 'sko', 'mote-tilbehør', 'sport', 'elektronikk', 'interiør', 'hus-og-hage', 'bil-og-motor', 'kjæledyr', 'skjønnhet', 'underholdning', 'barn', 'tjenester']
    
    for kategori in kategoriar:
        runner.crawl(ViatrumfSpider, kategori=kategori)

    d = runner.join()
    d.addBoth(lambda _: reactor.stop())
    reactor.run()

if runningLocally:
    try:
        run(None, None)
    except Exception as e:
        print(e)