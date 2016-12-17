import json
import logging
import os

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from .database import get_session
from .database import ContratoWeb, ContratoWebHistorial
from .database import ContratoXls
from .crawler.compranet_web.spiders.compranet_spider import CompraNetSpider


logger = logging.getLogger('compranet.web')

session = get_session()

def read_jl(jl_path, drop_keys={'_CONDITIONAL_PREFIX_FOR'}):
    """Read a JSON Lines file"""
    # TODO: remove drop_keys once scraper removes these fields
    def drop(d, keys):
        for key in keys:
            d.pop(key, None)
        return(d)

    with open(jl_path) as jl_file:
        jl = [drop(json.loads(line), drop_keys) for line in jl_file]
        return(jl)

def load_jl(jl_path, updated, urls_path=None, skip_dup=False, session=session):
    """Load a JSON Lines file to database"""
    # TODO: implement change tracking
    # TODO: remove updated argument once field is added to scraper
    # TODO: remove urls_path and skip_dup after import of old results
    jl = read_jl(jl_path)
    if urls_path is not None:
        with open(urls_path) as urls_file:
            urls = [url.strip() for url in urls_file.readlines()]
        assert len(urls) == len(jl)

    for idx, json_obj in enumerate(jl):
        json_obj['_UPDATED'] = updated
        if urls_path:
            json_obj['ANUNCIO'] = urls[idx]
        anuncio = json_obj['ANUNCIO']
        if anuncio == '':
            continue
        if skip_dup:
            qr = session.query(ContratoWeb).get(anuncio)
            if qr is None:
                print("Adding {}".format(anuncio))
                session.add(ContratoWeb(**json_obj))
            else:
                print("Skipping {}".format(anuncio))
        else:
            session.add(ContratoWeb(**json_obj))
        session.commit()

def scrape_missing(source=None):
    #TODO: add support for tracking scraper history
    settings_module = 'compranet.crawler.compranet_web.settings'
    os.environ['SCRAPY_SETTINGS_MODULE'] = settings_module
    session = get_session()
    w_urls = set(x[0] for x in session.query(ContratoWeb.ANUNCIO).all())
    if source is None:
        x_urls = set(x[0] for x in session.query(ContratoXls.ANUNCIO).all())
    else:
        xls_query = (session.query(ContratoXls.ANUNCIO)
                     .filter(ContratoXls._SOURCE == source))
        x_urls = set(x[0] for x in xls_query.all())
    urls_to_scrape = list(x_urls.difference(w_urls))
    process = CrawlerProcess(get_project_settings())
    process.crawl(CompraNetSpider, urls=urls_to_scrape)
    process.start()


