import json
import logging

from .database import get_session
from .database import ContratoWeb, ContratoWebHistorial


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
            urls = urls_file.readlines()
        assert len(urls) == len(jl)

    for idx, json_obj in enumerate(jl):
        json_obj['_UPDATED'] = updated
        if urls_path:
            json_obj['ANUNCIO'] = urls[idx]
        if skip_dup:
            anuncio = json_obj['ANUNCIO']
            qr = session.query(ContratoWeb).get(anuncio)
            if qr is None:
                session.add(ContratoWeb(**json_obj))
        else:
            session.add(ContratoWeb(**json_obj))
        session.commit()



