# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import logging

from compranet.database import get_session, ContratoWeb


class SqlitePipeline(object):
    def open_spider(self, spider):
        self.session = get_session()

    def close_spider(self, spider):
        self.session.close()

    def process_item(self, item, spider):
        try:
            self.session.add(ContratoWeb(**item))
            self.session.commit()
            logging.debug("Inserted to database {}".format(item['ANUNCIO']))
        except:
            logging.exception("SqlitePipeline exception")
            self.session.rollback()
        return item
