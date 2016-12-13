# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy

class MusicSpiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    
    star_name = scrapy.Field()
    concert_name = scrapy.Field()
    concert_place = scrapy.Field()
    concert_time = scrapy.Field()
    concert_venue = scrapy.Field()
    concert_price = scrapy.Field()
