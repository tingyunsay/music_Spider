#!/usr/bin/env python
# -*- coding:utf-8 -*-  
import sys,os


if len(sys.argv) < 2:
		print "no spider is target , please check."
elif sys.argv[1]:
		spider = sys.argv[1]
		os.system("nohup scrapy crawl %s >> mv_spider.log 2>&1 &"%spider)
		print "spider %s is runing ...."%spider


