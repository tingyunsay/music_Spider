#!/usr/bin/env python
# -*- coding:utf-8 -*-  
import sys,os
import re
import time
import subprocess
import commands

def is_runing(spider_name):
	a = commands.getoutput("ps -ef|grep %s|grep -v grep"%spider_name)
	return True if a else False	


def is_runing(spider_name):
        a = commands.getoutput("ps -ef|grep %s|grep -v grep"%spider_name)
        return True if a else False
"""
with open('config.json','r') as f:
        spider_name = eval(f.read()).keys()
        for spider in spider_name[:]:
                if not re.search('2',spider):
                        command = "nohup scrapy crawl %s >> mv_spider.%s.log 2>&1 &"%(spider,(time.strftime("%Y-%m-%d %X",time.localtime())).replace(' ','-'))
                        os.system(command)
                        if is_runing(spider):
                                print "spider %s is runing ...."%spider
                        else:
                                print "spider %s start fail ...."%spider
"""
if len(sys.argv) < 2:
        with open('config.json','r') as f:
                spider_name = eval(f.read()).keys()
                res = []
                for spider in spider_name[:]:
                        if not re.search('2',spider):
                                res.append(spider)
                print "你可以抓取的spider有 ，共%s个:\n %s\t\t\t\n"%(len(res),(','.join(res)))
        print "参数过少，请指定爬取spider!"
else:
        spider = sys.argv[1]
        command = "nohup scrapy crawl %s >> mv_spider.%s.log 2>&1 &"%(spider,(time.strftime("%Y-%m-%d %X",time.localtime())).replace(' ','-'))
        os.system(command)
        if is_runing(spider):
                print "spider %s is runing ...."%spider
        else:
                print "spider %s start fail ...."%spider






"""
with open('config.json','r') as f:
		spider_name = eval(f.read()).keys()
		for spider in spider_name[2:3]:
				command = "nohup scrapy crawl %s >> mv_spider.%s.log 2>&1 &"%(spider,(time.strftime("%Y-%m-%d %X",time.localtime())).replace(' ','-'))
				os.system(command)
				if is_runing(spider):
						print "spider %s is runing ...."%spider
				else:
						print "spider %s start fail ...."%spider

"""
