#!coding=utf8
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import scrapy
#from scrapy_redis.spiders import RedisSpider
from scrapy.loader import ItemLoader
from scrapy.http import Request
from music_Spider.items import MusicSpiderItem
import requests
import bs4
from scrapy.loader.processors import MapCompose
from bs4 import BeautifulSoup
import lxml
import re
from urllib2 import URLError
import json
import time
import signal
from pybloomfilter import BloomFilter
import os
from scrapy.exceptions import CloseSpider
from urllib import quote_plus
import datetime
from scrapy.item import Item,Field
import hashlib
from music_Spider.Path_translate import Relative_to_Absolute,Get_Valid_Url,get_HeadUrl
from music_Spider.Total_page_circulate import Total_page_circulate


class MusicSpider(scrapy.Spider):
	name ='douban'
	allowed_domain = []
		
	def __init__(self,*args,**kwargs):
		super(MusicSpider,self).__init__(*args,**kwargs)
		#用一个list来存放所有的json配置中的k,v，变成了一个元祖list，遍历这个list
		#scrapy.log.start("./log.txt",loglevel=INFO,logstdout=True)
		self.now = time.time()
		self.one_month_ago = datetime.datetime(time.localtime(self.now).tm_year,time.localtime(self.now).tm_mon-1,time.localtime(self.now).tm_mday)
		self.config = []
		self.Index_Url = ""
			
	
	def start_requests(self):
		with open('config.json','r') as f:
			data = json.load(f)
			for i in data.iteritems():
				if i[0].encode('utf-8') == self.name:
					self.config.append(i)
			f.close()
		
		for v in self.config:
			if len(v[1]) == 2:
				self.Index_Url = v[1][0]['Index_Url']
				Max_Page = v[1][0]['Max_Page']
				Final_Xpath = v[1][1]['Final_Xpath']
				#这里我想改成每一个第一页面都渲染一次，同意用xpath去得到最大页面，而不用bs4和正则了，所以在和parse_first之间，再多加一层渲染的处理
				meta = {
						'splash':{
								'endpoint':'render.html',
								'args':{
										'wait':0.5,
										'images':0,
										'render_all':1
										}
								}
						}
				request = Request(self.Index_Url,self.parse_splash,meta=meta)
				request.meta['Index_Url'] = self.Index_Url
				request.meta['Max_Page'] = Max_Page
				request.meta['Final_Xpath'] = Final_Xpath
				yield request
			
			if len(v[1]) == 3:
				self.Index_Url = v[1][0]['Index_Url']
				Is_Json = v[1][0]['Is_Json']
				Max_Page = v[1][0]['Max_Page']
				All_Detail_Page = v[1][1]['All_Detail_Page']
				Final_Xpath = v[1][2]['Final_Xpath']
				meta = {
						'splash':{
								'endpoint':'render.html',
								'args':{
										'wait':0.5,
										'images':0,
										'render_all':1
										}
								}
						}
				if Is_Json == 1:
						for url in self.Index_Url:
								request = Request(url,self.parse_json)
								request.meta['Index_Url'] = url
								request.meta['Is_Json'] = Is_Json
								request.meta['Max_Page'] = Max_Page
								request.meta['All_Detail_Page'] = All_Detail_Page
								request.meta['Final_Xpath'] = Final_Xpath
								yield request
				else:
						for url in  self.Index_Url:
								request = Request(url,self.parse_splash,meta=meta)
								request.meta['Index_Url'] = url
								request.meta['Is_Json'] = Is_Json
								request.meta['Max_Page'] = Max_Page
								request.meta['All_Detail_Page'] = All_Detail_Page
								request.meta['Final_Xpath'] = Final_Xpath
								yield request	
				

			if len(v[1]) == 5:
				self.Index_Url = v[1][0]['Index_Url']
				Max_Page = v[1][0]['Max_Page']
				All_Detail_Page = v[1][1]['All_Detail_Page']
				Signal_Detail_Page = v[1][2]['Signal_Detail_Page']
				Target_Detail_Page = v[1][3]['Target_Detail_Page']
				Final_Xpath = v[1][4]['Final_Xpath']
				meta = {
						'splash':{
								'endpoint':'render.html',
								'args':{
										'wait':0.5,
										'images':0,
										'render_all':1
										}
								}
						}
				request = Request(self.Index_Url,callback = self.parse_splash,meta=meta)
				request.meta['Index_Url'] = self.Index_Url
				request.meta['Max_Page'] = Max_Page
				request.meta['All_Detail_Page'] = All_Detail_Page
				request.meta['Signal_Detail_Page'] = Signal_Detail_Page
				request.meta['Target_Detail_Page'] = Target_Detail_Page
				request.meta['Final_Xpath'] = Final_Xpath
				yield request
				

	def parse_splash(self,response):
		#这边就是管你有没有，我都接收，在使用的时候判断，如果不存在，说明要直接到final_parse处
		Index_Url = response.meta.get('Index_Url',None)
		Max_Page = response.meta.get('Max_Page',None)
		All_Detail_Page = response.meta.get('All_Detail_Page',None)
		Signal_Detail_Page = response.meta.get('Signal_Detail_Page',None)
		Target_Detail_Page = response.meta.get('Target_Detail_Page',None)
		Final_Xpath = response.meta.get('Final_Xpath',None)
		max_pages = 2
		try:
				max_pages = re.search(Max_Page['re'],''.join(response.xpath(Max_Page['xpath']).extract())).group()
		except Exception,e:
				print Exception,":",e
		#这里是替换末尾的\d+，记住，遇上其他情况，就扩展这个get_HeadUrl()
		urls = get_HeadUrl(Index_Url)

		max_pages = Total_page_circulate(self.name,int(max_pages))
		print "最大页数是:%d"%max_pages
		if All_Detail_Page is None:
				#for i in range(1,max_pages+1):
				for i in range(1,2):
						url = urls.format(page=str(i))
						request = Request(url,callback = self.parse_final,dont_filter=True,meta={
											'splash':{
											'endpoint':'render.html',
											'args':{
													'wait':0.5,
													'images':0,
													'render_all':1
													}
											}
								})
						request.meta['Final_Xpath'] = Final_Xpath
						yield request
		else:
				#for i in range(1,int(max_pages)+1):
				for i in range(1,2):
						try:
								url = urls.format(page=str(i))
						except Exception,e:
								print Exception,":",e
						request = Request(url,callback = self.parse_first,meta={
										'splash':{
										'endpoint':'render.html',
										'args':{
												'wait':5,
												'images':0,
												'render_all':1
												}
										}
								})
						request.meta['Index_Url'] = Index_Url
						request.meta['All_Detail_Page'] = All_Detail_Page
						request.meta['Signal_Detail_Page'] = Signal_Detail_Page
						request.meta['Target_Detail_Page'] = Target_Detail_Page
						request.meta['Final_Xpath'] = Final_Xpath
						yield request

	def parse_json(self,response):
		Index_Url = response.meta.get('Index_Url',None)
		Max_Page = response.meta.get('Max_Page',None)
		All_Detail_Page = response.meta.get('All_Detail_Page',None)
		Signal_Detail_Page = response.meta.get('Signal_Detail_Page',None)
		Target_Detail_Page = response.meta.get('Target_Detail_Page',None)
		Final_Xpath = response.meta.get('Final_Xpath',None)
		res_json = json.loads(response.body_as_unicode())
		
		depth = 0
		try:
				while depth < len(Max_Page['index']):
						res_json = res_json.get(Max_Page['index'][depth])
						depth += 1
		except Exception,e:
				print Exception,":",e
		urls = get_HeadUrl(Index_Url)	
		
		print "now the res_json is %s"%res_json
		max_pages = Total_page_circulate(self.name,int(res_json))
		print "最大页数是:%d"%max_pages
		if All_Detail_Page is None:
				#for i in range(1,max_pages+1):
				for i in range(1,2):
						url = urls.format(page=str(i))
						request = Request(url,callback = self.parse_final,dont_filter=True,meta={
											'splash':{
											'endpoint':'render.html',
											'args':{
													'wait':0.5,
													'images':0,
													'render_all':1
													}
											}
								})
						request.meta['Final_Xpath'] = Final_Xpath
						yield request
		else:
				#for i in range(1,int(max_pages)+1):
				for i in range(1,2):
						try:
								url = urls.format(page=str(i))
						except Exception,e:
								print Exception,":",e
						request = Request(url,callback = self.parse_json2,dont_filter=True)
						request.meta['Index_Url'] = Index_Url
						request.meta['All_Detail_Page'] = All_Detail_Page
						request.meta['Signal_Detail_Page'] = Signal_Detail_Page
						request.meta['Target_Detail_Page'] = Target_Detail_Page
						request.meta['Final_Xpath'] = Final_Xpath
						yield request
		
	def parse_json2(self,response):
		Index_Url = response.meta.get('Index_Url',None)
		All_Detail_Page = response.meta.get('All_Detail_Page',None)
		Signal_Detail_Page = response.meta.get('Signal_Detail_Page',None)
		Target_Detail_Page = response.meta.get('Target_Detail_Page',None)
		Final_Xpath = response.meta.get('Final_Xpath',None)
		detail_url = []
		res_json = json.loads(response.body_as_unicode())
		#递归读取最底层的key对应的value值，我去，想出来了～～[这里是要for一遍最底层的list，所以要读到len-1处，然后在得到detail_url]
		depth = 0
		length = len(All_Detail_Page['index'])
		while depth < length - 1:
				res_json = res_json.get(All_Detail_Page['index'][depth])
				depth += 1
		#print "now the res_json is %s"%res_json
		for i in res_json:
				detail_url.append(i.get(All_Detail_Page['index'][length-1]))
		try:
				detail_url = Relative_to_Absolute(Index_Url,detail_url)
		except Exception,e:
				print Exception,":",e
		
		for url in detail_url:
				if Signal_Detail_Page is None:
						request = Request(url,callback = self.parse_final,meta={
											'splash':{
											'endpoint':'render.html',
											'args':{
													'wait':0.5,
													'images':0,
													'render_all':1
													}
											}
									})
						request.meta['Final_Xpath'] = Final_Xpath
						#time.sleep(4)
						yield request
				else:
						request = Request(url,callback = self.parse_second)
						request.meta['Index_Url'] = Index_Url
						request.meta['Signal_Detail_Page'] = Signal_Detail_Page
						request.meta['Target_Detail_Page'] = Target_Detail_Page
						request.meta['Final_Xpath'] = Final_Xpath
						yield request

			
	def parse_first(self,response):
		Index_Url = response.meta.get('Index_Url',None)
		All_Detail_Page = response.meta.get('All_Detail_Page',None)
		Signal_Detail_Page = response.meta.get('Signal_Detail_Page',None)
		Target_Detail_Page = response.meta.get('Target_Detail_Page',None)
		Final_Xpath = response.meta.get('Final_Xpath',None)
		Some_Info = {}
		if 'Some_Info' in All_Detail_Page.keys():
				keys = All_Detail_Page['Some_Info'].keys()
				for key in keys:
						try:
								Some_Info[key] = response.xpath(All_Detail_Page['Some_Info'][key]).extract()[0]
						except Exception,e:
								print Exception,":",e
		#一个页面可能会需要多个提取的xpath，这里就指定为一个list了
		detail_url = []
		for xpath in All_Detail_Page['xpath']:
				for url in Relative_to_Absolute(Index_Url,response.xpath(xpath).extract()):
						detail_url.append(url)
		#在考虑在每一层加一个判断，相当于如果没有（第一个）要传递给下一层的数据，就直接传递给final_parse（注：在传递给final_parse时需要判断是否需要渲染，这里我暂时先默认都渲染，但是之后可以考虑在config.json的Final_Xpath加一个flag，1表示需要渲染，0表示不需要）
		for url in detail_url:
				if Signal_Detail_Page is None:
						request = Request(url,callback = self.parse_final,meta={
											'splash':{
											'endpoint':'render.html',
											'args':{
													'wait':0.5,
													'images':0,
													'render_all':1
													}
											}
									})
						request.meta['Some_Info'] = Some_Info
						request.meta['Final_Xpath'] = Final_Xpath
						#time.sleep(4)
						yield request
				else:
						request = Request(url,callback = self.parse_second)
						request.meta['Index_Url'] = Index_Url
						request.meta['Some_Info'] = Some_Info
						request.meta['Signal_Detail_Page'] = Signal_Detail_Page
						request.meta['Target_Detail_Page'] = Target_Detail_Page
						request.meta['Final_Xpath'] = Final_Xpath
						yield request
	
	def parse_second(self,response):
		Index_Url = response.meta.get('Index_Url',None)
		Signal_Detail_Page = response.meta.get('Signal_Detail_Page',None)
		Target_Detail_Page = response.meta.get('Target_Detail_Page',None)
		Final_Xpath = response.meta.get('Final_Xpath',None)
		Some_Info = response.meta.get('Some_Info',None)
		if 'Some_Info' in Signal_Detail_Page.keys():
				keys = Signal_Detail_Page['Some_Info'].keys()
				for key in keys:
						try:
								Some_Info[key] = response.xpath(Signal_Detail_Page['Some_Info'][key]).extract()[0]
						except Exception,e:
								print Exception,":",e
		
		detail_url = Relative_to_Absolute(Index_Url,response.xpath(Signal_Detail_Page['xpath']).extract())
		if Signal_Detail_Page is None:
				for url in detail_url:
						request = Request(url,callback = self.parse_final,meta={
												'splash':{
												'endpoint':'render.html',
												'args':{
														'wait':0.5,
														'images':0,
														'render_all':1
													}
												}
											})
						request.meta['Some_Info'] = Some_Info
						request.meta['Final_Xpath'] = Final_Xpath
						#time.sleep(4)
						yield request
		else:
				try:
					for url in detail_url:
						#print "now the url is %s"%url
						request = Request(url,callback = self.parse_third)
						request.meta['Index_Url'] = Index_Url
						request.meta['Target_Detail_Page'] = Target_Detail_Page
						request.meta['Final_Xpath'] = Final_Xpath
						request.meta['Some_Info'] = Some_Info
						yield request
				except Exception,e:
					print Exception,":",e
		

	def parse_third(self,response):
		Index_Url = response.meta['Index_Url']
		Target_Detail_Page = response.meta.get('Target_Detail_Page',None)
		Final_Xpath = response.meta.get('Final_Xpath',None)
		Some_Info = response.meta.get('Some_Info',None)
		if 'Some_Info' in Target_Detail_Page.keys():
				keys = Target_Detail_Page['Some_Info'].keys()
				for key in keys:
						try:
								Some_Info[key] = response.xpath(Target_Detail_Page['Some_Info'][key]).extract()[0]
						except Exception,e:
								print Exception,":",e
		#print "now the url is %s, and the Some_Info is %s"%(response.url,Some_Info["artist_name"])
		detail_url = Relative_to_Absolute(Index_Url,response.xpath(Target_Detail_Page['xpath']).extract())	
		for url in detail_url:
				request = scrapy.Request(url,callback = self.parse_final,meta = {
										'splash':{
										'endpoint':'render.html',
										'args':{
												'wait':5,
												'images':0,
												'render_all':1
												}
										}
								})				
				request.meta['Some_Info'] = Some_Info
				request.meta['Final_Xpath'] = Final_Xpath
				#print "我就想看看传递过去的Some_Info是%s,并且当前访问的url是%s"%(Some_Info['artist_name'],url)
				yield request


	def parse_final(self,response):
		Final_Xpath = response.meta.get('Final_Xpath',None)
		Some_Info = response.meta.get('Some_Info',None)

		#print "我就想看看得到的Some_Info是%s"%(Some_Info['artist_name'])
		#l = ItemLoader(item=MusicSpiderItem(), response=response)
		if 'All_Xpath' not in Final_Xpath.keys():
				item = MusicSpiderItem()
				l = ItemLoader(item=item, response=response)
				for key in Final_Xpath.keys():
						item.fields[key] = Field()
						try:
								map(lambda x:l.add_xpath(key , x),Final_Xpath[key])
						except Exception,e:
								print Exception,":",e
				if Some_Info:
						for key in Some_Info.keys():
								item.fields[key] = Field()
								l.add_value(key , Some_Info[key])
				yield l.load_item()
		else:
		#感觉这里不能用itemloader的add_xxx方法了，因为要先找到一个页面所有的含有目标item的块，再在每个块里面提取出单个item，itemloader的话是一次性直接全取出，add_xpath不能再细分了;;打算用add_value方法
				All_Xpath = Final_Xpath['All_Xpath']
				del Final_Xpath['All_Xpath']
				for i in response.xpath(All_Xpath):
						item = MusicSpiderItem()
						l = ItemLoader(item=MusicSpiderItem(), response=response)
						for key in Final_Xpath.keys():
								item.fields[key] = Field()
								try:
										map(lambda x:l.add_value(key , ''.join(i.xpath(x).extract())),Final_Xpath[key])
								except Exception,e:
										print Exception,",",e
						if Some_Info:
								for key in Some_Info.keys():
									item.fields[key] = Field()
									l.add_value(key , Some_Info[key])
						yield l.load_item()
				
										#'splash':{
								#		'endpoint':'render.html',
								#		'args':{
								#				'wait':5,
								#				'images':0,
								#				'render_all':1
								#				}
								#		}
								#})				