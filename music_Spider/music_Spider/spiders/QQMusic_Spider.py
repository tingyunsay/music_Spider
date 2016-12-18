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

#相对url转绝对url，假定将从网页中提取的不是http开头的，全部替换成从Index_Url中的第一个/前面部分+网页中提取到的部分					
#你传进来的是list，就返回list；是str，就返回str。一一对应的
def Relative_to_Absolute(index_url,url_tail):
	head_url = re.search(r'.+/',index_url).group()
	if type(url_tail) is list:
		res_urls = []
		if (not re.search(r'^http://',url_tail[0])) and (not re.search(r'^https://',url_tail[0])):
			#print "批量，相对转绝对.........."
			if re.search(r'^/',url_tail[0]):
			#带有/开头的url，要去除掉这个/，再拼接
				for url in url_tail:
					url = re.sub(r'^/',"",url)
					res_urls.append(head_url + url)
			else:
				for url in url_tail:
					res_urls.append(head_url + url)
			return res_urls
		else:
			return url_tail
	else:
		if (not re.search(r'^http://',url_tail)) and (not re.search(r'^https://',url_tail)):
			if re.search(r'^/',url_tail):
				return head_url + re.sub(r'^/',"",url_tail)
			else:
				return head_url + url_tail
		else:
			return url_tail

def Get_Valid_Url(urls):
	if type(urls) is list and len(urls) > 1:
		res_urls = []
		if re.search(r'^//',urls[0]):
			for url in urls:
				res_urls.append("https://"+re.sub(r'^//',"",url))
		else:
			res_urls = urls
		return res_urls
	else:
		if re.search(r'^//',''.join(urls)):
			return re.sub(r'^//',"",''.join(urls))
		else:
			return ''.join(urls)
			

def get_HeadUrl(index_url):
	return re.sub("(\d+)$","{page}",index_url)
	
	

class MusicSpider(scrapy.Spider):
	name ='qq_music'
	allowed_domain = []
		
	def __init__(self,*args,**kwargs):
		super(MusicSpider,self).__init__(*args,**kwargs)
		#用一个list来存放所有的json配置中的k,v，变成了一个元祖list，遍历这个list
		#scrapy.log.start("./log.txt",loglevel=INFO,logstdout=True)
		#self.log = open("./log.txt",'a')
		#print >> self.log,"__________________________分割线___________________________________"
		#print >> self.log,"At Time %s : 爬虫启动............"%time.ctime()
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
			if len(v[1]) == 3:
				self.Index_Url = v[1][0]['Index_Url']
				Max_Page = v[1][0]['Max_Page']
				All_Detail_Page = v[1][1]['All_Detail_Page']
				Final_Xpath = v[1][2]['Final_Xpath']
				
				headers={'User-Agent':"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.75 Safari    /537.36"}
				
				response = requests.get(self.Index_Url,headers = headers).content.decode('utf-8')
				soup = BeautifulSoup(response,"lxml")
				result = response
				if Max_Page['soup']:
					result = str(soup.select(Max_Page['soup']))
				pageNums = re.search(Max_Page['re'],result).group()
				print "最大页数是:%s"%pageNums
				
				urls = get_HeadUrl(self.Index_Url)
				
				#for i in range(1,int(pageNums)+1):
				for i in range(1,2):
						url = urls.format(page=str(i))
						request = Request(url,callback = self.parse_first)
						request.meta['Index_Url'] = self.Index_Url
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
				
				headers={'User-Agent':"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.75 Safari/537.36"}
				response = requests.get(self.Index_Url,headers = headers).content.decode('utf-8')
				#这里统一将页面编码成了utf-8(str类型)，BeautifulSoup统一给它们加上html的架子，每一段都用<p>包起来
				soup = BeautifulSoup(response,"lxml")
				result = response
				if Max_Page['soup']:
					result = str(soup.select(Max_Page['soup']))
				pageNums = re.search(Max_Page['re'],result).group()
				print "最大页数是:%s"%pageNums
				
				#这里就完全抛弃了之前的postdata的想法，直接是找必要的元素，（我假定：可能会存在多个需要传递的变化参数，预留一下这种情况的处理方法）
				urls = get_HeadUrl(self.Index_Url)#其中变化的页面参数用page替换了，下面才会有format
				#for i in range(1,int(pageNums)+1):
				for i in range(1,2):
						url = urls.format(page=str(i))
						request = Request(url,callback = self.parse_first)
						request.meta['Index_Url'] = self.Index_Url
						request.meta['All_Detail_Page'] = All_Detail_Page
						request.meta['Signal_Detail_Page'] = Signal_Detail_Page
						request.meta['Target_Detail_Page'] = Target_Detail_Page
						request.meta['Final_Xpath'] = Final_Xpath
						yield request
				
			
	def parse_first(self,response):
		Index_Url = response.meta['Index_Url']
		All_Detail_Page = response.meta['All_Detail_Page']
		Signal_Detail_Page = response.meta.get('Signal_Detail_Page',None)
		Target_Detail_Page = response.meta.get('Target_Detail_Page',None)
		Final_Xpath = response.meta['Final_Xpath']
		#一个页面可能会需要多个提取的xpath，这里就指定为一个list了
		detail_url = []
		for xpath in All_Detail_Page['xpath']:
				for url in Get_Valid_Url(response.xpath(xpath).extract()):
						detail_url.append(url)
		#现在考虑在每一层加一个判断，相当于如果没有（第一个）要传递给下一层的数据，就直接传递给final_parse（注：在传递给final_parse时需要判断是否需要渲染，这里我暂时先默认都渲染，但是之后可以考虑在config.json的Final_Xpath加一个flag，1表示需要渲染，0表示不需要）
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
						yield request
				else:
						request = Request(url,callback = self.parse_second)
						request.meta['Index_Url'] = Index_Url
						request.meta['Signal_Detail_Page'] = Signal_Detail_Page
						request.meta['Target_Detail_Page'] = Target_Detail_Page
						request.meta['Final_Xpath'] = Final_Xpath
						yield request
	
	def parse_second(self,response):
		Index_Url = response.meta['Index_Url']
		Signal_Detail_Page = response.meta['Signal_Detail_Page']
		Target_Detail_Page = response.meta['Target_Detail_Page']
		Final_Xpath = response.meta['Final_Xpath']
		Some_Info = {}
		if 'Some_Info' in Signal_Detail_Page.keys():
				keys = Signal_Detail_Page['Some_Info'].keys()
				for key in keys:
						try:
								Some_Info[key] = response.xpath(Signal_Detail_Page['Some_Info'][key]).extract()[0]
						except Exception,e:
								print Exception,":",e
		
		detail_url = Relative_to_Absolute(Index_Url,response.xpath(Signal_Detail_Page['xpath']).extract())
		if type(detail_url) is list:
				for url in detail_url:
						request = Request(url,callback = self.parse_third)
						request.meta['Index_Url'] = Index_Url
						request.meta['Target_Detail_Page'] = Target_Detail_Page
						request.meta['Final_Xpath'] = Final_Xpath
						request.meta['Some_Info'] = Some_Info
						yield request
		else:
				request = Request(detail_url,callback = self.parse_third)
				request.meta['Index_Url'] = Index_Url
				request.meta['Target_Detail_Page'] = Target_Detail_Page
				request.meta['Final_Xpath'] = Final_Xpath
				request.meta['Some_Info'] = Some_Info
				yield request
		
	def parse_third(self,response):
		Index_Url = response.meta['Index_Url']
		Target_Detail_Page = response.meta['Target_Detail_Page']
		Final_Xpath = response.meta['Final_Xpath']
		#之后会在每一层都加上一个Some_Info，只要存在它，就会附加传递信息
		Some_Info = response.meta.get('Some_Info',None)
		if 'Some_Info' in Target_Detail_Page.keys():
				keys = Target_Detail_Page['Some_Info'].keys()
				for key in keys:
						try:
								Some_Info[key] = response.xpath(Target_Detail_Page['Some_Info'][key]).extract()[0]
						except Exception,e:
								print Exception,":",e

		detail_url = Relative_to_Absolute(Index_Url,response.xpath(Target_Detail_Page['xpath']).extract())
		

		if type(detail_url) is list:
				#print "这里得到的是批量的，为什么只有一个?"
				for url in detail_url:
					#print "now the url is : %s"%url
					request = scrapy.Request(url,callback = self.parse_final,meta = {
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
					yield request
		else:
				request = Request(detail_url,callback = self.parse_final,meta = {
											'splash':{
											'endpoint':'render.html',
											'args':{
													'wait':1,
													'images':0,
													'render_all':1
													}
												}
									})
				request.meta['Some_Info'] = Some_Info
				request.meta['Final_Xpath'] = Final_Xpath
				yield request

	def parse_final(self,response):
		Final_Xpath = response.meta['Final_Xpath']
		Some_Info = response.meta.get('Some_Info',None)
		#l = ItemLoader(item=MusicSpiderItem(), response=response)
		
		if 'All_Xpath' not in Final_Xpath.keys():
				item = MusicSpiderItem()
				l = ItemLoader(item=item, response=response)
				for key in Final_Xpath.keys():
						item.fields[key] = Field()
						if Final_Xpath[key] == "":
								l.add_value(key , Final_Xpath[key])
						elif type(Final_Xpath[key]) is int:
								l.add_value(key , Final_Xpath[key])
						else:
								try:
										l.add_xpath(key , Final_Xpath[key])
								except Exception,e:
										print Exception,":",e
				if Some_Info:
						for key in Some_Info.keys():
								item.fields[key] = Field()
								l.add_value(key , Some_Info[key])
				yield l.load_item()
		else:
				#l = ItemLoader(item=MusicSpiderItem(), response=response)
				All_Xpath = Final_Xpath['All_Xpath']
				for i in response.xpath(Final_Xpath['All_Xpath']):
						item = MusicSpiderItem()
						l = ItemLoader(item=MusicSpiderItem(), response=response)
						for key in Final_Xpath.keys():
								item.fields[key] = Field()
								try:
										l.add_xpath(key , Final_Xpath[key])
								except Exception,e:
										print Exception,",",e
						if Some_Info:
								for key in Some_Info.keys():
									item.fields[key] = Field()
									l.add_value(key , Some_Info[key])
						yield l.load_item()
				
