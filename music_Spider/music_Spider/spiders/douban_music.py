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
from music_Spider.path_translate import R_2_A,G_V_U,U_G,C_U_V
from music_Spider.total_page_circulate import T_P_C,T_T_P,T_P_B


class music_Spider(scrapy.Spider):
	name ='douban_music'
	allowed_domain = []
		
	def __init__(self,*args,**kwargs):
		super(music_Spider,self).__init__(*args,**kwargs)
		self.now = time.time()
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
			#self.Splash作为全局变量，控制final页是否渲染
			if len(v[1]) == 2:
				self.Splash = v[1][0]['Splash']
				self.Index_Url = v[1][0]['Index_Url']
				Segement = v[1][0]['Index_Url']['Segement']
				Final_Xpath = v[1][1]['Final_Xpath']
				#有json这个字段，说明为json页
				if  self.Index_Url.has_key('json'):
						for url in self.Index_Url['url']:
								request = Request(url,self.parse_json)
								request.meta['Index_Url'] = url
								request.meta['Segement'] = Segement
								request.meta['Final_Xpath'] = Final_Xpath
								yield request
				else:
						#默认是不指定splash，即不渲染，走else；只要加上了splash这个key，就表示需要渲染。
						#每一层会有这样的规则
						if self.Index_Url.has_key('splash'):
								for url in self.Index_Url['url']:
										request = Request(url,self.parse_zero,meta={
												'splash':{
												'endpoint':'render.html',
												'args':{
														'wait':0.5,
														'images':0,
														'render_all':1
														}
												}
										})				
										request.meta['Index_Url'] = url
										request.meta['Segement'] = Segement
										request.meta['Final_Xpath'] = Final_Xpath
										yield request
						else:
								for url in  self.Index_Url['url']:
										request = Request(url,self.parse_zero)				
										request.meta['Index_Url'] = url
										request.meta['Segement'] = Segement
										request.meta['Final_Xpath'] = Final_Xpath
										yield request
								
			
			if len(v[1]) == 3:
				self.Splash = v[1][0]['Splash']
				self.Index_Url = v[1][0]['Index_Url']
				Segement = v[1][0]['Index_Url']['Segement']
				First = v[1][1]['First']
				Final_Xpath = v[1][2]['Final_Xpath']
				if  self.Index_Url.has_key('json'):
						for url in self.Index_Url['url']:
								request = Request(url,self.parse_json)
								request.meta['Index_Url'] = url
								request.meta['Segement'] = Segement
								request.meta['First'] = First
								request.meta['Final_Xpath'] = Final_Xpath
								yield request
				else:
						if self.Index_Url.has_key('splash'):
								for url in  self.Index_Url['url']:
										request = Request(url,self.parse_zero,meta={
												'splash':{
												'endpoint':'render.html',
												'args':{
														'wait':0.5,
														'images':0,
														'render_all':1
														}
												}
										})				
										request.meta['Index_Url'] = url
										request.meta['Segement'] = Segement
										request.meta['First'] = First
										request.meta['Final_Xpath'] = Final_Xpath
										yield request
						else:
								for url in  self.Index_Url['url']:
										request = Request(url,self.parse_zero)
										request.meta['Index_Url'] = url
										request.meta['Segement'] = Segement
										request.meta['First'] = First
										request.meta['Final_Xpath'] = Final_Xpath
										yield request	
				
			if len(v[1]) == 4:
				self.Splash = v[1][0]['Splash']
				self.Index_Url = v[1][0]['Index_Url']
				Segement = v[1][0]['Index_Url']['Segement']
				First = v[1][1]['First']
				Second = v[1][2]['Second']
				Final_Xpath = v[1][3]['Final_Xpath']
				if self.Index_Url.has_key('json'):
						for url in self.Index_Url['url']:
								request = Request(url,callback = self.parse_json)
								request.meta['Index_Url'] = url
								request.meta['Segement'] = Segement
								request.meta['First'] = First
								request.meta['Second'] = Second
								request.meta['Final_Xpath'] = Final_Xpath
								yield request
				else:
						if self.Index_Url.has_key('splash'):
								for url in self.Index_Url['url']:
										request = Request(url,callback = self.parse_zero,dont_filter=True,meta={
													'splash':{
															'endpoint':'render.html',
															'args':{
																	'wait':0.5,
																	'images':0,
																	'render_all':1
																}
															}
														})
										request.meta['Index_Url'] = url
										request.meta['Segement'] = Segement
										request.meta['First'] = First
										request.meta['Second'] = Second
										request.meta['Final_Xpath'] = Final_Xpath
										yield request
						else:
								for url in self.Index_Url['url']:
										request = Request(url,callback = self.parse_zero,dont_filter=True)
										request.meta['Index_Url'] = url
										request.meta['Segement'] = Segement
										request.meta['First'] = First
										request.meta['Second'] = Second
										request.meta['Final_Xpath'] = Final_Xpath
										yield request

			if len(v[1]) == 5:
				self.Splash = v[1][0]['Splash']
				self.Index_Url = v[1][0]['Index_Url']
				Segement = v[1][0]['Index_Url']['Segement']
				First = v[1][1]['First']
				Second = v[1][2]['Second']
				Third = v[1][3]['Third']
				Final_Xpath = v[1][4]['Final_Xpath']
				if self.Index_Url.has_key('json'):
						for url in self.Index_Url['url']:
								request = Request(url,callback = self.parse_json)
								request.meta['Index_Url'] = url
								request.meta['Segement'] = Segement
								request.meta['First'] = First
								request.meta['Second'] = Second
								request.meta['Third'] = Third
								request.meta['Final_Xpath'] = Final_Xpath
								yield request
				else:
						if self.Index_Url.has_key('splash'):
								for url in self.Index_Url['url']:
										request = Request(url,callback = self.parse_zero,meta={
													'splash':{
															'endpoint':'render.html',
															'args':{
																	'wait':0.5,
																	'images':0,
																	'render_all':1
																}
															}
														})
										request.meta['Index_Url'] = url
										request.meta['Segement'] = Segement
										request.meta['First'] = First
										request.meta['Second'] = Second
										request.meta['Third'] = Third
										request.meta['Final_Xpath'] = Final_Xpath
										yield request
						else:
								for url in self.Index_Url['url']:
										request = Request(url,callback = self.parse_zero)
										request.meta['Index_Url'] = url
										request.meta['Segement'] = Segement
										request.meta['First'] = First
										request.meta['Second'] = Second
										request.meta['Third'] = Third
										request.meta['Final_Xpath'] = Final_Xpath
										yield request
	
	def parse_zero(self,response):
		#这个是真正处理第一级页面的函数，上面只不过是一个分类，每个层级需要传递的参数确定，再分别传输
		#规定：接受Segement参数，或者xpath参数，前者表示会分页，并将分页得到的link提交到绑定函数处理，再传递给正常的下一层
		Index_Url = response.meta.get('Index_Url',None)
		Segement = response.meta.get('Segement',None)
		First = response.meta.get('First',None)
		Second = response.meta.get('Second',None)
		Third = response.meta.get('Third',None)
		Final_Xpath = response.meta.get('Final_Xpath',None)
		max_pages = 1
		urls = ""
		level = 0
		is_sege = 0

		#分清跳转的前提条件：Segement存在Max_Page，即这个函数必然是跳转到对应的分页处理函数segment_xxx，然后再返回url给parse_next ; else即是跳转到正常parse_next
		if Segement.has_key('Max_Page'):
				if not Segement['Max_Page'].has_key('json'):
						try:
								max_pages = re.search(Segement['Max_Page']['re'],''.join(response.xpath(Segement['Max_Page']['xpath']).extract())).group()
						except Exception,e:
								print Exception,":",e
						if isinstance(max_pages,unicode):
								max_pages = max_pages.encode('utf-8')
						if isinstance(max_pages,int) or isinstance(max_pages,str):
								max_pages = T_P_C(self.name,int(max_pages),level)
						else:
								raise ValueError("parse_zero: ERROR 1,in the splashing parse,can not find the Max_page,please check!!!")
						urls,start_url = U_G(Index_Url,self.name,level)
				#存在json即用json的方式去解读
				else:
						res_json = json.loads(response.body_as_unicode())
						depth = 0
						if isinstance(Segement['Max_Page']['index'],list):
								try:
									while depth < len(Segement['Max_Page']['index']):
										res_json = res_json.get(Segement['Max_Page']['index'][depth])
										depth += 1
								except Exception,e:
									print Exception,":",e 
								max_pages = T_P_C(self.name,int(res_json),level)
						else:
								raise ValueError("parse_zero: ERROR 1 ,in the json parse,can not find the Max_page,please check!!!")
						#将这个U_G做成和R_2_A一样的函数，主要应对的还是分页
						urls,start_url = U_G(Index_Url,self.name,level)
				#如果该站点压根没有告诉你有多少页面，那就只能手动给出一个值了，如下函数.
				if not max_pages:
					max_pages = T_P_B(self.name,level)	
				print "最大页数是:%d"%max_pages
				#分了页，之后，就是绑定分页处理函数.（存在segement参数就绑定，不存在就直接进入下一层）
				#当然跳转到下一层只是说明这一层分页得到的页面不要再处理，还是需要判断是否需要渲染
				if Segement['Max_Page'].has_key('segement'):
						if Segement['Max_Page'].has_key('splash'):
								begin = 0
								try:
										begin = re.search('\d+$',start_url).group()
								except Exception,e:
										print Exception,":",e,".parse_zero: ERROR 1-2,can not find the start page number in the splash page,please check!!!"
								for i in range(int(begin),max_pages+1):
										i = T_T_P(i,self.name,level)
										url = urls.format(page=str(i))
										if C_U_V(url):
												request = Request(url,callback = self.segement_zero,dont_filter=True,meta={
																			'splash':{
																			'endpoint':'render.html',
																			'args':{
																					'wait':0.5,
																					'images':0,
																					'render_all':1
																					}
																				}
																			})
												request.meta['Index_Url'] = url
												request.meta['First'] = First
												request.meta['Second'] = Second
												request.meta['Third'] = Third
												request.meta['segement'] = Segement['Max_Page']['segement']
												request.meta['Final_Xpath'] = Final_Xpath
												yield request
										else:
												continue
						else:
								begin = 0
								try:
										begin = re.search('\d+$',start_url).group()
								except Exception,e:
										print Exception,":",e,".parse_zero: ERROR 1-2,can not find the start page number in the normal page,please check!!!"
								for i in range(int(begin),max_pages+1):
										i = T_T_P(i,self.name,level)
										url = urls.format(page=str(i))
										if C_U_V(url):
												request = Request(url,callback = self.segement_zero,dont_filter=True)
												request.meta['Index_Url'] = url
												request.meta['First'] = First
												request.meta['Second'] = Second
												request.meta['Third'] = Third
												request.meta['segement'] = Segement['Max_Page']['segement']
												request.meta['Final_Xpath'] = Final_Xpath
												yield request
										else:
												continue
				else:
						if Segement['Max_Page'].has_key('splash'):
								begin = 0
								try:
										begin = re.search('\d+$',start_url).group()
								except Exception,e:
										print Exception,":",e,".parse_zero: ERROR 1-3,can not find the start page number,please check!!!"
								for i in range(int(begin),max_pages+1):
										i = T_T_P(i,self.name,level)
										url = urls.format(page=str(i))
										if C_U_V(url):
												request = Request(url,callback = self.parse_first,dont_filter=True,meta={
																			'splash':{
																			'endpoint':'render.html',
																			'args':{
																					'wait':0.5,
																					'images':0,
																					'render_all':1
																					}
																				}
																			})
												request.meta['Index_Url'] = url
												request.meta['First'] = First
												request.meta['Second'] = Second
												request.meta['Third'] = Third
												request.meta['Final_Xpath'] = Final_Xpath
												yield request
										else:
												continue
						else:
								begin = 0
								try:
										begin = re.search('\d+$',start_url).group()
								except Exception,e:
										print Exception,":",e,".parse_zero: ERROR 1-4,can not find the start page number,please check!!!"
								for i in range(int(begin),max_pages+1):
										i = T_T_P(i,self.name,level)
										url = urls.format(page=str(i))
										if C_U_V(url):
												request = Request(url,callback = self.parse_first,dont_filter=True)
												request.meta['Index_Url'] = url
												request.meta['First'] = First
												request.meta['Second'] = Second
												request.meta['Third'] = Third
												request.meta['Final_Xpath'] = Final_Xpath
												yield request
										else:
												continue
						


		else:
				detail_url = []
				if not Segement.has_key('json'):
						for xpath in Segement['xpath']:
								for url in R_2_A(Index_Url,response.xpath(xpath).extract(),self.name,level,is_sege):
										detail_url.append(url)
				else:
						res_json = json.loads(response.body_as_unicode())
						#递归读取最底层的key对应的value值，我去，想出来了～～[这里是要for一遍最底层的list，所以要读到len-1处，然后在得到detail_url]
						depth = 0
						length = len(Segement['index'])
						while depth < length - 1:
							res_json = res_json.get(Segement['index'][depth])
							depth += 1
							#print "now the res_json is %s"%res_json
						for i in res_json:
							detail_url.append(i.get(Segement['index'][length-1]))
						try:
							detail_url = R_2_A(Index_Url,detail_url,self.name,level,is_sege)
						except Exception,e:
							print Exception,":",e
				if First is None:
						if self.Splash:
								for url in detail_url:
										if C_U_V(url):
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
												continue
						else:
								for url in detail_url:
										if C_U_V(url):
												request = Request(url,callback = self.parse_final,dont_filter=True)
												request.meta['Final_Xpath'] = Final_Xpath
												yield request
										else:
												continue
				else:
						for url in detail_url:
								if C_U_V(url):
										request = Request(url,callback = self.parse_first)
										request.meta['Index_Url'] = url
										request.meta['First'] = First
										request.meta['Second'] = Second
										request.meta['Third'] = Third
										request.meta['Final_Xpath'] = Final_Xpath
										yield request
								else:
										continue
								
						


	def parse_first(self,response):
		Index_Url = response.meta.get('Index_Url',None)
		First = response.meta.get('First',None)
		Second = response.meta.get('Second',None)
		Third = response.meta.get('Third',None)
		Final_Xpath = response.meta.get('Final_Xpath',None)
		Some_Info = {}
		max_pages = 1
		urls = ""
		level = 1
		is_sege = 0

		if 'Some_Info' in First.keys():
				keys = First['Some_Info'].keys()
				for key in keys:
						try:
								Some_Info[key] = response.xpath(First['Some_Info'][key]).extract()[0]
						except Exception,e:
								print Exception,":",e
		
		if First.has_key('Max_Page'):
				if not First['Max_Page'].has_key('json'):
						try:
								max_pages = re.search(First['Max_Page']['re'],''.join(response.xpath(First['Max_Page']['xpath']).extract())).group()
						except Exception,e:
								print Exception,":",e
						if isinstance(max_pages,unicode):
								max_pages = max_pages.encode('utf-8')
						if isinstance(max_pages,int) or isinstance(max_pages,str):
								max_pages = T_P_C(self.name,int(max_pages),level)
						else:
								raise ValueError("parse_first: ERROR 1,in the splashing,can not find the Max_page,please check!!!")
						urls,start_url = U_G(Index_Url,self.name,level)
				#存在json即用json的方式去解读
				else:
						res_json = json.loads(response.body_as_unicode())
						depth = 0
						if isinstance(First['Max_Page']['index'],list):
								try:
									while depth < len(First['Max_Page']['index']):
										res_json = res_json.get(First['Max_Page']['index'][depth])
										depth += 1
								except Exception,e:
									print Exception,":",e 
								max_pages = T_P_C(self.name,int(res_json),level)
						else:
								raise ValueError("parse_first: ERROR 1,in the json parse,can not find the Max_page,please check!!!")
						urls,start_url = U_G(Index_Url,self.name,level)
				#如果该站点压根没有告诉你有多少页面，那就只能手动给出一个值了，如下函数.
				if not max_pages:
					max_pages = T_P_B(self.name,level)	
				print "最大页数是:%d"%max_pages
				#分了页，之后，就是绑定分页处理函数.这里是segement_first:又分成是否需要渲染
				if First['Max_Page'].has_key('segement'):
						if First['Max_Page'].has_key('splash'):
								begin = 0
								try:
										begin = re.search('\d+$',start_url).group()
								except Exception,e:
										print Exception,":",e,".parse_first: ERROR 1-1,can not find the start page number,please check!!!"
								for i in range(int(begin),max_pages+1):
										i = T_T_P(i,self.name,level)
										url = urls.format(page=str(i))
										if C_U_V(url):
												request = Request(url,callback = self.segement_first,dont_filter=True,meta={
																				'splash':{
																				'endpoint':'render.html',
																				'args':{
																					'wait':0.5,
																					'images':0,
																					'render_all':1
																					}
																				}
																			})
												request.meta['Index_Url'] = url
												request.meta['Some_Info'] = Some_Info
												request.meta['segement'] = First['Max_Page']['segement']
												request.meta['First'] = First
												request.meta['Second'] = Second
												request.meta['Third'] = Third
												request.meta['Final_Xpath'] = Final_Xpath
												yield request
										else:
												continue
						else:
								begin = 0
								try:
										begin = re.search('\d+$',start_url).group()
								except Exception,e:
										print Exception,":",e,".parse_first: ERROR 1-2,can not find the start page number,please check!!!"
								for i in range(int(begin),max_pages+1):
										i = T_T_P(i,self.name,level)
										url = urls.format(page=str(i))
										if C_U_V(url):
												request = Request(url,callback = self.segement_first,dont_filter=True)
												request.meta['Some_Info'] = Some_Info
												request.meta['Index_Url'] = url
												request.meta['segement'] = First['Max_Page']['segement']
												request.meta['First'] = First
												request.meta['Second'] = Second
												request.meta['Third'] = Third
												request.meta['Final_Xpath'] = Final_Xpath
												yield request
										else:
												continue
				else:
						if First['Max_Page'].has_key('splash'):
								begin = 0
								try:
										begin = re.search('\d+$',start_url).group()
								except Exception,e:
										print Exception,":",e,".parse_first: ERROR 1-3,can not find the start page number,please check!!!"
								for i in range(int(begin),max_pages+1):
										i = T_T_P(i,self.name,level)
										url = urls.format(page=str(i))
										if C_U_V(url):
												request = Request(url,callback = self.parse_second,dont_filter=True,meta={
																				'splash':{
																				'endpoint':'render.html',
																				'args':{
																					'wait':0.5,
																					'images':0,
																					'render_all':1
																					}
																				}
																			})
												request.meta['Index_Url'] = url
												request.meta['Some_Info'] = Some_Info
												request.meta['First'] = First
												request.meta['Second'] = Second
												request.meta['Third'] = Third
												request.meta['Final_Xpath'] = Final_Xpath
												yield request
										else:
												continue
						else:
								begin = 0
								try:
										begin = re.search('\d+$',start_url).group()
								except Exception,e:
										print Exception,":",e,".parse_first: ERROR 1-4,can not find the start page number,please check!!!"
								for i in range(int(begin),max_pages+1):
										i = T_T_P(i,self.name,level)
										url = urls.format(page=str(i))
										if C_U_V(url):
												request = Request(url,callback = self.parse_second,dont_filter=True)
												request.meta['Some_Info'] = Some_Info
												request.meta['Index_Url'] = url
												request.meta['First'] = First
												request.meta['Second'] = Second
												request.meta['Third'] = Third
												request.meta['Final_Xpath'] = Final_Xpath
												yield request
										else:
												continue
							

		else:
				detail_url = []
				if not First.has_key('json'):
						for xpath in First['xpath']:
								for url in R_2_A(Index_Url,response.xpath(xpath).extract(),self.name,level,is_sege):
										detail_url.append(url)
				else:
						res_json = json.loads(response.body_as_unicode())
						#递归读取最底层的key对应的value值，我去，想出来了～～[这里是要for一遍最底层的list，所以要读到len-1处，然后在得到detail_url]
						depth = 0
						length = len(First['index'])
						while depth < length - 1:
							res_json = res_json.get(First['index'][depth])
							depth += 1
							#print "now the res_json is %s"%res_json
						for i in res_json:
							detail_url.append(i.get(First['index'][length-1]))
						try:
							detail_url = R_2_A(Index_Url,detail_url,self.name.level,is_sege)
						except Exception,e:
							print Exception,":",e
				if Second is None:
						if self.Splash:
								for url in detail_url:
										if C_U_V(url):
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
												request.meta['Some_Info'] = Some_Info
												request.meta['Final_Xpath'] = Final_Xpath
												yield request
										else:
												continue
						else:
								for url in detail_url:
										if C_U_V(url):
												request = Request(url,callback = self.parse_final,dont_filter=True)
												request.meta['Some_Info'] = Some_Info
												request.meta['Final_Xpath'] = Final_Xpath
												yield request
										else:
												continue
				else:
						for url in detail_url:
								if C_U_V(url):
										request = Request(url,callback = self.parse_second)
										request.meta['Index_Url'] = url
										request.meta['Second'] = Second
										request.meta['Third'] = Third
										request.meta['Final_Xpath'] = Final_Xpath
										yield request
								else:
										continue
	
	def parse_second(self,response):
		Index_Url = response.meta.get('Index_Url',None)
		Second = response.meta.get('Second',None)
		Third = response.meta.get('Third',None)
		Final_Xpath = response.meta.get('Final_Xpath',None)
		Some_Info = {}
		max_pages = 1
		urls = ""
		level = 2
		is_sege = 0

		if 'Some_Info' in Second.keys():
				keys = Second['Some_Info'].keys()
				for key in keys:
						try:
								Some_Info[key] = response.xpath(Second['Some_Info'][key]).extract()[0]
						except Exception,e:
								print Exception,":",e
		if Second.has_key('Max_Page'):
				if not Second['Max_Page'].has_key('json'):
						try:
								max_pages = re.search(Second['Max_Page']['re'],''.join(response.xpath(Second['Max_Page']['xpath']).extract())).group()
						except Exception,e:
								print Exception,":",e
						if isinstance(max_pages,unicode):
								max_pages = max_pages.encode('utf-8')
						if isinstance(max_pages,int) or isinstance(max_pages,str):
								max_pages = T_P_C(self.name,int(max_pages),level)
						else:
								raise ValueError("parse_second: ERROR 1,in the splashing,can not find the Max_page,please check!!!")
						urls,start_url = U_G(Index_Url,self.name,level)
				#存在json即用json的方式去解读
				else:
						res_json = json.loads(response.body_as_unicode())
						depth = 0
						if isinstance(First['Max_Page']['index'],list):
								try:
									while depth < len(First['Max_Page']['index']):
										res_json = res_json.get(First['Max_Page']['index'][depth])
										depth += 1
								except Exception,e:
									print Exception,":",e 
								max_pages = T_P_C(self.name,int(res_json),level)
						else:
								raise ValueError("parse_second: ERROR 1,in the json parse,can not find the Max_page,please check!!!")
						urls,start_url = U_G(Index_Url,self.name,level)
				#如果该站点压根没有告诉你有多少页面，那就只能手动给出一个值了，如下函数.
				if not max_pages:
					max_pages = T_P_B(self.name,level)	
				print "最大页数是:%d"%max_pages
				#分了页，之后，就是绑定分页处理函数.这里是segement_first:又分成是否需要渲染
				if Second['Max_Page'].has_key('segement'):
						if Second['Max_Page'].has_key('splash'):
								begin = 0
								try:
										begin = re.search('\d+$',start_url).group()
								except Exception,e:
										print Exception,":",e,".parse_second: ERROR 1-1,can not find the start page number,please check!!!"
								for i in range(int(begin),max_pages+1):
										i = T_T_P(i,self.name,level)
										url = urls.format(page=str(i))
										if C_U_V(url):
												request = Request(url,callback = self.segement_second,dont_filter=True,meta={
																			'splash':{
																			'endpoint':'render.html',
																			'args':{
																					'wait':0.5,
																					'images':0,
																					'render_all':1
																					}
																				}
																			})
												request.meta['Index_Url'] = url
												request.meta['Some_Info'] = Some_Info
												request.meta['segement'] = Second['Max_Page']['segement']
												request.meta['Second'] = Second
												request.meta['Third'] = Third
												request.meta['Final_Xpath'] = Final_Xpath
												yield request
										else:
												continue
						else:
								begin = 0

								try:
										begin = re.search('\d+$',start_url).group()
								except Exception,e:
										print Exception,":",e,".parse_second: ERROR 1-2,can not find the start page number,please check!!!"

								for i in range(int(begin),max_pages+1):
										i = T_T_P(i,self.name,level)
										url = urls.format(page=str(i))
										if C_U_V(url):
												request = Request(url,callback = self.segement_second,dont_filter=True)
												request.meta['Some_Info'] = Some_Info
												request.meta['Index_Url'] = url
												request.meta['segement'] = Second['Max_Page']['segement']
												request.meta['Second'] = Second
												request.meta['Third'] = Third
												request.meta['Final_Xpath'] = Final_Xpath
												yield request
										else:
												continue
				else:
						if Second['Max_Page'].has_key('splash'):
								begin = 0
								try:
										begin = re.search('\d+$',start_url).group()
								except Exception,e:
										print Exception,":",e,".parse_second: ERROR 1-3,can not find the start page number,please check!!!"
								for i in range(int(begin),max_pages+1):
										i = T_T_P(i,self.name,level)
										url = urls.format(page=str(i))
										if C_U_V(url):
												request = Request(url,callback = self.parse_third,dont_filter=True,meta={
																			'splash':{
																			'endpoint':'render.html',
																			'args':{
																					'wait':0.5,
																					'images':0,
																					'render_all':1
																					}
																				}
																			})
												request.meta['Index_Url'] = url
												request.meta['Some_Info'] = Some_Info
												request.meta['Second'] = Second
												request.meta['Third'] = Third
												request.meta['Final_Xpath'] = Final_Xpath
												yield request
										else:
												continue
						else:
								begin = 0
								try:
										begin = re.search('\d+$',start_url).group()
								except Exception,e:
										print Exception,":",e,".parse_second: ERROR 1-4,can not find the start page number,please check!!!"
								for i in range(int(begin),max_pages+1):
										i = T_T_P(i,self.name,level)
										url = urls.format(page=str(i))
										if C_U_V(url):
												request = Request(url,callback = self.parse_third,dont_filter=True)
												request.meta['Some_Info'] = Some_Info
												request.meta['Index_Url'] = url
												request.meta['Second'] = Second
												request.meta['Third'] = Third
												request.meta['Final_Xpath'] = Final_Xpath
												yield request
										else:
												continue

		else:
				detail_url = []
				if not Second.has_key('json'):
						for xpath in Second['xpath']:
								for url in R_2_A(Index_Url,response.xpath(xpath).extract(),self.name,level,is_sege):
										detail_url.append(url)
				else:
						res_json = json.loads(response.body_as_unicode())
						#递归读取最底层的key对应的value值，我去，想出来了～～[这里是要for一遍最底层的list，所以要读到len-1处，然后在得到detail_url]
						depth = 0
						length = len(Second['index'])
						while depth < length - 1:
							res_json = res_json.get(Second['index'][depth])
							depth += 1
							#print "now the res_json is %s"%res_json
						for i in res_json:
							detail_url.append(i.get(Second['index'][length-1]))
						try:
							detail_url = R_2_A2(Index_Url,detail_url,self.name,level,is_sege)
						except Exception,e:
							print Exception,":",e
				if Third is None:
						if self.Splash:
								for url in detail_url:
										if C_U_V(url):
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
												request.meta['Some_Info'] = Some_Info
												request.meta['Final_Xpath'] = Final_Xpath
												yield request
										else:
												continue
						else:
								for url in detail_url:
										if C_U_V(url):
												request = Request(url,callback = self.parse_final,dont_filter=True)
												request.meta['Some_Info'] = Some_Info
												request.meta['Final_Xpath'] = Final_Xpath
												yield request
										else:
												continue
				else:
						for url in detail_url:
								if C_U_V(url):
										request = Request(url,callback = self.parse_third)
										request.meta['Index_Url'] = url
										request.meta['Third'] = Third
										request.meta['Final_Xpath'] = Final_Xpath
										yield request
								else:
										continue
		

	def parse_third(self,response):
		Index_Url = response.meta['Index_Url']
		Third = response.meta.get('Third',None)
		Final_Xpath = response.meta.get('Final_Xpath',None)
		Some_Info = {}
		urls = ""
		max_pages = 1
		level = 3
		is_sege = 0

		if 'Some_Info' in Third.keys():
				keys = Third['Some_Info'].keys()
				for key in keys:
						try:
								Some_Info[key] = response.xpath(Third['Some_Info'][key]).extract()[0]
						except Exception,e:
								print Exception,":",e
		if Third.has_key('Max_Page'):
				if not Third['Max_Page'].has_key('json'):
						try:
								max_pages = re.search(Third['Max_Page']['re'],''.join(response.xpath(Third['Max_Page']['xpath']).extract())).group()
						except Exception,e:
								print Exception,":",e
						if isinstance(max_pages,unicode):
								max_pages = max_pages.encode('utf-8')
						if isinstance(max_pages,int) or isinstance(max_pages,str):
								max_pages = T_P_C(self.name,int(max_pages),level)
						else:
								raise ValueError("parse_third: ERROR 1,in the splashing,can not find the Max_page,please check!!!")
						urls,start_url = U_G(Index_Url,self.name,level)
				#存在json即用json的方式去解读
				else:
						res_json = json.loads(response.body_as_unicode())
						depth = 0
						if isinstance(First['Max_Page']['index'],list):
								try:
									while depth < len(First['Max_Page']['index']):
										res_json = res_json.get(First['Max_Page']['index'][depth])
										depth += 1
								except Exception,e:
									print Exception,":",e 
								max_pages = T_P_C(self.name,int(res_json),level)
						else:
								raise ValueError("parse_third: ERROR 1,in the json parse,can not find the Max_page,please check!!!")
						urls,start_url = U_G(Index_Url,self.name,level)
				#如果该站点压根没有告诉你有多少页面，那就只能手动给出一个值了，如下函数.
				if not max_pages:
					max_pages = T_P_B(self.name,level)	
				print "最大页数是:%d"%max_pages
				#分了页，之后，就是绑定分页处理函数.这里是segement_third:又分成是否需要渲染
				if Third['Max_Page'].has_key('segement'):
						if Third['Max_Page'].has_key('splash'):
								begin = 0
								try:
										begin = re.search('\d+$',start_url).group()
								except Exception,e:
										print Exception,":",e,".parse_third: ERROR 1-1,can not find the start page number,please check!!!"
								for i in range(int(begin),max_pages+1):
										i = T_T_P(i,self.name,level)
										url = urls.format(page=str(i))
										if C_U_V(url):
												request = Request(url,callback = self.segement_third,dont_filter=True,meta={
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
												request.meta['segement'] = Third['Max_Page']['segement']
												request.meta['Third'] = Third
												request.meta['Final_Xpath'] = Final_Xpath
												yield request
										else:
												continue
						else:
								begin = 0
								try:
										begin = re.search('\d+$',start_url).group()
								except Exception,e:
										print Exception,":",e,".parse_third: ERROR 1-2,can not find the start page number,please check!!!"
								for i in range(int(begin),max_pages+1):
										i = T_T_P(i,self.name,level)
										url = urls.format(page=str(i))
										if C_U_V(url):
												request = Request(url,callback = self.segement_third,dont_filter=True)
												request.meta['Index_Url'] = url
												request.meta['Some_Info'] = Some_Info
												request.meta['segement'] = Third['Max_Page']['segement']
												request.meta['Third'] = Third
												request.meta['Final_Xpath'] = Final_Xpath
												yield request
										else:
												continue
				else:
						if Third['Max_Page'].has_key('splash'):
								begin = 0
								try:
										begin = re.search('\d+$',start_url).group()
								except Exception,e:
										print Exception,":",e,".parse_third: ERROR 1-3,can not find the start page number,please check!!!"
								for i in range(int(begin),max_pages+1):
										i = T_T_P(i,self.name,level)
										url = urls.format(page=str(i))
										if C_U_V(url):
												request = Request(url,callback = self.parse_forth,dont_filter=True,meta={
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
												request.meta['Third'] = Third
												request.meta['Final_Xpath'] = Final_Xpath
												yield request
										else:
												continue
						else:
								begin = 0
								try:
										begin = re.search('\d+$',start_url).group()
								except Exception,e:
										print Exception,":",e,".parse_third: ERROR 1-4,can not find the start page number,please check!!!"
								for i in range(int(begin),max_pages+1):
										i = T_T_P(i,self.name,level)
										url = urls.format(page=str(i))
										if C_U_V(url):
												request = Request(url,callback = self.parse_forth,dont_filter=True)
												request.meta['Some_Info'] = Some_Info
												request.meta['Third'] = Third
												request.meta['Final_Xpath'] = Final_Xpath
												yield request
										else:
												continue
		
		else:
				detail_url = []
				if not Third.has_key('json'):
						for xpath in Third['xpath']:
								for url in R_2_A(Index_Url,response.xpath(xpath).extract(),self.name,level,is_sege):
										detail_url.append(url)
				else:
						res_json = json.loads(response.body_as_unicode())
						#递归读取最底层的key对应的value值，我去，想出来了～～[这里是要for一遍最底层的list，所以要读到len-1处，然后在得到detail_url]
						depth = 0
						length = len(Third['index'])
						while depth < length - 1:
							res_json = res_json.get(Third['index'][depth])
							depth += 1
							#print "now the res_json is %s"%res_json
						for i in res_json:
							detail_url.append(i.get(Third['index'][length-1]))
						try:
							detail_url = R_2_A(Index_Url,detail_url,self.name,level,is_sege)
						except Exception,e:
							print Exception,":",e
				if Third is None:
						if self.Splash:
								for url in detail_url:
										if C_U_V(url):
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
												request.meta['Some_Info'] = Some_Info
												request.meta['Final_Xpath'] = Final_Xpath
												yield request
										else:
												continue
						else:
								for url in detail_url:
										if C_U_V(url):
												request = Request(url,callback = self.parse_final,dont_filter=True)
												request.meta['Final_Xpath'] = Final_Xpath
												yield request
										else:
												continue
				else:
						for url in detail_url:
								if C_U_V(url):
										request = Request(url,callback = self.parse_final)
										request.meta['Some_Info'] = Some_Info
										request.meta['Index_Url'] = Index_Url
										request.meta['Final_Xpath'] = Final_Xpath
										yield request
								else:
										continue
	
	def parse_forth(self,response):
		pass
	
	#现在修改逻辑是：每一层绑定一个segement函数，在存在segement字段的时候，需要交给这个segement函数处理。
	#第一层一般都需要分页，我们加上segement这个字段，并且后面的命名规则定为：segement_zero,segement_first,segement_second,segement_third......
	def segement_zero(self,response):
		#这边就是管你有没有，我都接收，在使用的时候判断，如果不存在，说明要直接到final_parse处
		Some_Info = response.meta.get('Some_Info',None)
		Index_Url = response.meta.get('Index_Url',None)
		segement = response.meta.get('segement',None)
		First = response.meta.get('First',None)
		Second = response.meta.get('Second',None)
		Third = response.meta.get('Third',None)
		Final_Xpath = response.meta.get('Final_Xpath',None)
		detail_url = []
		level = 0
		is_sege = 1
		
		if segement.has_key('json'):
				res_json = json.loads(response.body_as_unicode())
				#递归读取最底层的key对应的value值，我去，想出来了～～[这里是要for一遍最底层的list，所以要读到len-1处，然后在得到detail_url]
				depth = 0
				length = len(segement['index'])
				while depth < length - 1:
						res_json = res_json.get(segement['index'][depth])
						depth += 1
				#print "now the res_json is %s"%res_json
				for i in res_json:
						detail_url.append(i.get(segement['index'][length-1]))
				try:
						detail_url = R_2_A(Index_Url,detail_url,self.name,level,is_sege)
				except Exception,e:
						print Exception,":",e
		else:
				for xpath in segement['xpath']:
						for url in R_2_A(Index_Url,response.xpath(xpath).extract(),self.name,level,is_sege):
								detail_url.append(url)
		#这里是第一层的分页处理函数，如果接受到了下一层级的数据，就继续传递给parse_first(第二层) ; 如果分完页，没有下一层的数据，说明得到的这些页面就是目标页面，直接进到parse_final
		if First is None:
				if self.Splash:
						for url in detail_url:
								if C_U_V(url):
										request = Request(url,callback = self.parse_final,dont_filter=True,meta={
															'splash':{
															'endpoint':'render.html',
															'args':{
																	#只有aiyiyi需要load 10s，才能拿到播放量
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
										continue
				else:
						for url in detail_url:
								if C_U_V(url):
										request = Request(url,callback = self.parse_final,dont_filter=True)
										request.meta['Some_Info'] = Some_Info
										request.meta['Final_Xpath'] = Final_Xpath
										yield request
								else:
										continue
		else:
				if segement.has_key('splash'):
						for url in detail_url:
								if C_U_V(url):
										#增加一个可有可无的splash参数，存在就渲染，不存在就默认走静态
										request = Request(url,callback = self.parse_first,dont_filter=True,meta={
															'splash':{
															'endpoint':'render.html',
															'args':{
																	'wait':0.5,
																	'images':0,
																	'render_all':1
																	}
																}
															})
										#我没想到起始页有不是www.xxxx.com/xxx/xxx这种开头的，这个芒果台是list.mangguo.com/.... 
										#这里我不能用这个url头部来构造下一层url，所以我把当前页面的url头部作为新的Index_Url传递下去
										request.meta['Some_Info'] = Some_Info
										request.meta['Index_Url'] = url
										request.meta['First'] = First
										request.meta['Second'] = Second
										request.meta['Third'] = Third
										request.meta['Final_Xpath'] = Final_Xpath
										yield request
								else:
										continue
				else:
						for url in detail_url:
								if C_U_V(url):
										request = Request(url,callback = self.parse_first,dont_filter=True)
										request.meta['Some_Info'] = Some_Info
										request.meta['Index_Url'] = url
										request.meta['First'] = First
										request.meta['Second'] = Second
										request.meta['Third'] = Third
										request.meta['Final_Xpath'] = Final_Xpath
										yield request
								else:
										continue		
	
	def segement_first(self,response):
		Some_Info = response.meta.get('Some_Info',None)
		Index_Url = response.meta.get('Index_Url',None)
		segement = response.meta.get('segement',None)
		First = response.meta.get('First',None)
		Second = response.meta.get('Second',None)
		Third = response.meta.get('Third',None)
		Final_Xpath = response.meta.get('Final_Xpath',None)
		detail_url = []
		level = 1
		is_sege = 1
		if First.has_key('json'):
				res_json = json.loads(response.body_as_unicode())
				#递归读取最底层的key对应的value值，我去，想出来了～～[这里是要for一遍最底层的list，所以要读到len-1处，然后在得到detail_url]
				depth = 0
				length = len(segement['index'])
				while depth < length - 1:
						res_json = res_json.get(segement['index'][depth])
						depth += 1
				#print "now the res_json is %s"%res_json
				for i in res_json:
						detail_url.append(i.get(segement['index'][length-1]))
				try:
						detail_url = R_2_A(Index_Url,detail_url,self.name,level,is_sege)
				except Exception,e:
						print Exception,":",e
		else:
				for xpath in segement['xpath']:
						for url in R_2_A(Index_Url,response.xpath(xpath).extract(),self.name,level,is_sege):
								detail_url.append(url)
		#这里是第一层的分页处理函数，如果接受到了下一层级的数据，就继续传递给parse_first(第二层) ; 如果分完页，没有下一层的数据，说明得到的这些页面就是目标页面，直接进到parse_final
		if Second is None:
				if self.Splash:
						for url in detail_url:
								if C_U_V(url):
										request = Request(url,callback = self.parse_final,dont_filter=True,meta={
															'splash':{
															'endpoint':'render.html',
															'args':{
																	#只有aiyiyi需要load 10s，才能拿到播放量
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
										continue
				else:
						for url in detail_url:
								if C_U_V(url):
										request = Request(url,callback = self.parse_final,dont_filter=True)
										request.meta['Some_Info'] = Some_Info
										request.meta['Final_Xpath'] = Final_Xpath
										yield request
								else:
										continue
		else:
				if segement.has_key('splash'):
						for url in detail_url:
								if C_U_V(url):
										#增加一个可有可无的splash参数，存在就渲染，不存在就默认走静态
										request = Request(url,callback = self.parse_second,dont_filter=True,meta={
															'splash':{
															'endpoint':'render.html',
															'args':{
																	'wait':0.5,
																	'images':0,
																	'render_all':1
																	}
																}
															})
										#我没想到起始页有不是www.xxxx.com/xxx/xxx这种开头的，这个芒果台是list.mangguo.com/.... 
										#这里我不能用这个url头部来构造下一层url，所以我把当前页面的url头部作为新的Index_Url传递下去
										request.meta['Some_Info'] = Some_Info
										request.meta['Index_Url'] = url
										request.meta['Second'] = Second
										request.meta['Third'] = Third
										request.meta['Final_Xpath'] = Final_Xpath
										yield request
								else:
										continue
				else:
						for url in detail_url:
								if C_U_V(url):
										request = Request(url,callback = self.parse_second,dont_filter=True)
										request.meta['Some_Info'] = Some_Info
										request.meta['Index_Url'] = url
										request.meta['Second'] = Second
										request.meta['Third'] = Third
										request.meta['Final_Xpath'] = Final_Xpath
										yield request
								else:
	
									continue		

	def segement_second(self,response):
		Some_Info = response.meta.get('Some_Info',None)
		Index_Url = response.meta.get('Index_Url',None)
		segement = response.meta.get('segement',None)
		Second = response.meta.get('Second',None)
		Third = response.meta.get('Third',None)
		Final_Xpath = response.meta.get('Final_Xpath',None)
		detail_url = []
		level = 2
		is_sege = 1
		if Second.has_key('json'):
				res_json = json.loads(response.body_as_unicode())
				#递归读取最底层的key对应的value值，我去，想出来了～～[这里是要for一遍最底层的list，所以要读到len-1处，然后在得到detail_url]
				depth = 0
				length = len(segement['index'])
				while depth < length - 1:
						res_json = res_json.get(segement['index'][depth])
						depth += 1
				#print "now the res_json is %s"%res_json
				for i in res_json:
						detail_url.append(i.get(segement['index'][length-1]))
				try:
						detail_url = R_2_A(Index_Url,detail_url,self.name,level,is_sege)
				except Exception,e:
						print Exception,":",e
		else:
				for xpath in segement['xpath']:
						for url in R_2_A(Index_Url,response.xpath(xpath).extract(),self.name,level,is_sege):
								detail_url.append(url)
		#这里是第一层的分页处理函数，如果接受到了下一层级的数据，就继续传递给parse_first(第二层) ; 如果分完页，没有下一层的数据，说明得到的这些页面就是目标页面，直接进到parse_final
		if Third is None:
				if self.Splash:
						for url in detail_url:
								if C_U_V(url):
										request = Request(url,callback = self.parse_final,dont_filter=True,meta={
															'splash':{
															'endpoint':'render.html',
															'args':{
																	#只有aiyiyi需要load 10s，才能拿到播放量
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
										continue
				else:
						for url in detail_url:
								if C_U_V(url):
										request = Request(url,callback = self.parse_final,dont_filter=True)
										request.meta['Some_Info'] = Some_Info
										request.meta['Final_Xpath'] = Final_Xpath
										yield request
								else:
										continue
		else:
				if segement.has_key('splash'):
						for url in detail_url:
								if C_U_V(url):
										#增加一个可有可无的splash参数，存在就渲染，不存在就默认走静态
										request = Request(url,callback = self.parse_third,dont_filter=True,meta={
															'splash':{
															'endpoint':'render.html',
															'args':{
																	'wait':0.5,
																	'images':0,
																	'render_all':1
																	}
																}
															})
										#我没想到起始页有不是www.xxxx.com/xxx/xxx这种开头的，这个芒果台是list.mangguo.com/.... 
										#这里我不能用这个url头部来构造下一层url，所以我把当前页面的url头部作为新的Index_Url传递下去
										request.meta['Some_Info'] = Some_Info
										request.meta['Index_Url'] = url
										request.meta['Second'] = Second
										request.meta['Third'] = Third
										request.meta['Final_Xpath'] = Final_Xpath
										yield request
								else:
										continue
				else:
						for url in detail_url:
								if C_U_V(url):
										request = Request(url,callback = self.parse_third,dont_filter=True)
										request.meta['Some_Info'] = Some_Info
										request.meta['Index_Url'] = url
										request.meta['Second'] = Second
										request.meta['Third'] = Third
										request.meta['Final_Xpath'] = Final_Xpath
										yield request
								else:
										continue		
	
	def segement_third(self,response):
		Some_Info = response.meta.get('Some_Info',None)
		Index_Url = response.meta.get('Index_Url',None)
		segement = response.meta.get('segement',None)
		Third = response.meta.get('Third',None)
		Forth = response.meta.get('Forth',None)
		Final_Xpath = response.meta.get('Final_Xpath',None)
		detail_url = []
		level = 3
		is_sege = 1
		if Third.has_key('json'):
				res_json = json.loads(response.body_as_unicode())
				#递归读取最底层的key对应的value值，我去，想出来了～～[这里是要for一遍最底层的list，所以要读到len-1处，然后在得到detail_url]
				depth = 0
				length = len(segement['index'])
				while depth < length - 1:
						res_json = res_json.get(segement['index'][depth])
						depth += 1
				#print "now the res_json is %s"%res_json
				for i in res_json:
						detail_url.append(i.get(segement['index'][length-1]))
				try:
						detail_url = R_2_A(Index_Url,detail_url,self.name,level,is_sege)
				except Exception,e:
						print Exception,":",e
		else:
				for xpath in segement['xpath']:
						for url in R_2_A(Index_Url,response.xpath(xpath).extract(),self.name,level,is_sege):
								detail_url.append(url)
		#这里是第一层的分页处理函数，如果接受到了下一层级的数据，就继续传递给parse_first(第二层) ; 如果分完页，没有下一层的数据，说明得到的这些页面就是目标页面，直接进到parse_final
		if Forth is None:
				if self.Splash:
						for url in detail_url:
								if C_U_V(url):
										request = Request(url,callback = self.parse_final,dont_filter=True,meta={
															'splash':{
															'endpoint':'render.html',
															'args':{
																	#只有aiyiyi需要load 10s，才能拿到播放量
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
										continue
				else:
						for url in detail_url:
								if C_U_V(url):
										request = Request(url,callback = self.parse_final,dont_filter=True)
										request.meta['Some_Info'] = Some_Info
										request.meta['Final_Xpath'] = Final_Xpath
										yield request
								else:
										continue
		else:
				if segement.has_key('splash'):
						for url in detail_url:
								if C_U_V(url):
										#增加一个可有可无的splash参数，存在就渲染，不存在就默认走静态
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
										#我没想到起始页有不是www.xxxx.com/xxx/xxx这种开头的，这个芒果台是list.mangguo.com/.... 
										#这里我不能用这个url头部来构造下一层url，所以我把当前页面的url头部作为新的Index_Url传递下去
										request.meta['Some_Info'] = Some_Info
										request.meta['Index_Url'] = url
										request.meta['Final_Xpath'] = Final_Xpath
										yield request
								else:
										continue
				else:
						for url in detail_url:
								if C_U_V(url):
										request = Request(url,callback = self.parse_final,dont_filter=True)
										request.meta['Some_Info'] = Some_Info
										request.meta['Index_Url'] = url
										request.meta['Final_Xpath'] = Final_Xpath
										yield request
								else:
										continue	
		
	
		
	


	def parse_final(self,response):
		#我去，这个Final_Xpath竟然只会传递一次......你要是动了这个Final_Xpath，那就无法修改回来了
		Final_Xpath = response.meta.get('Final_Xpath',None)
		Some_Info = response.meta.get('Some_Info',None)
		
		if 'All_Xpath' not in Final_Xpath.keys():
				item = MusicSpiderItem()
				l = ItemLoader(item=item, response=response)
				for key in Final_Xpath.keys():
						item.fields[key] = Field()
						try:
								#itemloader在add_xxx方法找不到值的时候，会自动忽略这个字段，可是我不想忽略它，这时候需要将其置为空("")
								if map(lambda x:1 if x else 0, map(lambda x:response.xpath(x).extract() if x != "/" else "",Final_Xpath[key])) in [[0,0],[0]] and key != "site_name":		
										map(lambda x:l.add_value(key , ""),["just_one"])
								elif key == "site_name":
										map(lambda x:l.add_value(key , x),Final_Xpath[key])
								else:
										map(lambda x:l.add_xpath(key , x) if response.xpath(x).extract() != [] else "",Final_Xpath[key])
						except Exception,e:
								print Exception,":",e
				if Some_Info:
						for key in Some_Info.keys():
								item.fields[key] = Field()
								l.add_value(key , Some_Info[key])
				yield l.load_item()
		else:
		#感觉这里不能用itemloader的add_xxx方法了，因为要先找到一个页面所有含有目标item的块，再在每个块里面提取出单个item，itemloader的话是一次性直接全取出，add_xpath不能再细分了;;打算用add_value方法
				my_Final_Xpath = Final_Xpath.copy()
				All_Xpath = my_Final_Xpath['All_Xpath'].copy()
				del my_Final_Xpath['All_Xpath']
				all_xpath = All_Xpath['all_xpath']
				del All_Xpath['all_xpath']
				for i in response.xpath(all_xpath[0]):
						item = MusicSpiderItem()
						l = ItemLoader(item=item, response=response)
						#把All_Xpath中的数据提取出来
						for key in All_Xpath.keys():
								item.fields[key] = Field()
								try:
										#itemloader在add_xxx方法找不到值的时候，会自动忽略这个字段，可是我不想忽略它，这时候需要将其置为空("")
										if not map(lambda x:1 if x else 0, map(lambda x:response.xpath(x).extract() if x != "/" else "",All_Xpath[key])) in [[0,0],[0]]:
												map(lambda x:l.add_value(key , ""),["just_one"])
										else:
												map(lambda x:l.add_value(key, i.xpath(x).extract()) if i.xpath(x).extract() != [] else "",All_Xpath[key])
								except Exception,e:
										print Exception,",",e
						#将除了All_Xpath中的数据提取出来，像豆瓣就特别需要这种情况，一般下面的数据是（多次取得），All_Xpath中才是真正单条的数据
						for key in my_Final_Xpath.keys():
								item.fields[key] = Field()
								try:
										if map(lambda x:1 if x else 0, map(lambda x:response.xpath(x).extract() if x != "/" else "",Final_Xpath[key])) in [[0,0],[0]] and key != "site_name":
												map(lambda x:l.add_value(key , ""),["just_one"])
										elif key == "site_name":
												map(lambda x:l.add_value(key , x),my_Final_Xpath[key])
										else:
												map(lambda x:l.add_xpath(key , x) if response.xpath(x).extract() != [] else "",Final_Xpath[key])
								except Exception,e:
											print Exception,":",e
					
						if Some_Info:
								for key in Some_Info.keys():
									item.fields[key] = Field()
									l.add_value(key , Some_Info[key])
						yield l.load_item()
