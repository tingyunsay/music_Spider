#!coding=utf-8

import re
import requests
import json

#本文件存储一些通用的函数


#相对url转绝对url，假定将从网页中提取的不是http开头的，全部替换成从Index_Url中的第一个/前面部分+网页中提取到的部分					
#将穿进来的url统一转换成list返回
def Relative_to_Absolute(index_url,url_tail):
		head_url = re.search(r'(.+//).+?/',index_url).group()
		if type(url_tail) is list and len(url_tail) > 0:
				res_urls = []
				if re.search(r'^//',url_tail[0]):
						for url in url_tail:
								res_urls.append(re.sub(r'^//',"http://",url))
						return res_urls
				elif (not re.search(r'^http://',url_tail[0])) and (not re.search(r'^https://',url_tail[0])):
						if re.search(r'^/',url_tail[0]):
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
				if (not re.search(r'^http://',''.join(url_tail))) and (not re.search(r'^https://',''.join(url_tail))):
						if re.search(r'^/',''.join(url_tail)):
								return [head_url + re.sub(r'^/',"",''.join(url_tail))]
						else:
								return [head_url + ''.join(url_tail)]
				else:
						return [''.join(url_tail)]
				



#这个函数的作用：
#	进来一个urls，为list时，取第一个，作如下判断，如果不是正确url，全部格式更换之后返回一个正确的url list
#				  为str时，判断其是否是一个有效的链接，youku有这样的link：//http://www.youku.com/.....，不是正确的url，需要格式化
#这个暂时还只是遇见如上的情况，如果有其他的情况，进行补充
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

#这个函数的作用：
#	应对通常的分页的情况，当存在分页，则会有一个类似标识 page_no 的参数，一般这个参数都是在url末尾 {直接使用Url_Generate即可，替换末尾的page_no}
#	其他情况在UG函数中重写
def Url_Generate(index_url):
		return re.sub("(\d+)$","{page}",index_url)


#遇到其他非法的url，能再在此补充这个if
def Check_Url_Valid(url):
	if re.search('com/$',url):
			return False
	else:
			return True


#-----------------------------total_page_circulate


def Turn_True_Page(page_no,page_num):
		return (page_no)*page_num


#两种情况计算max_pages：1.直接从页面拿取，返回原值,one_page为1时即这种情况	2.经过计算，除 单个页面元素的值，返回这个值+1，表明有多少页
def Total_page_circulate(max_pages,one_page):
		if one_page == 1:
				return max_pages
		return (max_pages/one_page) + 1





