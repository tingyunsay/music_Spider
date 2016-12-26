#!coding=utf-8

import re

#相对url转绝对url，假定将从网页中提取的不是http开头的，全部替换成从Index_Url中的第一个/前面部分+网页中提取到的部分					
#将穿进来的url统一转换成list返回
def Relative_to_Absolute(index_url,url_tail):
	#把http://..../匹配出来
	#print "the url_tail 2222 is %s \n"%url_tail
	#print "the index_url is 222222 %s \n"%index_url
	head_url = re.search(r'(.+//).+?/',index_url).group()
	if type(url_tail) is list and len(url_tail) > 0:
		res_urls = []
		if re.search(r'^//',url_tail[0]):
			for url in url_tail:
				res_urls.append(re.sub(r'^//',"http://",url))
			return res_urls
		elif (not re.search(r'^http://',url_tail[0])) and (not re.search(r'^https://',url_tail[0])):
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
		if (not re.search(r'^http://',''.join(url_tail))) and (not re.search(r'^https://',''.join(url_tail))):
			if re.search(r'^/',''.join(url_tail)):
				return [head_url + re.sub(r'^/',"",''.join(url_tail))]
			else:
				return [head_url + ''.join(url_tail)]
		else:
			return [''.join(url_tail)]

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
			

def get_HeadUrl(index_url,spider_name):
	if spider_name == "aiqiyi_mv":
			return re.sub(r"30","{page}",index_url)
	if spider_name == "tudou":
			index_url = "http://www.tudou.com/list/albumData.action?tagType=3&firstTagId=12&sort=1&page=1"
			return re.sub("(\d+)$","{page}",index_url)
	else:
			return re.sub("(\d+)$","{page}",index_url)
	

#这两者是letv特有的，它们页面间的信息只支持json传输，而传输的json信息中却没有直接的link，而是到了页面通过一个js函数，用传递的参数去构造一个个url（我只能找到js函数的操作，然后自己去构造这些url）
def get_letv_url(sid_list):
	url = "http://search.lekan.letv.com/lekan/apisearch_json.so?from=pc&ps=14&stype=1&cg=9&leIds={star_id}&pn=1"
	res_urls = []
	try:
			for sid in sid_list:
					res_urls.append(url.format(star_id=sid))
	except Exception,e:
			print Exception,":",e
	return res_urls

def get_letv_url2(vid_list):
	url = "http://www.le.com/ptv/vplay/{video_id}.html"
	res_urls = []
	try:
			for sid in vid_list:
					res_urls.append(url.format(video_id=sid))
	except Exception,e:
			print Exception,":",e
	return res_urls











