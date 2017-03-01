#!coding=utf-8

from music_Spider import general_func
import re
import requests
import json

#接受五个参数，两个传递给Relative_to_Absolute（通用的跳转规则）；第三，四，五个决定特殊化定制
def R_2_A(index_url,url_tail,site_name,level,is_sege):
		if not is_sege:
				if level == 0:
						
						return general_func.Relative_to_Absolute(index_url,url_tail)
				elif level == 1:
						
						return general_func.Relative_to_Absolute(index_url,url_tail)
				elif level == 2:
						
						return general_func.Relative_to_Absolute(index_url,url_tail)
				elif level == 3:
						
						return general_func.Relative_to_Absolute(index_url,url_tail)
				elif level == 4:
						
						return general_func.Relative_to_Absolute(index_url,url_tail)
		else:
				if level == 0:

						return general_func.Relative_to_Absolute(index_url,url_tail)
				elif level == 1:
						
						return general_func.Relative_to_Absolute(index_url,url_tail)
				elif level == 2:
						
						return general_func.Relative_to_Absolute(index_url,url_tail)
				elif level == 3:
						
						return general_func.Relative_to_Absolute(index_url,url_tail)
				elif level == 4:
						
						return general_func.Relative_to_Absolute(index_url,url_tail)


#接受三个参数，一个传递给Except_For_PageNo（通用生成url规则）；第二三决定特殊化定制
#这个函数的作用是：
#		应对分页的情况，但存在分页，则会有一个类似标识 page_no 的参数，一般这个参数都是在url末尾 {直接使用Url_Generate即可，替换末尾的page_no}
#			如:http://www.xiami.com/artist/index/c/1/type/0/class/0/page/1
#			这时候，我们只需要按照规则构造这个page_no即可跳转到对应网页，那么这个函数的作用即是：构造 剥离出page_no的其他部分的url
#			如这里即是：http://www.xiami.com/artist/index/c/1/type/0/class/0/page/{page_no} 	之后给这个page_no赋值即可
#
#		若类似这种：www.youku.tv/1.html    -->   www.youku.tv/{page_no}.html
def U_G(index_url,site_name,level):
		if level == 0:
				if site_name == "xiami_album":
						return re.sub('(?<=r/)\d+',"{page}",index_url),index_url

				return general_func.Url_Generate(index_url),index_url
		elif level == 1:

				return general_func.Url_Generate(index_url),index_url
		elif level == 2:
				if site_name == "wangyiyun_album":
						index_url = index_url+"&offset=0"
						return re.sub("(\d+)$","{page}",index_url),index_url
				
				return general_func.Url_Generate(index_url),index_url
		elif level == 3:
				if site_name == "xiami_mv":
						index_url = index_url + "?type=all&page=1"
						return general_func.Url_Generate(index_url),index_url
				if site_name == "xiami_music":
						index_url = index_url + "?page=1"
						return general_func.Url_Generate(index_url),index_url

				return general_func.Url_Generate(index_url),index_url
		elif level == 4:
	
				return general_func.Url_Generate(index_url),index_url
		elif level == 5:
	
				return general_func.Url_Generate(index_url),index_url

#详情请到general_func中查看
def G_V_U(urls):
	return general_func.Get_Valid_Url(urls)

def C_U_V(url):
	return general_func.Check_Url_Valid(url)










