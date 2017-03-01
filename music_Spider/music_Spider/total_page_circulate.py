#!coding=utf-8

from music_Spider import general_func


#这个文件：分离出所有需要计算站点实际page页面的模块，一个可扩展的if elif语句来实现

#这个是对所有的当前同级页面的总的page数的一个统计
#Total_page_circulate接受两个参数，前者是最大的数据数，后者是每一页面包含的数据数，相除得到最大页面的page_no
def T_P_C(site_name,max_pages,level):
		if level == 0:
				
				return general_func.Total_page_circulate(max_pages,1)
		elif level == 1:
				if site_name == "xiami_album":
						return general_func.Total_page_circulate(max_pages,30)
				if site_name == "xiami_music":
						return general_func.Total_page_circulate(max_pages,30)
				
				return general_func.Total_page_circulate(max_pages,1)
		elif level == 2:
				
				return general_func.Total_page_circulate(max_pages,1)
		elif level == 3:
				if site_name == "xiami_album":
						return general_func.Total_page_circulate(max_pages,9)
				if site_name == "xiami_mv":
						return general_func.Total_page_circulate(max_pages,20)
				if site_name == "xiami_music":
						return general_func.Total_page_circulate(max_pages,20)

				return general_func.Total_page_circulate(max_pages,1)
		elif level == 4:
				
				return general_func.Total_page_circulate(max_pages,1)
		elif level == 5:
				
				return general_func.Total_page_circulate(max_pages,1)


#指定当前层你指定的page数，像豆瓣，即没有最大页数，无法自动切分分页，手动指定
def T_P_B(site_name,level):
		if level == 0:
				if site_name == "douban_movie":
						return 17
				raise ValueError("level0 没有最大页数，请手动指定一个值!!!")
		elif level == 1:
				
				raise ValueError("level1 没有最大页数，请手动指定一个值!!!")
		elif level == 2:
				
				raise ValueError("level2 没有最大页数，请手动指定一个值!!!")
		elif level == 3:
				if site_name == "xiami_music":
						return 1

				raise ValueError("level3 没有最大页数，请手动指定一个值!!!")
		elif level == 4:
				
				raise ValueError("level4 没有最大页数，请手动指定一个值!!!")
		elif level == 5:
			
				raise ValueError("level5 没有最大页数，请手动指定一个值!!!")
				
#有些网页的page_no格式不一样
#比如:www.example.net/xxx/xxxx/page_no=0
#	  www.example.net/xxx/xxxx/page_no=20
#	  www.example.net/xxx/xxxx/page_no=40
#这时候还需要对page_no作处理
#Turn_True_Page，作类似 1 ->> 1*page_num 这样的改变，默认是1，指定一页包含的page_num即可
def T_T_P(page_no,site_name,level):
		if level == 0:

				return general_func.Turn_True_Page(page_no,1)
		elif level == 1:

				return general_func.Turn_True_Page(page_no,1)
		elif level == 2:
				if site_name == "wangyiyun_album":
						return general_func.Turn_True_Page(page_no,12)

				return general_func.Turn_True_Page(page_no,1)
		elif level == 3:

				return general_func.Turn_True_Page(page_no,1)
		elif level == 4:

				return general_func.Turn_True_Page(page_no,1)
		elif level == 5:

				return general_func.Turn_True_Page(page_no,1)
				










