#!/usr/bin/env python
# -*- coding:utf-8 -*-  
import sys,os
import re
#os.system("docker run -d -it -p 8050:8050 scrapinghub/splash --max-timeout 3600")

splash_image_id = ""

os.system("docker ps -a| grep tingyun > temp.txt")
with open('temp.txt','r') as f:
	temp = f.read()
	try:
		splash_image_id = re.search('.{12}',temp).group().replace("/","")
	except Exception,e:
		print Exception,":",e

#print "当前的splash id是 : ",splash_image_id,"\n"

if not splash_image_id:
		status_code = os.system("docker run -it -d -p 8050:8050 --name tingyun scrapinghub/splash")
		if status_code == 0:
				print "Splash not exists , start success , and named tingyun....."
		else:
				print "Error1 , fail!!!"
else:
		status_code = os.system("docker restart %s"%splash_image_id)
		if status_code == 0:
				print "Splash exists , restart tingyun success ....."
		else:
				print "Error2 , fail!!!"
		
temp_file = './temp.txt'

if os.path.exists(temp_file):
		os.remove(temp_file)
		print "Delete temp_file success ....."








