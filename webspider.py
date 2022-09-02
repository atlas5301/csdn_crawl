from asyncio import exceptions
from urllib import response
import requests
import threading
import time
import random
from typing import Dict,Any,List
from bs4 import BeautifulSoup
import re
import json
import os, shutil, bs4

class mywebspider(object):
    write_lock = threading.Lock()
    known_url: set =set()
    known_list: List[str] =[]
    checked_url: set = set()
    unknown_url_cache: List = [] #both in mem and to file
    checked_url_cache: List = [] #to file
    failed_url: set = set()
    failed_url_cache: List = []#to file
    fail_log_path: str = "./fail_url.log"
    exception_log_path: str = "./exception.log"
    known_url_path: str = "./known_url.txt"
    checked_url_path: str = "./checked_url.txt"
    log_path: str = "./log.txt"
    
    page_config_path: str = "./configs/"
    web_page_path: str = "./pages/"
    page_failed_path: str = "./failed/"

    known_url_this_time = set()

    session = requests.Session()

    known_user_id: set = set()
    known_users: Dict[str,Dict] = {}

    def __init__(self) -> None:
        self.fail_log_path: str = "./fail_url.log"
        self.known_url_path: str = "./known_url.txt"
        self.checked_url_path: str = "./checked_url.txt"
        self.page_config_path: str = "./configs/"
        if not os.path.exists(self.page_config_path):
            os.makedirs(self.page_config_path)

        self.page_failed_path: str = "./failed/"
        if not os.path.exists(self.page_failed_path):
            os.makedirs(self.page_failed_path)     
           
        self.known_url = set()
        cnt = 0

        with open(self.checked_url_path, mode="a+") as tempfile:
            tempfile.seek(0,os.SEEK_SET)
            for line in tempfile:
                tmp = line.replace("\n","")
                print("*",end="")
                self.checked_url.add(tmp)
            print("")

        with open(self.fail_log_path, mode="a+") as tempfile:
            tempfile.seek(0,os.SEEK_SET)
            for line in tempfile:
                tmp = line.replace("\n","")
                print("*",end="")
                self.failed_url.add(tmp)
            print("")

        with open(self.known_url_path, mode="a+") as tempfile:
            tempfile.seek(0,os.SEEK_SET)
            for line in tempfile:
                tmp = line.replace("\n","")
                print("*",end="")
                cnt += 1
                self.known_url.add(tmp)
        print(cnt)
        time.sleep(2)
            
    def cache_flush(self):
        self.write_lock.acquire()
        tmpfile = open(self.known_url_path,mode='a')
        for i in self.unknown_url_cache:
            self.known_list.append(i)
            tmpfile.write(i)
            tmpfile.write("\n")
        tmpfile.close()
        self.unknown_url_cache = []

        tmpfile = open(self.checked_url_path,mode='a')
        for i in self.checked_url_cache:
            tmpfile.write(i)
            tmpfile.write("\n")
        tmpfile.close()
        self.checked_url_cache= []

        tmpfile = open(self.fail_log_path,mode='a')
        for i in self.failed_url_cache:
            tmpfile.write(i)
            tmpfile.write("\n")
        tmpfile.close()
        self.failed_url_cache = []
        self.write_lock.release()
        
    def is_known(self, url: str) -> bool:
        return (url in self.known_url)
    
    def auto_append(self, url: str):
        if self.is_known(url):
            pass
        else:
            self.known_url.add(url)
            self.unknown_url_cache.append(url)
        if not(url in self.known_url_this_time):
            self.known_url_this_time.add(url)

    def gen_header(self) -> Dict:
        return {'user-agent':'Googlebot'}

    def get_page_response(self, url: str):
        tmpdict: Dict = {}
        headers = self.gen_header()
        try:
            response = self.session.get(url= url,headers= headers, timeout= (5,20))

        except requests.exceptions.Timeout:
            tmpdict["signal"] = "timeout"

        except requests.exceptions.ConnectionError:
            tmpdict["signal"] = "connection_error"
                
        else:
            tmpdict["signal"] = "success"
            tmpdict["response"] = response

        return tmpdict

    def get_page(self, url: str):
        failed = False
        #print("now getting page~~~ ",end="",flush=True)
        for i in range(3):
            t = self.get_page_response(url)
            if t["signal"] == "success":
                #print("success!")
                return t
            if not failed:
                failed = True
                print("failed, retrying ",end="",flush= True)
            time.sleep(0.3+random.random())
            print(i+1,end=" ",flush= True)
        print("")
        return t

    def page_analysis(self):
        pass

    def url_auto_get_base(self, src_url: str):
        tmp = self.get_page(src_url)
        if tmp["signal"] == "success":
            data = json.loads(tmp["response"].text)
            try:
                recs = data["data"]["www-blog-recommend"]["info"]
            except KeyError:
                #print(data)
                with open(self.exception_log_path, mode="a+") as outfile:
                    json.dump(data,outfile)
                print('an exception occured')        
            else:
                for rec in recs:
                    #print(rec["extend"]["url"])
                    print("*",end="")
                    if not (rec["extend"]["url"] in self.known_url):
                        tmpdict = rec["extend"]
                        with open((self.page_config_path+str(rec["extend"]["product_id"])+".json"), mode="w") as outfile:
                            json.dump(tmpdict, outfile)
                    self.auto_append(rec["extend"]["url"])
            print(len(self.known_url))

    def url_auto_get(self, maxnum: int, src_url: str):
        t = -1
        tries = 1000
        curr_attemp = 0
        while len(self.known_url) < maxnum:
            if len(self.known_url_this_time)==t:
                tries-=1
                if (tries < 0):
                    break
            else:
                tries = 1000
                t = len(self.known_url_this_time)
            curr_attemp+=1
            print(str(curr_attemp)+" still have "+str(tries)+" ||| len this time is "+str(len(self.known_url_this_time)),end=" |||| ",flush=True)
            self.url_auto_get_base(src_url=src_url)
            self.cache_flush()
            time.sleep(3 + random.random()*3)

    def csdn_img_download(self, url: str, path: str, number: int):
        for i in range(10):
            try:
                pic = requests.get(url,headers = self.gen_header(),timeout= 6.0)
            except Exception:
                print("pic failed:")
                print(url)
                time.sleep(0.5+i+random.random()*2)
                self.failed_url_cache.append(url + " "+ str(i))
                self.cache_flush()
            else:
                filepattern = re.compile("(\.[\w]*)")
                name = path + str(number)+filepattern.findall(url)[-1]
                f = open(name,"wb")
                f.write(pic.content)
                f.close()
                print(str(number),end=" ",flush=True)
                break

    def csdn_page_download_base(self, config_dict: Dict, rootpath: str) -> str:
        url = config_dict["url"]
        tmpdict = self.get_page(url)
        result_dict = {}
        if tmpdict["signal"] == "success":
            html = tmpdict["response"]
            with open(rootpath + "main.html", mode="w") as htmlfile:
                htmlfile.write(html.text)
            
            soup = bs4.BeautifulSoup(html.text,features="lxml")

            #article
            context = soup.find("article")
            with open(rootpath + "article.html", mode="w") as t:
                try: 
                    for tmp in context.contents:
                        t.write(str(tmp))
                except Exception:
                    tmpdict["signal"] = "failed"
                else:
                    pass
            
            result_dict["title"] = config_dict["title"]
            result_dict["product_id"] = config_dict["product_id"]
            result_dict["csdnTag"] = config_dict["csdnTag"]
            result_dict["job"] = config_dict["job"]
            result_dict["user_name"] = config_dict["user_name"]
            result_dict["created_at"] = config_dict["created_at"]
            result_dict["nickname"] = config_dict["nickname"]
            result_dict["company"] = config_dict["company"]
            result_dict["views"] = config_dict["views"]
            result_dict["comments"] = config_dict["comments"]
            result_dict["user_days"] = config_dict["user_days"]
            result_dict["url"] = config_dict["url"]

            piclist = []
            try:
                imgs = context.find_all("img")
                for imgurl in imgs:
                    piclist.append(imgurl["src"])
            except Exception:
                tmpdict["signal"] = "failed"                

            self.checked_url_cache.append(str(config_dict["product_id"])+" "+str(len(piclist)))

            result_dict["picList"] = piclist
            tmpcnt = 0
            pictmpdict = {}
            threadlist: List[threading.Thread] = []
            print("downloading images: ",end="",flush=True)
            for tmp_url in piclist:
                pictmpdict[tmp_url] = tmpcnt
                tmpthread = threading.Thread(target=self.csdn_img_download,args=(tmp_url,rootpath,tmpcnt))
                tmpthread.start()
                threadlist.append(tmpthread)
                tmpcnt+=1
                time.sleep(0.2+random.random()*0.1)

            for t in threadlist:
                t.join()
            print("")

            result_dict["picdict"] = pictmpdict

            with open(rootpath + "config_simple.html", mode="w") as outfile:
                json.dump(result_dict, outfile)

        return tmpdict["signal"]
    
    def csdn_page_download(self, page_uid: str):
        if not os.path.exists(self.page_config_path + str(page_uid) + ".json"):
            return "file don't exist"
        if not os.path.exists(self.web_page_path):
            os.makedirs(self.web_page_path)
        if not os.path.exists(self.web_page_path+str(page_uid)):
            os.makedirs(self.web_page_path+str(page_uid))

        print("downloading "+page_uid)
        tmpdict : Dict
        with open(self.page_config_path + str(page_uid) + ".json",mode= "r") as page_conf:
            tmpdict = json.load(page_conf)
        
        result = self.csdn_page_download_base(tmpdict, self.web_page_path + "/" + str(page_uid) + "/")
        if result == "success":
            shutil.move(self.page_config_path + str(page_uid) + ".json",
            self.web_page_path + str(page_uid) + "/page.json")
        else:
            self.failed_url_cache.append(page_uid)
            self.cache_flush()
            shutil.move(self.page_config_path + str(page_uid) + ".json",
            self.page_failed_path + str(page_uid) + ".json")

        return result

    def download_all(self):
        files = os.listdir(self.page_config_path)
        for file in files:
            file = file.replace(".json","")
            self.csdn_page_download(file)
            self.cache_flush()
            time.sleep(1+random.random()*2)
        self.cache_flush()

    def csdnwebspider_single(url: str):
        pass

'''
tmp = mywebspider()
t=tmp.get_page("https://cms-api.csdn.net/v1/web_home/select_content?componentIds=www-blog-recommend")
if t["signal"] == "success":
    #pass
    print(t["response"].text)
'''

tmp = mywebspider()
tmp.url_auto_get(maxnum=2900, src_url="https://cms-api.csdn.net/v1/web_home/select_content?componentIds=www-blog-recommend")
time.sleep(2)
tmp.download_all()
for i in range(2500,6000,800):
    tmp.url_auto_get(maxnum=i, src_url="https://cms-api.csdn.net/v1/web_home/select_content?componentIds=www-blog-recommend")
    tmp.download_all()
