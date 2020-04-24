from bs4 import BeautifulSoup
import requests
import pandas as pd
from multiprocessing import Pool
import time
import random

categories = ["economy","finance", "health-and-science", "media", "real-estate", "energy", "transportation", "industrials", "retail", "wealth", "small-business", # business
"invest-in-you", "personal-finance", "financial-advisors", "options-action", "etf-street", "earnings", "trader-talk", # investing
"cybersecurity", "enterprise", "internet", "mobile", "social-media", "venture-capital", "tech-guide",  # tech
"white-house", "policy","defense","congress","2020-elections","europe-politics","china-politics", "asia-politics", "world-politics"]    # politics
# 35 categories
# except: buffett archive

class crawler:
    def __init__(self, category_subset_list):
        # change user agent
        self.headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit 537.36 (KHTML, like Gecko) Chrome",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
            }
        self.category_subset_list = category_subset_list

    def _rand_sleep(self, max_time):
        sleep_time = random.randint(1, max_time)
        time.sleep(sleep_time)

    def _set_crawling(self, category_subset):
        for category in category_subset:
            res_dicts = []
            while True:
                try:
                    for page in range(1,10):
                        # url collect
                        strart_url = "https://www.cnbc.com/"+category+"/?page="+str(page)
                        html = requests.get(strart_url, headers=self.headers).content
                        soup = BeautifulSoup(html, "html.parser")
                        urls =[tag.get("href") for tag in soup.find_all("a", class_ = "Card-title")]
                        if not urls:    break

                        # parse data
                        self._parse(category, urls, res_dicts)
                        self._rand_sleep(10)
                    break
                
                except:
                    print("ZZzzzz...")
                    self._rand_sleep(20)
                    continue

            # 1 category complete
            self._write_csv(category, res_dicts)
            self._rand_sleep(30)

    # one category, parse
    def _parse(self, category, urls, res_dicts):
        i = 0
        for url in urls:
            i+=1
            print(category+" now...", str(i)+"/"+str(len(urls)), "\nurl:", url,)

            body = ""
            key_point = ""

            news_html = requests.get(url, headers=self.headers).content
            news_soup = BeautifulSoup(news_html, "html.parser")
            
            bsKeyPoints = news_soup.select(".RenderKeyPoints-list li")
            for bs_key_point in bsKeyPoints:
                key_point+=bs_key_point.text

            bs_bodys = news_soup.select(".ArticleBody-articleBody>.group>p")
            for bs_body in bs_bodys:
                body+=bs_body.text

            news_dict = {"key_point":key_point, "body": body, "category":category, "url":url}
            res_dicts.append(news_dict)
            self._rand_sleep(5)


    def _write_csv(self, category, res_dicts):
        news_table = pd.DataFrame(res_dicts)
        path = "/Users/seungyoungoh/workspace/data_capstone/data/"
        news_table.to_csv(path+category+".csv", mode='a')
        time.sleep(30)
        print(category, "done")


    def run(self):
        with Pool(processes = 4) as pool:
            pool.map(self._set_crawling, self.category_subset_list)
    #            if input("continue?\t 1: Y, 2: N") != '1': break

if __name__ == "__main__":
    max_time_end = time.time()+(60*300)

    allocate = len(categories)//4
    remainder = len(categories)%allocate
    category_subset_list = [categories[i*allocate:(i+1)*allocate] for i in range(4)]    # 4분할
    category_subset_list[3]+=(categories[allocate*4:])

    c = crawler(category_subset_list)
    c.run()
    print("crawling done!")


