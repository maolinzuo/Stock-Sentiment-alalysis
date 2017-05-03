
import signal
import datetime
import time as tm
from afinn import Afinn
from selenium import webdriver
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary

# the directory should be where Gecko Driver lies
driver = webdriver.Firefox(executable_path='/Users/maolinzuo/Desktop/web-spider/geckodriver')

afinn = Afinn()


class TimeOutException(Exception):
	print 'Time-out'

def timeout_handler(signum, frame):
	raise TimeOutException

def get_news_sentiment(url, date):

	try:
		# browse the website
		driver.get(url)
		news = ""
		# find the web elements where text lies
		paragraphs = driver.find_elements_by_tag_name('p')
		for p in paragraphs:
			# get the text in that element
			news = news + p.text.encode('UTF-8')
		# get the sentiment of the news text by Afinn
		sc = int(afinn.score(news))
		print sc
		return sc
	except:
		return 0

if __name__ == '__main__':
	#
	# Topic is indicated here!
	topic = "apple"
	#
	#
	# input start_date of news below
	start_time = datetime.date(2016,3,25)
	#
	#

	interval = datetime.timedelta(days = 1)
	end_time = start_time + interval
	start_second = tm.mktime(start_time.timetuple())
	end_second = tm.mktime(end_time.timetuple())

	f = open(topic + '_news_sentiment_April.txt', 'w')
	#
	# input end_date of news below
	while start_time < datetime.date(2016, 4, 9):
	#
	#
		pos_score = 0
		neg_score = 0
		# query url
		url = 'https://hn.algolia.com/?query='+topic+'&sort=byPopularity&prefix&page=0&dateRange=custom&type=story&dateStart='+ str(start_second) + '&dateEnd=' + str(end_second)
		driver.get(url)
		tm.sleep(0.5)
		num = 1
		href_links = []

		# find the all the links in that date
		while True:
			try:
				href_element = driver.find_element_by_xpath('//*[@id="main"]/div/section/section/section/div['+str(num)+']/div[1]/div[1]/div[1]/ul/li[5]/a')
				href_links.append(href_element.get_attribute('href'))
				num += 1
			except:
				break
		
		# browse the websites got above and get the sentiments
		for href in href_links:
			signal.signal(signal.SIGALRM, timeout_handler)
			signal.alarm(15)
			try:
				score = get_news_sentiment(href, start_time)
			except TimeOutException:
				continue
			if score < 0:
				neg_score += score
			else:
				pos_score += score

		f.write(str(start_time) + ' ' + str(pos_score/len(href_links)) + ' ' + str(neg_score/len(href_links)) + '\n')
		

		start_time += interval
		start_second += 86400
		end_second += 86400

	# for link in links:
	# 	get_news_sentiment(link)

	# file_name = 'news.txt'
	# f = open(file_name,'w')
	# news_list = driver.find_element_by_id('article-text')
	# news_text_list = news_list.find_elements_by_tag_name('p')
	# for li in news_text_list:
	# 	news = news + li.text.encode('utf-8')
	# f.write(news)





