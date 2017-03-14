import pandas as pd
import numpy as np
import os
import urllib2
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
import requests
import time
import re
import ast
import itertools
from  more_itertools import unique_everseen
import matplotlib as plt
import json
import sys
import random

class web_scrap(object):

    def __init__(self):

        # All links from Wayfair.com
        self.all_links = []

        # Links from each product category
        self.product_list = []
        self.review_listing = []
        self.review_list= []
        self.sku_sitemap_list = []
        self.redirected_url_list=[]
        self.complete_sku_list=[]
        # Dictionary for products
        self.product = {}

        # Product URLs that got caught in exception(error)
        self.exception_list = []

        # Stores dates
        self.dates_list = []


    def link_scanner(self):
        """ Scans the Wayfair Sitemap and extracts each product URL. """
        sitemap = "https://www.wayfair.com/sitemap.xml"

        req = urllib2.Request(sitemap, headers={ 'User-Agent': 'Mozilla/5.0' })
        time.sleep(2.1)
        html = urllib2.urlopen(req).read()


        soup = BeautifulSoup(html, 'lxml')
        self.all_links = [each.text for each in soup.find_all("loc")]
        del self.all_links[-2:]
        print "Total links from sitemap: ",  len(self.all_links)
        #print self.all_links
        time.sleep(1.25)

        count = 1
        for each in self.all_links:
            product_list = []

            req = urllib2.Request(each, headers={ 'User-Agent': 'Mozilla/5.0' })
            print "Sleeping..."
            time.sleep(2.1)
            print "Connecting to link: %i" % count
            count +=1
            html = urllib2.urlopen(req).read()
            soup = BeautifulSoup(html, 'lxml')
            self.product_list = [each.text for each in soup.find_all("loc")]

        #print self.products
        print "@@@ Product links are collected."
        print "@@@ Total Product Count: " , len(self.product_list)
        with open("product_list.txt", "w") as text_file:
            text_file.write(str(self.product_list))

    def customer_reviews(self):
        """ Scraps every customer review by date from each product that Wayfair offers. """
        print "@@@ Scanning customer reviews..."
        with open('entire_exceptions.txt') as f:
            lines = f.read().splitlines()
            for line in lines:
                data=ast.literal_eval(line)

        print "Number of links: " , len(data)

        # Randomize User-Agent and IP Address not to get blacklisted.
        ua = UserAgent()
        proxy = {"https":"https://username:p3ssw0rd@10.10.1.10:3128"}


        count = 0
        for product in data[2800:]:
            count +=1
            self.product["URL"] = product
            print "Product No: " , count

            try:
                sku = product.split("-")[-1].strip(".html")
                self.product["SKU"] = sku
                print "Product SKU: ", sku
            except Exception as e:
                print "Error ocurred in URL Parsing: " + str(e) + str(product)
                self.exception_list.append(product)

            print "Connecting: %s" % str(product)
            req = urllib2.Request(product, headers={ 'User-Agent': str(ua.safari)})
            print "Sleeping..."
            time.sleep(2.6)
            try:
                html = urllib2.urlopen(req).read()
            except Exception as e:
                if product not in self.exception_list:
                    self.exception_list.append(product)
                print "Error ocurred: " + str(e)
            else:
                print "Connection established."

            soup = BeautifulSoup(html, 'lxml')

            # Validate if URL is a product or intermediary page.
            spans = soup.find_all('span', {'class' : 'ProductDetailBreadcrumbs-item--product'})
            for tag in spans:
                tag = str(tag)
                if "SKU: " in tag:
                    print "URL is product page: " , product
                else:
                    print "URL is an intermediary page: " , product
                    if product not in self.exception_list:
                            self.exception_list.append(product)
            if not spans:
                print "**URL is an intermediary page: " , product
                if product not in self.exception_list:
                    self.exception_list.append(product)

            ## ----------------------------------##
            # Loops through review pages to get every single review.
            page_number = 0
            while True:
                page_number +=1
                try:
                    print "Page Number: " , page_number
                    json_url= "https://www.wayfair.com/a/product_review_page/get_update_reviews_json?_format=json&product_sku=" + "{}&page_number={}&sort_order=relevance&filter_rating=&filter_tag=&item_per_page=5&is_nova=true&has_customer_photos=true&_txid=otAgcVhr7DJA3qoyhQCAAg%3D%3D".format(sku,page_number)
                    req = urllib2.Request(json_url, headers={ 'User-Agent': str(ua.safari)})
                    time.sleep(2.42)
                    html = urllib2.urlopen(req).read()
                    json_data = json.loads(html)
                    for review in json_data['reviews']:
                        print review['date']
                        self.product['date'] = review['date']
                        self.dates_list.append(review['date'])
                except Exception as e:
                    print "Error in JSON URL: time-out" + str(e)
                    if product not in self.exception_list:
                        self.exception_list.append(product)
                    #with open("exception_JSON_list.txt", "w") as text_file:
                    #    text_file.write(str(self.exception_list))
                    #print "EXITING SCRIPT DUE TO OVER-TIME."
                    #sys.exit()

                    break

                if json_data["has_reviews"] is False:
                    break
                else:
                    continue

        with open("reviews_updated.txt", "w") as text_file:
            text_file.write(str(self.dates_list))
        with open("exception_list.txt", "w") as text_file:
            text_file.write(str(self.exception_list))

        print self.product
# ------------------------------------------------------------------------------#
    def exceptions(self):
        """ Extracts dates from intermediary and exception pages. """
        with open('sku_list.txt') as f:
            lines = f.read().splitlines()
            for line in lines:
                collected_sku=ast.literal_eval(line)



        all_exceptions = []

        current_directory = os.path.dirname(os.path.abspath('__file__'))
        for files in os.listdir(current_directory + '/exceptions'):
            print files
            if files.endswith('.txt'):
                with open(current_directory + '/' + 'exceptions/' + files) as f:
                    lines = f.read().splitlines()
                    for line in lines:
                        data=ast.literal_eval(line)
                        all_exceptions.append(data)

        with open('entire_exceptions.txt') as f:
            lines = f.read().splitlines()
            for line in lines:
                all_exceptions=ast.literal_eval(line)

        remove_empty = [x for x in all_exceptions if x != []]
        links = list(itertools.chain.from_iterable(remove_empty))

        for link in links:
            r = requests.get(link)
            redirected_url = r.url.split('www')[-1].replace('%2F' , '/').replace('%3F' , '/').replace('').replace('%3' , '=').strip('&px=1')
            redirected_url = 'https://www.' + redirected_url + '?curpage=1'
            self.redirected_url_list.append(redirected_url)
            # To eliminate double-counting of re-directed URLs
            if self.redirected_url_list.count(redirected_url) < 2:

                req = urllib2.Request(redirected_url, headers={ 'User-Agent': str(ua.safari) })
                time.sleep(1.78)
                html = urllib2.urlopen(req).read()
                soup = BeautifulSoup(html, 'lxml')
                spans = soup.find_all('span', {'class' : 'ProductDetailBreadcrumbs-item--product'})
                if not spans: # Ratifies link is intermediary page.
                    sku_list = [each['data-sku'] for each in soup.find_all('div', {'class' : 'SbProductBlock-image-wrapper'})]
                    # Delete already scanned SKU
                    sku_list = list(set(sku_list) - set(collected_sku))
                    if len(sku_list) != 0:
                        #productbox_TMAR7666 > div.SbProductBlock-image-wrapper.js-clipbar_drag
                        for sku in sku_list:
                        # Loops through review pages to get every single review.
                            page_number = 0
                            while True:
                                page_number +=1
                                try:
                                    print "Page Number: " , page_number
                                    json_url= "https://www.wayfair.com/a/product_review_page/get_update_reviews_json?_format=json&product_sku=" + "{}&page_number={}&sort_order=relevance&filter_rating=&filter_tag=&item_per_page=5&is_nova=true&has_customer_photos=true&_txid=otAgcVhr7DJA3qoyhQCAAg%3D%3D".format(sku,page_number)
                                    req = urllib2.Request(json_url, headers={ 'User-Agent': str(ua.safari)})
                                    time.sleep(2.42)
                                    html = urllib2.urlopen(req).read()
                                    json_data = json.loads(html)
                                    for review in json_data['reviews']:
                                        print review['date']
                                        self.product['date'] = review['date']
                                        self.dates_list.append(review['date'])
                                except Exception as e:
                                    print "Error in JSON URL: time-out" + str(e)
                                    break

                                if json_data["has_reviews"] is False:
                                    break
                                else:
                                    continue

        with open("reviews_updated.txt", "w") as text_file:
            text_file.write(str(self.dates_list))
# ------------------------------------------------------------------------------#

    def intermediary_page(self):

        ua = UserAgent()

        with open('sku_list.txt') as f:
            lines = f.read().splitlines()
            for line in lines:
                collected_sku=ast.literal_eval(line)


        with open('entire_exceptions.txt') as f:
            lines = f.read().splitlines()
            for line in lines:
                all_exceptions=ast.literal_eval(line)
        link_count=0
        for link in all_exceptions:
            print "@@@ ORIGINAL LINK: " , link
            link_count+=1
            print link_count
            r = requests.get(link)
            time.sleep(1.1)
            try:
                redirected_url = r.url.split('www')[-1].replace('%2F' , '/').replace('%3F' , '/').replace('%3' , '=').strip('&px=1')
                redirected_url = 'https://www' + redirected_url
                print "@@@ REDIRECTED URL: " , redirected_url
                self.redirected_url_list.append(redirected_url)
            except Exception as e:
                print "Error in redirected_url: " + str(e)
                break

            if self.redirected_url_list.count(redirected_url) < 2: # To eliminate double-counting of products
                page_number=0
                empty_cut_off=0
                while True:
                    page_number +=1
                    print "Intermediary Page Number: " , page_number
                    redirected_url_ = redirected_url + '?curpage={}'.format(page_number)
                    print redirected_url_
                    req = urllib2.Request(redirected_url_, headers={ 'User-Agent': str(ua.safari) })
                    time.sleep(1.78)
                    try:
                        html = urllib2.urlopen(req).read()
                    except Exception as e:
                        print "HTTP Error 404: " + str(e)
                        break
                    soup = BeautifulSoup(html, 'lxml')
                    spans = soup.find_all('span', {'class' : 'ProductDetailBreadcrumbs-item--product'})
                    if not spans: # Ratifies link is intermediary page.
                        print "Link is intermediary page."
                        sku_list = [each['data-sku'] for each in soup.find_all('div', {'class' : 'SbProductBlock-image-wrapper'})]
                        if not sku_list:
                            print "Error ocurred."
                        # Delete already scanned SKU
                        sku_list = list(set(sku_list) - set(collected_sku))
                        print sku_list

                        if not sku_list:
                            empty_cut_off +=1
                            print "Empty: " , empty_cut_off
                        if sku_list not in self.complete_sku_list:
                            self.complete_sku_list.append(sku_list)
                        else:
                            break
                        no_more_next = soup.find_all('span', {'class' : 'Pagination-item is-inactive Pagination-icon--next js-next-page'})

                        if len(no_more_next) != 0 :
                            print "No more pagination."
                            break

                    else:
                        print "Page is a product."
                        break

                    if empty_cut_off > 10:
                        print empty_cut_off
                        break

        with open("complete_sku_list_5.txt", "w") as text_file:
            text_file.write(str(self.complete_sku_list))


    def review_pagination(self):
        ua = UserAgent()
        with open('entire_skus_together.txt') as f:
            lines = f.read().splitlines()
            for line in lines:
                data=ast.literal_eval(line)
        print "Total Number of SKUs: " , len(data)
        product_number = 0
        for sku in data:
            product_number += 1
            print "Scanning: " , product_number
            page_number = 0
            proxy_list = ['12.129.82.194:8080'] #,'97.77.104.22:3128','70.248.28.23:800']
            print "SKU : " , sku
            while True:
                page_number += 1

                try:
                    current_proxy = random.choice(proxy_list)
                    print "Page Number: " , page_number
                    json_url= "https://www.wayfair.com/a/product_review_page/get_update_reviews_json?_format=json&product_sku=" + "{}&page_number={}&sort_order=relevance&filter_rating=&filter_tag=&item_per_page=5&is_nova=true&has_customer_photos=true&_txid=otAgcVhr7DJA3qoyhQCAAg%3D%3D".format(sku,page_number)
                    proxy = urllib2.ProxyHandler({'http': current_proxy, 'https': current_proxy})
                    opener = urllib2.build_opener(proxy)
                    urllib2.install_opener(opener)
                    req = urllib2.Request(json_url, headers={ 'User-Agent': str(ua.safari)})
                    time.sleep(2.40)
                    html = urllib2.urlopen(req).read()
                    json_data = json.loads(html)
                    #if json_data['captcha'] = 1:
                    if not json_data['reviews']:
                        print "No more reviews"
                    for review in json_data['reviews']:
                        print review['date']
                        self.product['date'] = review['date']
                        self.dates_list.append(review['date'])
                except Exception as e:
                    print "Error in JSON URL: Captcha - " + str(e)
                    if sku not in self.exception_list:
                        self.exception_list.append(sku)
                    #sys.exit("Captcha error ocurred. Program is stopped.")
                    break

                if json_data["has_reviews"] is False:
                    break
                else:
                    continue


        with open("reviews_intermediary.txt", "w") as text_file:
            text_file.write(str(self.dates_list))
        with open("exception_list.txt", "w") as text_file:
            text_file.write(str(self.exception_list))




    def data_frame(self):
        """ Formats the dataframe and prepares for data extraction. """
        all_dates = []

        current_directory = os.path.dirname(os.path.abspath('__file__'))
        for files in os.listdir(current_directory + '/reviews'):
            if files.endswith('.txt'):
                with open(current_directory + '/' + 'reviews/' + files) as f:
                    lines = f.read().splitlines()
                    for line in lines:
                        data=ast.literal_eval(line)
                        all_dates.append(data)

        remove_empty = [x for x in all_dates if x != []]
        dates = list(itertools.chain.from_iterable(remove_empty))
        print len(dates)
        df = pd.DataFrame({'review_dates': dates})
        frequency=df['review_dates'].value_counts()
        #frequency.to_csv('total_reviews.csv')
        reviews=pd.read_csv('total_reviews.csv',parse_dates=True)
        GB=reviews.groupby([(reviews.index.year),(reviews.index.month)]).sum()
        print GB

analyzer=web_scrap()
analyzer.link_scanner()
analyzer.customer_reviews()
analyzer.intermediary_page()  #---> Implemented in addition to extract product URLs from intermediary pages
analyzer.review_pagination()  #---> Implemented pagination of customer review pages for each product based on SKU numbers.
analyzer.exceptions()
analyzer.data_frame()
