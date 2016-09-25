# -*- coding: utf-8 -*-
"""
Created on Fri Sep 16 15:08:38 2016

@author: asterios
"""

from bs4 import BeautifulSoup
import requests
from HTMLParser import HTMLParser
import pandas as pd

class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)

def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    text = s.get_data()
    text = text.replace('\xc2', ' ')
    text = text.replace('\xa0', ' ')
    text = text.replace('\n', ' ')    
    text = text.replace('\xe2\x80\x99', "'")
    text = text.replace('\xe2\x80\x98', "'")
    text = text.replace('\xe2\x80\x94', '-')
    text = text.replace('\xe2\x80\x93', '-')
    text = text.replace('\xe2\x80\x9c', '"')
    text = text.replace('\xe2\x80\x9d', '?')
    
    return ' '.join(text.split())
    

def parseArticle(article_url):
    page = requests.get(article_url, headers={'User-Agent':'Mozilla/5.0'})
    soup = BeautifulSoup(page.content)
    
    post_text = []
    
    #Retrieve main post
    main_post = soup.find("div", class_="expando-uninitialized").find("div", class_="md")
    remove_scripts = [x.extract() for x in main_post('script')]
    post_text.append(strip_tags(str(main_post)))
    
    #Retrieve comments
    comment_area = soup.find("div", class_="commentarea").find('div', class_='sitetable')
    for comment in comment_area.find_all("div", class_='comment'):
        
        try:
            actual_comment = comment.find('div', class_='entry').text
            actual_comment, _, _ = actual_comment.partition('\n\npermalinkembedsavereportgive')
            actual_comment, _, _ = actual_comment.partition('permalinkembedsaveparentreportgive')
            try:
                actual_comment = actual_comment.split('children')[1][1:]
            except:
                actual_comment = actual_comment.split('child')[1][1:]
            post_text.append(strip_tags(str(actual_comment)))
        except:
            pass
            
        #Retrieve comments' children
        comments_kids = comment.find('div', class_='child').text.split('children')[1:]
        for comments_kid in comments_kids:
            try:
                comments_kid, _, _ = comments_kid.partition('\n\npermalinkembedsaveparentreportgive')
                comments_kid, _, _ = comments_kid.partition('permalinkembedsaveparentreportgive')
                post_text.append(strip_tags(str(comments_kid[1:])))
            except:
                pass
    
    #Remove duplicates
    post_text = list(set(post_text))
    
    return post_text


def parsePage(page_url, article_texts):
    page = requests.get(page_url, headers={'User-Agent':'Mozilla/5.0'})
    soup = BeautifulSoup(page.content)
    
    main_part = soup.find("div", class_="sitetable")
    for article in main_part.find_all("div", class_='link'):
        article_url = 'https://www.reddit.com' + article['data-url']
        try:
            article_texts.extend(parseArticle(article_url))
        except:
            pass
        
    try:
        next_button = soup.find("span", class_="next-button").find('a')['href']
        return article_texts, next_button
    except:
        return article_texts, ''

if __name__ == "__main__":
    subreddits = ['datamining', 'statistics', 'datascience', 'machinelearning', 'dataanalysis']
    
    for subreddit in subreddits:
        print "Download data from " + subreddit + " subreddit"
        article_texts = []
        counter = 0
            
        init_url = 'https://www.reddit.com/r/' + subreddit + '/?count=1'
        article_texts, next_button = parsePage(init_url, article_texts)
        counter += 1
        print counter
        
        while next_button!='':
            article_texts, next_button = parsePage(next_button, article_texts)
            counter += 1
            print counter
            
            if counter%5 == 0:
                article_texts = list(set(article_texts))
                article_texts_df = pd.DataFrame(article_texts)
                print "Number of comments so far is " + str(article_texts_df.shape[0])
                article_texts_df.to_csv('reddit.com-' + subreddit + '.csv', index=False, header=False)
                
        article_texts = list(set(article_texts))
        article_texts_df = pd.DataFrame(article_texts)
        print "Number of comments so far is " + str(article_texts_df.shape[0])
        article_texts_df.to_csv('reddit.com-' + subreddit + '.csv', index=False, header=False)