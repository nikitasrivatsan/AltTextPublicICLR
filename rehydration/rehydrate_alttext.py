from requests_oauthlib import OAuth1Session
import csv
import pandas as pd
import json
import sys
import time
from typing import List, Generator
import os

# pip install python-twitter, pandas

def hydrate_tweets_generator(
    tweet_ids: List[int],
    # how many tweets in a batch
    num_tweets_per_batch: int = 100
  ):
  assert num_tweets_per_batch <= 100
  # identify the tweets already received
  retrieved = set()
  if os.path.exists('retrieved_ids.txt'):
      with open('retrieved_ids.txt', 'r') as infile:
        for line in infile:
            retrieved.add(line.strip())
  idx = 0
  oauth = get_oauth()
  with open('retrieved_ids.txt', 'w') as outfile:
      for tweet_id in retrieved:
        outfile.write(f'{tweet_id}\n')
      while idx < len(tweet_ids):
        print(idx, len(tweet_ids))
        # attempt to get tweets at least five times
        for i in range(5):
            try:
                start, end = idx, min(len(tweet_ids), idx + num_tweets_per_batch)
                # API Call will sleep if it would trigger losing quota, so sometimes, this call will magically sleep
                #batch = [tweet_id for tweet_id in tweet_ids[start:end] if tweet_id not in retrieved]
                batch = tweet_ids[start:end]
                statuses = get_statuses(batch, oauth)
                # write the retrieved ids to file
                for tweet_id in batch:
                    retrieved.add(tweet_id)
                    outfile.write(f'{tweet_id}\n')
                yield statuses
                idx = end    
                break
            except Exception as e:
                print(f'Error at {i} - {e}')

def get_oauth():
    oauth = OAuth1Session(
        client_key="HcjdvNpDT6RfbE1k0XbfdeVn4",
        client_secret="3MhTeIdNTufT6Pef2Xmh1AjVw8skd3GujljdrJGi6z2iiKldGg",
        resource_owner_key="874264893108568064-pAETzxgEC0DnmXOY1JmB73KbL3JHmc8",
        resource_owner_secret="IypwJjeRuY21wV4MW6tiwjNcsnqUULBeY1UASzgLIVKfX"
    )
    return oauth

def get_statuses(batch, oauth):
    params = {
        'tweet.fields' : 'id,text,possibly_sensitive,attachments',
        'media.fields' : 'media_key,type,url,alt_text',
        'expansions' : 'attachments.media_keys',
        'ids' : ','.join(batch)
    }
    response = oauth.get('https://api.twitter.com/2/tweets', params=params).json()
    return response

def read_tweet_ids(split):
    # get the tweet ids from the json file
    tweet_ids = []
    infilename = f'../dehydrated_data/{split}_temp_urls.csv'
    with open(infilename, 'r') as infile:
        csvreader = csv.DictReader(infile)
        for line in csvreader:
            tweet_ids.append(line['tweetId'])
    return tweet_ids

if __name__ == '__main__':
    # Takes one arguments, the path to the JSON API creds file
    # This is intendend just to test the API, I assume folks 
    # will copy this function and modify to their needs
    for split in ['train', 'val', 'test']:
        tweet_ids = read_tweet_ids(split)
        outfilename = f'../data/{split}_temp.csv'
        with open(outfilename, 'w') as outfile:
            csvwriter = csv.writer(outfile)
            csvwriter.writerow(['tweetId', 'text', 'media_id', 'alt_text', 'media_url_https'])
            for tweet_batch in hydrate_tweets_generator(tweet_ids, num_tweets_per_batch=1):
                if 'includes' not in tweet_batch:
                    print(tweet_batch)
                    print('Skipping...')
                    if 'title' in tweet_batch and tweet_batch['title'] == 'Too Many Requests':
                        print('Sleeping for 15min...')
                        time.sleep(15 * 60)
                    continue
                for m in tweet_batch['includes']['media']:
                    assert len(tweet_batch['data']) == 1
                    if 'alt_text' not in m:
                        print('Alt text not found...')
                        continue
                    t = tweet_batch['data'][0]
                    csvwriter.writerow([t['id'],t['text'],m['media_key'],m['alt_text'],m['url']])
                '''
                for t,m in zip(tweet_batch['data'], tweet_batch['includes']['media']):
                    print(t, m)
                for tweet_json in [t.AsJsonString() for t in tweet_batch]:
                    outfile.write(tweet_json + '\n')
                '''
