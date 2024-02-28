import csv
import json
import requests
from tqdm import tqdm

def load_urls():
    urls = []
    for split in ['train_temp', 'val_temp', 'test_temp']:
        print(split)
        with open(f'../dehydrated_data/{split}_urls.csv', 'r') as infile:
            urlreader = csv.DictReader(infile)
            for row in urlreader:
                urls.append((row['media_url_https'], row['tweetId'], row['media_id']))
    return urls

def download_thumbs(urls):
    datadir = '/trunk/datasets/nsrivats/test_rehydrate/'
    for url,tweet_id,media_id in tqdm(urls):
        img_data = requests.get(url).content
        with open(datadir + media_id + '.' + url.split('.')[-1], 'wb') as handler:
            handler.write(img_data)

def main():
    print('Reading in media URLs..')
    urls = load_urls()
    print('Downloading images...')
    download_thumbs(urls)

if __name__ == '__main__':
    main()
