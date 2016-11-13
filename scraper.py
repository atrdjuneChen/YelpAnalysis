# setup library imports
import io, time, json, random
import requests
from bs4 import BeautifulSoup
import pandas as pd

# import yelp client library
from yelp.client import Client
from yelp.oauth1_authenticator import Oauth1Authenticator

def authenticate(config_filepath):
    """
    Create an authenticated yelp-python client.

    Args:
        config_filepath (string): relative path (from this file) to a file with your Yelp credentials

    Returns:
        client (yelp.client.Client): authenticated instance of a yelp.Client
    """

    conf = open(config_filepath, 'r')
    cred = json.load(conf)
    auth = Oauth1Authenticator(**cred)
    client = Client(auth)
    return client


def parse_page(html, name = None, price = None, categories = None):
    """
    Parse the reviews on a single page of a restaurant.

    Args:
        html (string): String of HTML corresponding to a Yelp restaurant
        name (string): String of name of restaurant
        price (int): price level of the restaurant
        categories (string): String of categories of restaurant, seperated by comma

    Returns:
        tuple(list, string): a tuple of two elements
            first element: String of name of restaurant
            second element: price level of the restaurant
            third element: String of categories of restaurant, seperated by comma
            fourth element: list of dictionaries corresponding to the extracted review information
            fifth element: URL for the next page of reviews (or None if it is the last page)
    """

    soup = BeautifulSoup(html, 'html.parser')
    results = []
    attr_review = {'itemprop':'review'}
    attr_date = {'itemprop':'datePublished'}
    attr_rate = {'itemprop':'ratingValue'}
    attr_text = {'itemprop':'description'}
    attr_user = {'itemprop': 'author'}

    if name == None:
        name = soup.find('h1').text.strip()
        price_category = soup.find('div', class_='price-category')
        price_tag = price_category.find('span', class_='business-attribute price-range')
        if price_tag == None:
            price = -1
        else:
            price = len(price_tag.text)

    for review in soup.findAll('div', attrs = attr_review):
        user = review.find('meta', attrs = attr_user)['content']
        date = review.find('meta', attrs = attr_date)['content']
        rate = float(review.find('meta', attrs = attr_rate)['content'])
        text = review.find('p', attrs = attr_text).text
        results.append([name, price, categories, user, date, rate, text])

    nextpage = soup.find('a', class_='u-decoration-none next pagination-links_anchor')
    if nextpage == None:
        return (name, price, categories, results, None)

    return (name, price, categories, results, nextpage['href'])

def extract_reviews(url, categories):
    """
    Retrieve ALL of the reviews for a single restaurant on Yelp.

    Parameters:
        url (string): Yelp URL corresponding to the restaurant of interest.
        categories (string): The categories of the page.

    Returns:
        reviews (list): list of dictionaries containing extracted review information
    """

    html = requests.get(url).content
    name, price, categories, allreviews, nextpage = parse_page(html, categories = categories)
    while (nextpage != None):
        # wait a random time to avoid being detected scraper
        time.sleep(0.5 + random.random())
        html = requests.get(nextpage).content
        _, _, _, reviews, nextpage = parse_page(html, name, price, categories)
        allreviews.extend(reviews)

    print 'Extracted %d reviews from this business' % len(allreviews)

    return allreviews

def all_restaurants(client, query, category = None):
    """
    Retrieve ALL the restaurants on Yelp for a given query.

    Args:
        query (string): Search term

    Returns:
        results (list): list of yelp.obj.business.Business objects
    """

    results = []
    if category == None:
        param = {'term': 'restaurants'}
    else:
        param = {'term': 'restaurants', 'category_filter': category}

    response = client.search(query, **param)

    total = response.total
    if total > 1000:
        print 'Trying to get over 1000 records (%d), set to 1000' % total
        total = 1000

    i = 0
    for business in response.businesses:
        i += 1
        print i, '/', total, ':\n', business.url
        try:
            results.extend(extract_reviews(business.url, category))
        except Exception:
            print 'Oh maybe we are recognized as a robot'
            raw_input('Visit Yelp and pass the validation, then press any key to continue')
            try:
                results.extend(extract_reviews(business.url, category))
            except Exception:
                print 'Failed, try again'
                raw_input('press any key to continue')
                continue
            continue

    while i < total:
        param['offset'] = i
        response = client.search(query, **param)
        for business in response.businesses:
            i += 1
            print i, '/', total, ':\n', business.url
            try:
                results.extend(extract_reviews(business.url, category))
            except Exception:
                print 'Oh maybe we are recognized as a scraper'
                raw_input('Visit Yelp and pass the validation, then press any key to continue')
                try:
                    results.extend(extract_reviews(business.url, category))
                except Exception:
                    print 'Failed, try again'
                    raw_input('press any key to continue')
                    continue
                continue
    return results

if __name__ == '__main__':
    reviews = []
    random.seed(15688)
    client = authenticate('yelp.json')
    with open('category.list', 'r') as category_file:
        for line in category_file:
            print line
            if not line.startswith('#'):
                reviews.extend(all_restaurants(client, 'Pittsburgh, PA', line.strip()))
    pd.DataFrame(reviews).to_pickle('dataset.pickle')
