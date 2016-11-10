# setup library imports
import io, time, json
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

    if name == None:
        name = soup.find('h1').text.strip()
        price_category = soup.find('div', class_='price-category')
        price = len(price_category.find('span', 'business-attribute price-range'))
        categories = ','.join([c.text for c in price_category.find('span', 'category-str-list').findAll('a')])

    results = []
    attr_review = {'itemprop':'review'}
    attr_date = {'itemprop':'datePublished'}
    attr_rate = {'itemprop':'ratingValue'}
    attr_text = {'itemprop':'description'}
    attr_user = {'itemprop': 'author'}
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

def extract_reviews(url):
    """
    Retrieve ALL of the reviews for a single restaurant on Yelp.

    Parameters:
        url (string): Yelp URL corresponding to the restaurant of interest.

    Returns:
        reviews (list): list of dictionaries containing extracted review information
    """

    # Write solution here
    html = requests.get(url).content
    name, price, categories, allreviews, nextpage = parse_page(html)
    while (nextpage != None):
        html = requests.get(nextpage).content
        _, _, _, reviews, nextpage = parse_page(html, name, price, categories)
        allreviews.extend(reviews)

    return allreviews

def all_restaurants(client, query):
    """
    Retrieve ALL the restaurants on Yelp for a given query.

    Args:
        query (string): Search term

    Returns:
        results (list): list of yelp.obj.business.Business objects
    """

    param = {'category_filter': 'restaurants'}
    response = client.search(query, **param)
    total = response.total
    results = []
    i = 0
    for business in response.businesses:
        print i, '/', total, ':\n', business.url
        results.extend(extract_reviews(business.url))
    i = 1
    while i < total:
        i += 1
        param['offset'] = len(results)
        response = client.search(query, **param)
        for business in response.businesses:
            print i, '/', total, ':\n', business.url
            results.extend(extract_reviews(business.url))
    return results

if __name__ == '__main__':
    client = authenticate('yelp.json')
    reviews = all_restaurants(client, 'Pittsburgh')
    pd.DataFrame(reviews).to_pickle('dataset.pickle')
