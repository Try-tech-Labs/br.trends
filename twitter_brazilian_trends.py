import sys 
import getopt
import json
import requests 
import collections
from requests_oauthlib import OAuth1
from urllib.parse import urljoin


url_get_trends_WOEID = "https://api.twitter.com/1.1/trends/available.json"  
url_get_tends_by_location = "https://api.twitter.com/1.1/trends/place.json?id=" 


def get_brazilian_WOEID():
	'''This method uses a twitter endopoint that returns all world places that have trending topics, based on their WOEID (WOEID =  Yahoo! Where On Earth ID).
		With this information we can filter by Brazilian places and return it.

		** It returns a list of JSON objects containing the brazilian locations that have trends.
	'''
	try:
		response = requests.get(url_get_trends_WOEID, auth=auth)
		if response.status_code == 200:
			brazilian_results = filter(is_brazilian_WOEID, response.json())
		else:
			print(f"Twitter API request didn't return a valid status code. Status code:{response.status_code}")
			brazilian_results = None
		return brazilian_results
	except Exception as error:
		print(f"ERROR - get_brazilian_WOEID: Failed to fetch twitter API. Error:{error}")
		return None

def is_brazilian_WOEID(item):
	"""This method verifies on a json if there's a {Brazil} or {BR} strings on the country and/or the countryCode attributes of a location.
	
		** It returns a boolean value that indicates if the payload has the brazilian string on it.
	"""
	return item['country'] == "Brazil" or item['countryCode'] == 'BR'

def get_trends_by_location(location):
	"""This method uses a twitter API endpoint that returns a list of trends based on a specific location using WOEID as a filter.
	
		** It returns a list of JSON objects containing the topics found for the location.
	"""
	try:
		response = requests.get(url_get_tends_by_location+str(location['woeid']), auth=auth)
		if response.status_code == 200:
			return response.json()
		else:
			print(f"Twitter API request didn't return a valid status code. Status code:{response.status_code}")
			return None
	except Exception as error:
		print(f"ERROR - get_trends_by_location: Failed to fetch twitter API. Error:{error}")

def get_brazilian_trends(brazil_trends_location):
	"""This method searches by all the trending topics for all the brazilian locations found on get_brazilian_WOEID.
	
		** It returns a list of Twitter trending topics by brazilian location.
	"""
	trends = []
	for location in brazil_trends_location:
		[trends.append(item) for item in get_trends_by_location(location)]
	return trends

def clean_trend_line(trends):
	"""This method just remove some unusable data from the original payload and organize it on a more usable format.
	This method filters the trends if there's no proof if it's a trending (there's no tweet_volume attribute).

		** It returns a list of trending topics ordered by tweet volume, from most retweeted to less.
	"""
	clean_trends = {
		'trends': [],
	}
	for line in trends:
		for trend in line['trends']:
			if trend['tweet_volume'] is not None:
				clean_line = {
					"name": trend['name'],
					"query": trend['query'],
					"tweet_volume": trend['tweet_volume'],
					"fetched_at": line['created_at'],
					"trend_locations": line['locations'],
				}
				clean_trends['trends'].append(clean_line)
	ranked_trends = order_trends_by_volume(clean_trends)
	clean_trends['trends'] = ranked_trends
	return clean_trends

def clean_trends_results():
	"""This method retrieves a prefetched list of trends saved on a json file and clean it using the clean_trend_line

		** It returns a list of trending topics ordered by tweet volume, from most retweeted to less. 
	"""
	brazilian_trends = None
	with open('brazilian_trends.json', encoding='utf-8') as trends:
		brazilian_trends = json.load(trends)
	return clean_trend_line(brazilian_trends)
	
def get_tweet_volume(item):
	"""This method just get the volume on a json object

		** It returns a numeric or None that represents the tweets volume of a trending topic.
	"""
	return item['tweet_volume']

def order_trends_by_volume(trends):
	"""This method just sort a list of json trends by the tweets volume of each topic.

		** It returns a list of trending topics ordered by the topic tweet volume.
	"""
	ordered_list = sorted(trends['trends'], key=get_tweet_volume, reverse=True)
	return ordered_list

def group_trend_item_by_name(trends):
	"""This method grouops a list of topics by its name, to avoid duplicates on the list.
	Once the topic could be the same for different locations on brazil, we could group it as the same trend.
		** It must receive a list of trending topics that was already cleaned with the method (clean_trends_results)


		** It returns a list of list of topics grouped by name.
	"""
	grouped = collections.defaultdict(list)
	for item in trends:
		grouped[item['name']].append(item)
	return grouped.items()

def is_location_already_listed(location, list):
	"""This method verifies if a list already contains a specific location, comparing the name and woeid of the given location with all the listed locations

		** It returns a boolean value that represents if the list already contains the given location.
	"""
	for trend_location in list:
		if (location[0]['name'] == trend_location['name'] or 
			location[0]['woeid'] == trend_location['woeid']):
			return True
	return False

def remove_duplicates_from_group(grouped_trend):
	"""This method removes all the duplicated trends for the list of grouped trends.
		** It must receive a list of trends grouped by name, using the method (group_trend_item_by_name)
		
		** It returns a unique trending topic with a list of locations where it was found.
	"""
	trend = grouped_trend[0]
	locations = []
	for item in grouped_trend:
		if not is_location_already_listed(item['trend_locations'], locations):
			locations.append(item['trend_locations'][0])
	trend['trend_locations'] = locations
	return trend

def create_trending_topics_files():
	"""This method creates two different files with trending topics using the methods above.

	* The first file: brazilian_trends.json: it's almost raw list of brazilian trending topics, not grouped or filtered.
	* The second file: clean_brazilian_trends.json: it's a list of brazilian trending topics grouped by location, filtered and ordered by relevance

		** It returns the list of cleaned data
	"""
	results = get_brazilian_WOEID()
	with open('brazilian_trends.json', 'w', encoding='utf-8') as trends_file:
		trends = get_brazilian_trends(results)
		json.dump(trends, trends_file, ensure_ascii=False, indent=4)
	cleaned_data = clean_trends_results()
	grouped_trends = group_trend_item_by_name(cleaned_data['trends'])
	trends = []
	for model, group in grouped_trends:
		trends.append(remove_duplicates_from_group(group))
	cleaned_data['trends'] = trends
	with open('clean_brazilian_trends.json', 'w', encoding='utf-8') as clean_trends:
		json.dump(cleaned_data, clean_trends, ensure_ascii=False, indent=4)
	return cleaned_data

def get_twitter_trending_topics():
	"""This method only loads the list of cleaned brazilian trend from the json file.

		** It returns a list of cleaned trending topics or None (if the file not exists).
	"""
	try:
		with open('clean_brazilian_trends.json', encoding='utf-8') as clean_trends:
			return json.load(clean_trends)
	except:
		return None

if __name__ == '__main__':
	try:
		opts, args = getopt.getopt(sys.argv[1:],"k:s:a:t:h",["apikey=","apisecret=","accesstoken=","tokensecret=","help="])
	except getopt.GetoptError as error:
		print(error)
		print('Use twitter_brazilian_trends.py -h to see how to use this command')
		sys.exit(2)
	if len(opts) == 0:
		print('Use twitter_brazilian_trends.py -h to see how to use this command')
		sys.exit(2)
	for opt, arg in opts:
		if opt == '-h':
			print('[ -k | --api_key ]: <Twitter API consumer key>')
			print('[ -s | --api_secret ]: <Twitter API consumer secret>')
			print('[ -a | --access_token ]: <Twitter API access token>')
			print('[ -t | --token_secret ]: <Twitter API token secret>')
			sys.exit()
		elif opt in ("-k", "--api_key"):
			API_KEY = arg
		elif opt in ("-s", "--api_secret"):
			API_SECRET = arg
		elif opt in ("-a", "--access_token"):
			ACCESS_TOKEN = arg
		elif opt in ("-t", "--token_secret"):
			TOKEN_SECRET = arg

	auth = OAuth1(API_KEY, API_SECRET, ACCESS_TOKEN, TOKEN_SECRET)
	trends = get_twitter_trending_topics()
	if not trends:
		trends = create_trending_topics_files()
	topics_num=len(trends['trends'])
	for i in range(0, len(trends['trends'])):
		name = trends['trends'][i]['name']
		tweets = trends['trends'][i]['tweet_volume']
		date = trends['trends'][i]['fetched_at']
		locations = [location['name'] for location in trends['trends'][i]['trend_locations']]
		print(f'{i}* {name} - {tweets} tweets on {date} in the follwoing locations: {locations} \n')