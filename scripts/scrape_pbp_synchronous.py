import json
import os
import requests

USER_AGENT = "curl"
REFERER = "http://stats.nba.com/"
HEADERS = {
    'Host': 'stats.nba.com',
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
    'Referer': 'stats.nba.com',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
}
NUM_GAMES_PER_SEASON = 30*82/2

endpoint = 'http://stats.nba.com/stats/playbyplayv2?GameID={}&StartPeriod=0&EndPeriod=0&StartRange=0&EndRange=0&RangeType=0'

def make_request_to_nba_stats(url):
  return requests.get(url, headers=HEADERS).json()

def get_pbp_data_for_game(game_id):
  print "Game ID {}".format(game_id)
  with open(os.path.join('data', '{}.json'.format(game_id)), 'w') as outfile:
    print endpoint.format(game_id)
    json.dump(make_request_to_nba_stats(endpoint.format(game_id)), outfile)

def get_pbp_data_for_season(season):
  for game_num in range(1, NUM_GAMES_PER_SEASON + 1):
    get_pbp_data_for_game('002{:02}0{:04}'.format(season, game_num))

def main():
  if not os.path.exists('data'):
    os.makedirs('data')
  seasons = range(17, 18)
  for season in seasons:
    get_pbp_data_for_season(season)

if __name__ == '__main__':
  main()