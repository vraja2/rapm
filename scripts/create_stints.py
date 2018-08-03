import json
import os
import pandas as pd
import requests

NUM_GAMES_PER_SEASON = 30*82/2

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
ENDPOINT = 'http://stats.nba.com/stats/boxscoreadvancedv2?gameId={}&startPeriod=0&endPeriod=14&startRange={}&endRange={}&rangeType=2'

def get_period_start_seconds(period):
  if period > 4:
    return 720 * 4 + (period - 1 - 4) * (5 * 60)
  else:
    return 720 * (period - 1)

def get_period_end_seconds(period):
  if period > 4:
    return 720 * 4 + (period - 4) * (5 * 60)
  else:
    return 720 * period

def extract_score_margin(pbp_row):
  if pbp_row['SCOREMARGIN']:
    try:
      score_margin = int(pbp_row['SCOREMARGIN'])
      return score_margin
    except ValueError:
      return None
  else:
    return None

def get_stints_for_game(game_id):
  stints = []
  stint_lengths = []
  stint_margins = []
  with open(os.path.join('data', '{}.json'.format(game_id))) as pbp_data_file:
    # parse raw PBP data from stats.nba.com API, data for each game stored in separate files
    pbp_for_game_json = json.load(pbp_data_file)
    pbp_table_headers = pbp_for_game_json['resultSets'][0]['headers']
    pbp_changelog_json = pbp_for_game_json['resultSets'][0]['rowSet']
    pbp_changelog_df = pd.DataFrame(pbp_changelog_json)
    pbp_changelog_df.columns = pbp_table_headers
    prev_period = 0
    prev_event_time_seconds = None
    starting_score_margin = 0
    prev_score_margin = 0

    # use changelog style approach to reflect substitutions
    for i, pbp_row in pbp_changelog_df.iterrows():
      parsed_margin = extract_score_margin(pbp_row)
      if parsed_margin:
        prev_score_margin = parsed_margin
      # only make call to get updated lineup when substitution event takes place
      if is_sub_event(pbp_row):
        if int(pbp_row['PERIOD']) != prev_period:
          # get lineup for start of period
          period_start = get_period_start_seconds(int(pbp_row['PERIOD']))
          period_start_scaled = period_start * 10
          ranges = [(100, 150), (100, 200), (200, 300)]
          for range in ranges:
            lineup_json = requests.get(ENDPOINT.format(game_id, period_start_scaled + range[0], period_start_scaled + range[1]), headers=HEADERS).json()
            if len(lineup_json['resultSets'][0]['rowSet']) != 0:
              break
          df = pd.DataFrame(lineup_json['resultSets'][0]['rowSet'])
          df.columns = lineup_json['resultSets'][0]['headers']
          current_lineup_df = df[['PLAYER_ID', 'PLAYER_NAME']]
          prev_period = pbp_row['PERIOD']
          prev_event_time_seconds = period_start
        # get time in seconds that the lineup played
        event_time_seconds = convert_time_string_to_seconds(pbp_row)
        stint_length = event_time_seconds - prev_event_time_seconds
        stints.append(pd.DataFrame.copy(current_lineup_df))
        stint_margins.append(prev_score_margin - starting_score_margin)
        stint_lengths.append(stint_length)
        prev_event_time_seconds = event_time_seconds
        starting_score_margin = prev_score_margin
        current_lineup_df.loc[current_lineup_df['PLAYER_ID'] == pbp_row['PLAYER1_ID'], 'PLAYER_NAME'] = pbp_row['PLAYER2_NAME']
        current_lineup_df.loc[current_lineup_df['PLAYER_ID'] == pbp_row['PLAYER1_ID'], 'PLAYER_ID'] = pbp_row['PLAYER2_ID']
    stint_length = get_period_end_seconds(prev_period) - prev_event_time_seconds
    stints.append(pd.DataFrame.copy(current_lineup_df))
    stint_lengths.append(stint_length)
    stint_margins.append(prev_score_margin - starting_score_margin)
    for k, stint in enumerate(stints):
      if stint_lengths[k] == 0:
        continue
      print 'Stint length: {}'.format(stint_lengths[k])
      print stint
      print 'Stint margin: {}'.format(stint_margins[k])

def convert_time_string_to_seconds(row):
  time_string = row['PCTIMESTRING']
  period = int(row['PERIOD'])
  if period > 4:
    add = 720 * 4 + (period - 4) * (5 * 60)
  else:
    add = 720 * (period - 1)

  [min, sec] = time_string.split(":")

  min_elapsed = 11 - int(min)
  sec_elapsed = 60 - int(sec)

  return (add + (min_elapsed * 60) + sec_elapsed)

def is_sub_event(pbp_row):
  return (pbp_row['HOMEDESCRIPTION'] and 'SUB' in pbp_row['HOMEDESCRIPTION']) or\
         (pbp_row['VISITORDESCRIPTION'] and 'SUB' in pbp_row['VISITORDESCRIPTION'])

def get_stints_for_season(season):
  for game_num in range(1, NUM_GAMES_PER_SEASON + 1):
    get_stints_for_game('002{:02}0{:04}'.format(season, game_num))

def main():
  if not os.path.exists('data'):
    os.makedirs('data')
  seasons = range(17, 18)
  for season in seasons:
    get_stints_for_season(season)

if __name__ == '__main__':
  get_stints_for_game('0021700002')
  # main()