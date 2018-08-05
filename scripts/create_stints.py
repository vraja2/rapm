import json
import os
import pandas as pd
import re
import requests

from collections import defaultdict
from collections import namedtuple

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

FREE_THROW_REGEX = 'Free Throw (\d{1}) of (\d{1})'
MADE_SHOT_REGEX = '\(\d+ PTS\)'
REBOUND_REGEX = 'REBOUND \(Off:(\d+) Def:(\d+)\)'
ReboundStats = namedtuple('ReboundStats', ['offensive', 'defensive'])

def get_stints_for_game(game_id):
  stints = []
  stint_lengths = []
  stint_margins = []
  stint_possessions = []
  # TODO: figure out discrepancy in 26 possession stint
  with open(os.path.join('data', '{}.json'.format(game_id))) as pbp_data_file:
    # parse raw PBP data from stats.nba.com API, data for each game stored in separate files
    pbp_for_game_json = json.load(pbp_data_file)
    pbp_table_headers = pbp_for_game_json['resultSets'][0]['headers']
    pbp_changelog_json = pbp_for_game_json['resultSets'][0]['rowSet']
    pbp_changelog_df = pd.DataFrame(pbp_changelog_json)
    pbp_changelog_df.columns = pbp_table_headers

    # helper vars to keep track of state when populating stints
    prev_period = 0
    prev_event_time_seconds = None
    starting_score_margin = 0
    prev_score_margin = 0
    possessions_in_stint = 0
    rebounds_by_player = defaultdict(lambda: ReboundStats(offensive=0, defensive=0))
    prev_pbp_row = pd.Series()

    # use changelog style approach to reflect substitutions
    for i, pbp_row in pbp_changelog_df.iterrows():
      # print pbp_row
      parsed_margin = extract_score_margin(pbp_row)
      if parsed_margin:
        prev_score_margin = parsed_margin
      # only make call to get updated lineup when substitution event takes place
      if is_sub_event(pbp_row):
        if int(pbp_row['PERIOD']) != prev_period:
          # possession ends when period ends
          possessions_in_stint += 1
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
        stint_possessions.append(possessions_in_stint)
        possessions_in_stint = 0
        prev_event_time_seconds = event_time_seconds
        starting_score_margin = prev_score_margin
        current_lineup_df.loc[current_lineup_df['PLAYER_ID'] == pbp_row['PLAYER1_ID'], 'PLAYER_NAME'] = pbp_row['PLAYER2_NAME']
        current_lineup_df.loc[current_lineup_df['PLAYER_ID'] == pbp_row['PLAYER1_ID'], 'PLAYER_ID'] = pbp_row['PLAYER2_ID']
      print pbp_row['PCTIMESTRING']
      if is_turnover_event(pbp_row):
        print 'TURNOVER'
        possessions_in_stint += 1
      if is_violation_event(pbp_row):
        print 'VIOLATION'
        possessions_in_stint += 1
      player_id, curr_rebound_stats = parse_rebound_event(pbp_row)
      if player_id and curr_rebound_stats:
        prev_rebound_stats = rebounds_by_player[player_id]
        if is_player_defensive_rebound(prev_rebound_stats, curr_rebound_stats):
          print 'DEFENSIVE REBOUND'
          possessions_in_stint += 1
        rebounds_by_player[player_id] = curr_rebound_stats
      if is_field_goal_event(pbp_row):
        print 'FIELD GOAL'
        possessions_in_stint += 1
      if is_team_defensive_rebound(pbp_row, prev_pbp_row):
        possessions_in_stint += 1
      is_miss, current_free_throw, total_free_throws = parse_free_throw_event(pbp_row)
      if current_free_throw and total_free_throws and current_free_throw == total_free_throws:
        if not is_miss:
          possessions_in_stint += 1
      prev_pbp_row = pbp_row
    print rebounds_by_player
    stint_length = get_period_end_seconds(prev_period) - prev_event_time_seconds
    stints.append(pd.DataFrame.copy(current_lineup_df))
    stint_lengths.append(stint_length)
    stint_margins.append(prev_score_margin - starting_score_margin)
    # game ending signifies one last possession
    stint_possessions.append(possessions_in_stint + 1)
    for k, stint in enumerate(stints):
      if stint_lengths[k] == 0:
        continue
      print 'Stint length: {}'.format(stint_lengths[k])
      print stint
      print 'Stint margin: {}'.format(stint_margins[k])
      print 'Stint possessions: {}'.format(stint_possessions[k])
    print sum(stint_possessions)

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

def is_turnover_event(pbp_row):
  return (pbp_row['HOMEDESCRIPTION'] and 'Turnover' in pbp_row['HOMEDESCRIPTION']) or\
         (pbp_row['VISITORDESCRIPTION'] and 'Turnover' in pbp_row['VISITORDESCRIPTION'])

def is_violation_event(pbp_row):
  return (pbp_row['HOMEDESCRIPTION'] and 'Violation' in pbp_row['HOMEDESCRIPTION']) or\
         (pbp_row['VISITORDESCRIPTION'] and 'Violation' in pbp_row['VISITORDESCRIPTION'])

def parse_rebound_event(pbp_row):
  match = None
  if pbp_row['HOMEDESCRIPTION']:
    match = re.search(REBOUND_REGEX, pbp_row['HOMEDESCRIPTION'])
  elif pbp_row['VISITORDESCRIPTION']:
    match = re.search(REBOUND_REGEX, pbp_row['VISITORDESCRIPTION'])

  if match:
    return pbp_row['PLAYER1_ID'], ReboundStats(offensive=int(match.group(1)), defensive=int(match.group(2)))

  return None, None

def is_field_goal_event(pbp_row):
  match = None
  if pbp_row['HOMEDESCRIPTION'] and 'Free Throw' not in pbp_row['HOMEDESCRIPTION']:
    match = re.search(MADE_SHOT_REGEX, pbp_row['HOMEDESCRIPTION'])
  elif pbp_row['VISITORDESCRIPTION'] and 'Free Throw' not in pbp_row['VISITORDESCRIPTION']:
    match = re.search(MADE_SHOT_REGEX, pbp_row['VISITORDESCRIPTION'])

  return match

def parse_free_throw_event(pbp_row):
  match = None
  is_miss = False
  if pbp_row['HOMEDESCRIPTION']:
    is_miss = 'MISS' in pbp_row['HOMEDESCRIPTION']
    match = re.search(FREE_THROW_REGEX, pbp_row['HOMEDESCRIPTION'])
  elif pbp_row['VISITORDESCRIPTION']:
    is_miss = 'MISS' in pbp_row['VISITORDESCRIPTION']
    match = re.search(FREE_THROW_REGEX, pbp_row['VISITORDESCRIPTION'])
  if match:
    return is_miss, int(match.group(1)), int(match.group(2))

  return is_miss, None, None

def get_stints_for_season(season):
  for game_num in range(1, NUM_GAMES_PER_SEASON + 1):
    get_stints_for_game('002{:02}0{:04}'.format(season, game_num))

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

def is_player_defensive_rebound(rebound_stats_prev, rebound_stats_curr):
  return rebound_stats_curr.defensive - rebound_stats_prev.defensive == 1 and \
         rebound_stats_curr.offensive == rebound_stats_prev.offensive

def is_team_defensive_rebound(curr_pbp_row, prev_pbp_row):
  if prev_pbp_row.empty:
    return False

  if curr_pbp_row['HOMEDESCRIPTION']  and prev_pbp_row['VISITORDESCRIPTION']:
    return 'MISS' in prev_pbp_row['VISITORDESCRIPTION'] and 'Rebound' in curr_pbp_row['HOMEDESCRIPTION'] and int(prev_pbp_row['PLAYER1_TEAM_ID']) != int(curr_pbp_row['PLAYER1_ID'])
  if curr_pbp_row['VISITORDESCRIPTION'] and prev_pbp_row['HOMEDESCRIPTION']:
    return 'MISS' in prev_pbp_row['HOMEDESCRIPTION'] and 'Rebound' in curr_pbp_row['VISITORDESCRIPTION'] and int(prev_pbp_row['PLAYER1_TEAM_ID']) != int(curr_pbp_row['PLAYER1_ID'])

  return False

def main():
  if not os.path.exists('data'):
    os.makedirs('data')
  seasons = range(17, 18)
  for season in seasons:
    get_stints_for_season(season)

if __name__ == '__main__':
  get_stints_for_game('0021700002')
  # main()