"""
nba_odds_analysis.py
Author: Fred Sackfield
Date Created: 1-29-2017
Description: Module that imports create_nba_game_dict from
             nba_boxscore_sqlite.py and loops through
             each game dict to display actual results/totals vs. the original
             betting lines
"""

from nba_games import create_nba_game_dict

IDBase = '40090013'
IDList = []



for i in range(5):
    IDList.append(IDBase + str(i))


nba_game_dict = create_nba_game_dict(IDList)

for key in nba_game_dict:
    print(key)
    print('Away Team: ' + nba_game_dict[key]['away_team'])
    print('\t' + 'Points: ' + str(nba_game_dict[key]['away_box_score']['TEAM']['pts']))
    print('Home Team: ' + nba_game_dict[key]['home_team'])
    print('\t' + 'Points: ' + str(nba_game_dict[key]['home_box_score']['TEAM']['pts']))
    print('Betting Line: ' + str(nba_game_dict[key]['odds_details']['line']))
    print('Over/Under Line: ' + str(nba_game_dict[key]['odds_details']['over/under']))
    print('\t' + 'Actual total: ' + str(nba_game_dict[key]['away_box_score']['TEAM']['pts'] + nba_game_dict[key]['home_box_score']['TEAM']['pts']))
    print('\n')



    














    
