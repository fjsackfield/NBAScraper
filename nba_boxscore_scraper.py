"""
basketball_web_scraper.py
Author: Fred Sackfield
Date Created: 1-24-2017
Description: Test module for web scraping box score data from
             espn.com and storing in a sqlite db.
"""

import urllib
from bs4 import BeautifulSoup
import urllib.request as request
import datetime


box_score_base_url = 'http://www.espn.com/nba/boxscore?gameId='


def get_away_team(gameID):
    game_summary_url = 'http://www.espn.com/nba/game?gameId=' + gameID
    game_summary_page = request.urlopen(game_summary_url)
    soup = BeautifulSoup(game_summary_page, 'html.parser')

    away_team_dict = {}
    
    away_team_long_name = soup.find_all('span', {'class': 'long-name'})[0].text
    away_team_short_name = soup.find_all('span', {'class': 'short-name'})[0].text
    away_team_abbrev = soup.find_all('span', {'class': 'abbrev'})[0].text

    away_team_name = away_team_long_name + ' ' + away_team_short_name

    away_team_dict['name'] = away_team_name
    away_team_dict['abbrev'] = away_team_abbrev
    away_team_dict['record'] = get_away_record(gameID)
    

    return away_team_dict


def get_home_team(gameID):
    game_summary_url = 'http://www.espn.com/nba/game?gameId=' + gameID
    game_summary_page = request.urlopen(game_summary_url)
    soup = BeautifulSoup(game_summary_page, 'html.parser')

    home_team_dict = {}
    
    home_team_long_name = soup.find_all('span', {'class': 'long-name'})[1].text
    home_team_short_name = soup.find_all('span', {'class': 'short-name'})[1].text
    home_team_abbrev = soup.find_all('span', {'class': 'abbrev'})[1].text

    home_team_name = home_team_long_name + ' ' + home_team_short_name

    home_team_dict['name'] = home_team_name
    home_team_dict['abbrev'] = home_team_abbrev
    home_team_dict['record'] = get_home_record(gameID)

    return home_team_dict

def get_game_date(gameID):
    game_summary_url = 'http://www.espn.com/nba/game?gameId=' + gameID
    game_summary_page = request.urlopen(game_summary_url)
    soup = BeautifulSoup(game_summary_page, 'html.parser')

    meta = soup.find('meta', {'name':'title'})
    game_date = meta.attrs['content'].split(' - ')[2]
    date = datetime.datetime.strptime(game_date, '%B %d, %Y')
    date = date.strftime('%Y-%m-%d')
    date_list = date.split('-')
    date = datetime.date(int(date_list[0]), int(date_list[1]), int(date_list[2]))
    

    return date


def get_away_players(gameID):
    box_score_url = box_score_base_url + gameID
    box_score_page = request.urlopen(box_score_url)
    soup = BeautifulSoup(box_score_page, 'html.parser')
    away_tree = soup.find('div', {'class': 'col column-one gamepackage-away-wrap'})
    away_player_list = []
    away_players = away_tree.find_all_next('span', {'class': 'abbr'}, limit=13)
    
    for player in away_players:
        name = player.get_text()
        away_player_list.append(name)

    return away_player_list


def get_home_players(gameID):
    box_score_url = box_score_base_url + gameID
    box_score_page = request.urlopen(box_score_url)
    soup = BeautifulSoup(box_score_page, 'html.parser')
    home_tree = soup.find('div', {'class': 'col column-two gamepackage-home-wrap'})
    home_player_list = []
    home_players = home_tree.find_all_next('span', {'class': 'abbr'})
    
    for player in home_players:
        name = player.get_text()
        home_player_list.append(name)

    return home_player_list

def get_away_team_stats(gameID):
    box_score_url = box_score_base_url + gameID
    box_score_page = request.urlopen(box_score_url)
    soup = BeautifulSoup(box_score_page, 'html.parser')

    matchup_url = 'http://www.espn.com/nba/matchup?gameId=' + gameID
    matchup_page = request.urlopen(matchup_url)
    soup2 = BeautifulSoup(matchup_page, 'html.parser')

    team_stat_root = soup.find(text='TEAM').parent.nextSibling

    away_stat_dict = {}

    for i in range(13):
        if team_stat_root.nextSibling.attrs['class'] == ['fg']:
            splitList = team_stat_root.nextSibling.text.split('-')
            away_stat_dict['fgm'] = int(splitList[0])
            away_stat_dict['fga'] = int(splitList[1])
        elif (team_stat_root.nextSibling.attrs['class'] == ['3pt']):
            splitList = team_stat_root.nextSibling.text.split('-')
            away_stat_dict['3ptm'] = int(splitList[0])
            away_stat_dict['3pta'] = int(splitList[1])
        elif (team_stat_root.nextSibling.attrs['class'] == ['ft']):
            splitList = team_stat_root.nextSibling.text.split('-')
            away_stat_dict['ftm'] = int(splitList[0])
            away_stat_dict['fta'] = int(splitList[1])
        elif (team_stat_root.nextSibling.attrs['class'] == ['plusminus']):
            away_stat_dict['plusminus'] = 'N/A'
        else:
             away_stat_dict[team_stat_root.nextSibling.attrs['class'][0]] = int(team_stat_root.nextSibling.text)
        team_stat_root = team_stat_root.nextSibling

    fbp = soup2.find('tr', {'class': 'highlight', 'data-stat-attr': 'fastBreakPoints'})
    text = fbp.find('td').find_next('td').text
    text = string_helper(text)
    away_stat_dict['fastBreakPts'] = int(text)

    pip = soup2.find('tr', {'class': 'highlight', 'data-stat-attr': 'pointsInPaint'})
    text = pip.find('td').find_next('td').text
    text = string_helper(text)
    away_stat_dict['ptsInPaint'] = int(text)

    tf = soup2.find('tr', {'class': 'indent', 'data-stat-attr': 'technicalFouls'})
    text = tf.find('td').find_next('td').text
    text = string_helper(text)
    away_stat_dict['technicalFouls'] = int(text)

    ff = soup2.find('tr', {'class': 'indent', 'data-stat-attr': 'flagrantFouls'})
    text = ff.find('td').find_next('td').text
    text = string_helper(text)
    away_stat_dict['flagrantFouls'] = int(text)
    
    print('Away stat dict created for game ' + gameID)
    return away_stat_dict

def get_home_team_stats(gameID):
    box_score_url = box_score_base_url + gameID
    box_score_page = request.urlopen(box_score_url)
    soup = BeautifulSoup(box_score_page, 'html.parser')

    matchup_url = 'http://www.espn.com/nba/matchup?gameId=' + gameID
    matchup_page = request.urlopen(matchup_url)
    soup2 = BeautifulSoup(matchup_page, 'html.parser')

    team_stat_root = soup.find(text='TEAM')
    team_stat_root = team_stat_root.find_next(text='TEAM').parent.nextSibling
    home_stat_dict = {}


    for i in range(13):
        if team_stat_root.nextSibling.attrs['class'] == ['fg']:
            splitList = team_stat_root.nextSibling.text.split('-')
            home_stat_dict['fgm'] = int(splitList[0])
            home_stat_dict['fga'] = int(splitList[1])
        elif (team_stat_root.nextSibling.attrs['class'] == ['3pt']):
            splitList = team_stat_root.nextSibling.text.split('-')
            home_stat_dict['3ptm'] = int(splitList[0])
            home_stat_dict['3pta'] = int(splitList[1])
        elif (team_stat_root.nextSibling.attrs['class'] == ['ft']):
            splitList = team_stat_root.nextSibling.text.split('-')
            home_stat_dict['ftm'] = int(splitList[0])
            home_stat_dict['fta'] = int(splitList[1])
        elif (team_stat_root.nextSibling.attrs['class'] == ['plusminus']):
            home_stat_dict['plusminus'] = 'N/A'
        else:
             home_stat_dict[team_stat_root.nextSibling.attrs['class'][0]] = int(team_stat_root.nextSibling.text)
        team_stat_root = team_stat_root.nextSibling
        

    fbp = soup2.find('tr', {'class': 'highlight', 'data-stat-attr': 'fastBreakPoints'})
    text = fbp.find('td').find_next('td').find_next('td').text
    text = string_helper(text)
    home_stat_dict['fastBreakPts'] = int(text)

    pip = soup2.find('tr', {'class': 'highlight', 'data-stat-attr': 'pointsInPaint'})
    text = pip.find('td').find_next('td').find_next('td').text
    text = string_helper(text)
    home_stat_dict['ptsInPaint'] = int(text)

    tf = soup2.find('tr', {'class': 'indent', 'data-stat-attr': 'technicalFouls'})
    text = tf.find('td').find_next('td').find_next('td').text
    text = string_helper(text)
    home_stat_dict['technicalFouls'] = int(text)

    ff = soup2.find('tr', {'class': 'indent', 'data-stat-attr': 'flagrantFouls'})
    text = ff.find('td').find_next('td').find_next('td').text
    text = string_helper(text)
    home_stat_dict['flagrantFouls'] = int(text)
    
    print('Home stat dict created for game ' + gameID)
    return home_stat_dict
    
def get_away_box_score(gameID):
    box_score_url = box_score_base_url + gameID
    box_score_page = request.urlopen(box_score_url)
    soup = BeautifulSoup(box_score_page, 'html.parser')

    away_stat_dict = {}
    away_team_stat_dict = get_away_team_stats(gameID)
    
    away_stat_dict['TEAM'] = away_team_stat_dict
        
    for player in get_away_players(gameID):
        name = soup.find(text=player)
        away_stat_dict[name] = {}  #Initialize key(player_name)-value(nested dict of stats) nested dict for box_score_dict[gameID][away_box_score]
        stat_root = name.parent.parent.parent
        if 'DNP' in stat_root.nextSibling.text:
            away_stat_dict[name] = stat_root.nextSibling.text
        elif 'Did not play' in stat_root.nextSibling.text:
            away_stat_dict[name] = stat_root.nextSibling.text
        else:
            for i in range(14):
                if (stat_root.nextSibling.text == '--'):
                    away_stat_dict[name][stat_root.nextSibling.attrs['class'][0]] = 0
                elif (stat_root.nextSibling.text == '-----'):
                    away_stat_dict[name][stat_root.nextSibling.attrs['class'][0]] = 0
                elif stat_root.nextSibling.attrs['class'] == ['fg']:
                    splitList = stat_root.nextSibling.text.split('-')
                    away_stat_dict[name]['fgm'] = int(splitList[0])
                    away_stat_dict[name]['fga'] = int(splitList[1])
                elif (stat_root.nextSibling.attrs['class'] == ['3pt']):
                    splitList = stat_root.nextSibling.text.split('-')
                    away_stat_dict[name]['3ptm'] = int(splitList[0])
                    away_stat_dict[name]['3pta'] = int(splitList[1])
                elif (stat_root.nextSibling.attrs['class'] == ['ft']):
                    splitList = stat_root.nextSibling.text.split('-')
                    away_stat_dict[name]['ftm'] = int(splitList[0])
                    away_stat_dict[name]['fta'] = int(splitList[1])
                else:
                    away_stat_dict[name][stat_root.nextSibling.attrs['class'][0]] = int(stat_root.nextSibling.text)
                stat_root = stat_root.nextSibling
    
    print('Created away_box_score dict for game' + gameID)
    return away_stat_dict

    
def get_home_box_score(gameID):
    box_score_url = box_score_base_url + gameID
    box_score_page = request.urlopen(box_score_url)
    soup = BeautifulSoup(box_score_page, 'html.parser')

    home_stat_dict = {}
    home_team_stat_dict = get_home_team_stats(gameID)
 

    home_stat_dict['TEAM'] = home_team_stat_dict

    for player in get_home_players(gameID):
        name = soup.find(text=player)
        home_stat_dict[name] = {} #Initialize key(player_name)-value(nested dict of stats) nested dict for box_score_dict[gameID][home_box_score]
        stat_root = name.parent.parent.parent
        if 'DNP' in stat_root.nextSibling.text:
            home_stat_dict[name] = stat_root.nextSibling.text
        elif 'Did not play' in stat_root.nextSibling.text:
            home_stat_dict[name] = stat_root.nextSibling.text
        else:
            for i in range(14):
                if (stat_root.nextSibling.text == '--'):
                    home_stat_dict[name][stat_root.nextSibling.attrs['class'][0]] = 0
                elif (stat_root.nextSibling.text == '-----'):
                    home_stat_dict[name][stat_root.nextSibling.attrs['class'][0]] = 0
                elif stat_root.nextSibling.attrs['class'] == ['fg']:
                    splitList = stat_root.nextSibling.text.split('-')
                    home_stat_dict[name]['fgm'] = int(splitList[0])
                    home_stat_dict[name]['fga'] = int(splitList[1])
                elif (stat_root.nextSibling.attrs['class'] == ['3pt']):
                    splitList = stat_root.nextSibling.text.split('-')
                    home_stat_dict[name]['3ptm'] = int(splitList[0])
                    home_stat_dict[name]['3pta'] = int(splitList[1])
                elif (stat_root.nextSibling.attrs['class'] == ['ft']):
                    splitList = stat_root.nextSibling.text.split('-')
                    home_stat_dict[name]['ftm'] = int(splitList[0])
                    home_stat_dict[name]['fta'] = int(splitList[1])
                else:
                    home_stat_dict[name][stat_root.nextSibling.attrs['class'][0]] = int(stat_root.nextSibling.text)
                stat_root = stat_root.nextSibling
    
    print('Created home_box_score dict for game' + gameID)
    return home_stat_dict


def get_odds_details(gameID):
    game_summary_url = 'http://www.espn.com/nba/game?gameId=' + gameID
    game_summary_page = request.urlopen(game_summary_url)
    soup = BeautifulSoup(game_summary_page, 'html.parser')
    
    odds_details_soup = soup.find('div', {'class': 'odds-details'})
    odds_details_list = list(filter(('').__ne__, odds_details_soup.text.split('\n')))
    odds_details_dict = {}
    odds_details_dict['line'] = {}
    #print(odds_details_list)
    
    if odds_details_list[0].split(':')[1] == ' EVEN':
        odds_details_dict['line']['favorite'] = 'N/A'
        odds_details_dict['line']['margin'] = 0
    else:
        odds_details_line = list(filter(('').__ne__, odds_details_list[0].split(':')[1].split(' ')))

        odds_details_dict['line']['favorite'] = odds_details_line[0]
        odds_details_dict['line']['margin'] = float(odds_details_line[1])
    if len(odds_details_list) == 1:
        odds_details_dict['over/under'] = 0
    else:
        odds_details_dict['over/under'] = int(odds_details_list[1].split(':')[1])

    return odds_details_dict

def get_away_record(gameID):
    box_score_url = box_score_base_url + gameID
    box_score_page = request.urlopen(box_score_url)
    soup = BeautifulSoup(box_score_page, 'html.parser')

    record_dict = {}
    record_dict['overall'] = {}
    record_dict['away'] = {}

    team_info = soup.find('div', {'class': 'team-info'})
    away_record = team_info.find('div', {'class': 'record'})
    away_record = away_record.text.split(',')
    overall_list = away_record[0].split('-')

    record_dict['overall']['W'] = int(overall_list[0])
    record_dict['overall']['L'] = int(overall_list[1])

    if len(away_record) > 1:
        sub_list = away_record[1].split('-')
        record_dict['away']['W'] = int(sub_list[0])
        record_dict['away']['L'] = int(sub_list[1].split(' ')[0])
    else:
        return record_dict

    return record_dict

    
def get_home_record(gameID):
    box_score_url = box_score_base_url + gameID
    box_score_page = request.urlopen(box_score_url)
    soup = BeautifulSoup(box_score_page, 'html.parser')

    record_dict = {}
    record_dict['overall'] = {}
    record_dict['home'] = {}

    team_info = soup.find('div', {'class': 'team-info'})
    team_info = team_info.find_next('div', {'class': 'team-info'})
    home_record = team_info.find('div', {'class': 'record'})
    home_record = home_record.text.split(',')
    overall_list = home_record[0].split('-')
    
    record_dict['overall']['W'] = int(overall_list[0])
    record_dict['overall']['L'] = int(overall_list[1])

    if len(home_record) > 1:
        sub_list = home_record[1].split('-')
        record_dict['home']['W'] = int(sub_list[0])
        record_dict['home']['L'] = int(sub_list[1].split(' ')[0])
    else:
        return record_dict

    return record_dict





    
def string_helper(string):
    string_list = []

    for char in string:
        if (char == '\n' or char == '\t'):
            continue
        else:
            string_list.append(char)

    new_string = ''
    for char in string_list:
        new_string = new_string + char

    return new_string
            
    
    

    

    
    






