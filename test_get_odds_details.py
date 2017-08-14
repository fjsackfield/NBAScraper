import urllib
from bs4 import BeautifulSoup
import urllib.request as request

gameID = '400900126'
game_summary_url = 'http://www.espn.com/nba/game?gameId=' + gameID
game_summary_page = request.urlopen(game_summary_url)
soup = BeautifulSoup(game_summary_page, 'html.parser')
    
odds_details_soup = soup.find('div', {'class': 'odds-details'})
odds_details_list = list(filter(('').__ne__, odds_details_soup.text.split('\n')))
odds_details_dict = {}
odds_details_dict['line'] = {}
odds_details_line = list(filter(('').__ne__, odds_details_list[0].split(':')[1].split(' ')))

odds_details_dict['line']['favorite'] = odds_details_line[0]
odds_details_dict['line']['margin'] = float(odds_details_line[1])

odds_details_dict['over/under'] = int(odds_details_list[1].split(':')[1])

print(odds_details_dict)
