import urllib
import urllib.request as request
from bs4 import BeautifulSoup

url = 'http://www.espn.com/nba/standings/_/group/divison'
request = request.urlopen(url)
soup = BeautifulSoup(request, 'html.parser')


divs = soup.find_all('thead', {'class': 'standings-categories'})
#print(divs[0].fetchNextSiblings())
print(len(divs[0].fetchNextSiblings()))
for sib in divs[0].fetchNextSiblings():
    print(sib.find('span', {'class': 'team-names'}))
'''
teams = []

for div in divs:
    team_names = div.find_all('span', {'class': 'team-names'})
    for team in team_names:
        teams.append(team.text)

print(teams)
'''
