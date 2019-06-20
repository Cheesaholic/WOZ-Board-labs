import Functies
import pandas as pd
import os
import geopandas as gpd

direc = os.getcwd()

gemeenten, gemeente = Functies.getAllWoonplaats()
del gemeente

leaderboard = pd.DataFrame(columns=['gemcode', 'gemnaam', 'diflen', 'foutlen', 'wozlen', 'score', 'datetime'])


gemlist = gemeenten['GEMEENTENAAM'].drop_duplicates().tolist()
for gemeente in gemlist:
    gemcode = gemeenten.loc[gemeenten['GEMEENTENAAM'] == gemeente].reset_index()['GEMEENTECODE'][0]
    bag, checkwoznum, wozfout, wozlen = Functies.checkWOZNUM(gemeente, gemeenten)
    foutlen = len(wozfout)
    dif = Functies.getDifBagWoz(checkwoznum, [1,2,3]).fillna('')
    diflen = len(dif)
    wozfout['NUMJUIST'] = [Functies.getRightNummeraanduiding(bag, g) for g in wozfout.itertuples()]
    if((diflen == 0) & (foutlen == 0)):
        score = 100
    else:
        score = round((100 - (((diflen + foutlen) / wozlen) * 100)), 4)
    i = pd.Timestamp.now()
    datetime = str(str(i.day) + '-' + str(i.month) + '-' + str(i.year) + ' ' + str(i.hour) + ':' + str(i.minute))
    row = {'gemcode' : gemcode, 'gemnaam' : gemeente, 'diflen' : diflen, 'foutlen' : foutlen, 'wozlen' : wozlen, 'score' : score, 'datetime' : datetime}
    leaderboard = leaderboard.append(row, ignore_index=True)
leaderboard.index += 1

leaderboard = leaderboard.sort_values(by=['score'], ascending=False)

scoreboard = {}
ploats = 1
for v in leaderboard['score'].drop_duplicates():
    scoreboard[v] = ploats
    ploats = ploats + 1

leaderboard['plaats'] = [scoreboard[g] for g in leaderboard['score']]

leaderboard.to_csv(str(direc) + '/leaderboard/leaderboard.csv', index=False)

leaderboard['geometry'] = [Functies.getGemeenteGeo(g) for g in leaderboard['gemcode']]
leaderboard = gpd.GeoDataFrame(leaderboard)
leaderboard.crs = {'init' : 'epsg:28992'}
leaderboard = leaderboard.to_crs({'init': 'epsg:3857'})
json_file = open(str(direc) + "/leaderboard/leader_json.txt", 'w')
json_file.write(leaderboard.to_json())
json_file.close()

