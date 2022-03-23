import time
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import json
import requests

conf=SparkConf()
conf.setAppName("BigData")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,1)
ssc.checkpoint("checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",6100)

#{'status': 'Played', 'roundId': 4405654, 'gameweek': 1, 'teamsData': {'1628': {'scoreET': 0, 'coachId': 268775, 'side': 'home', 'teamId': 1628, 'score': 0, 'scoreP': 0, 'hasFormation': 1, 'formation': {'bench': [{'playerId': 8049, 'ownGoals': '0', 'redCards': '0', 'goals': '0', 'yellowCards': '0'}, {'playerId': 13745, 'ownGoals': '0', 'redCards': '0', 'goals': '0', 'yellowCards': '0'}, {'playerId': 8501, 'ownGoals': '0', 'redCards': '0', 'goals': '0', 'yellowCards': '0'}, {'playerId': 8391, 'ownGoals': '0', 'redCards': '0', 'goals': '0', 'yellowCards': '0'}, {'playerId': 520317, 'ownGoals': '0', 'redCards': '0', 'goals': '0', 'yellowCards': '0'}, {'playerId': 8471, 'ownGoals': '0', 'redCards': '0', 'goals': '0', 'yellowCards': '0'}, {'playerId': 8554, 'ownGoals': '0', 'redCards': '0', 'goals': '0', 'yellowCards': '0'}], 'lineup': [{'playerId': 127537, 'ownGoals': '0', 'redCards': '0', 'goals': '0', 'yellowCards': '0'}, {'playerId': 397168, 'ownGoals': '0', 'redCards': '0', 'goals': '0', 'yellowCards': '0'}, {'playerId': 8425, 'ownGoals': '0', 'redCards': '0', 'goals': '0', 'yellowCards': '0'}, {'playerId': 8221, 'ownGoals': '1', 'redCards': '0', 'goals': '0', 'yellowCards': '0'}, {'playerId': 454, 'ownGoals': '0', 'redCards': '0', 'goals': '0', 'yellowCards': '0'}, {'playerId': 8142, 'ownGoals': '0', 'redCards': '0', 'goals': '0', 'yellowCards': '88'}, {'playerId': 8186, 'ownGoals': '0', 'redCards': '0', 'goals': '0', 'yellowCards': '0'}, {'playerId': 240559, 'ownGoals': '0', 'redCards': '0', 'goals': '0', 'yellowCards': '0'}, {'playerId': 38031, 'ownGoals': '0', 'redCards': '0', 'goals': '0', 'yellowCards': '0'}, {'playerId': 235555, 'ownGoals': '0', 'redCards': '0', 'goals': '0', 'yellowCards': '0'}, {'playerId': 8422, 'ownGoals': '0', 'redCards': '0', 'goals': '0', 'yellowCards': '0'}], 'substitutions': [{'playerIn': 8471, 'playerOut': 127537, 'minute': 46}, {'playerIn': 8554, 'playerOut': 397168, 'minute': 78}]}, 'scoreHT': 0}, '1673': {'scoreET': 0, 'coachId': 18572, 'side': 'away', 'teamId': 1673, 'score': 3, 'scoreP': 0, 'hasFormation': 1, 'formation': {'bench': [{'playerId': 15474, 'ownGoals': '0', 'redCards': '0', 'goals': '0', 'yellowCards': '0'}, {'playerId': 274482, 'ownGoals': '0', 'redCards': '0', 'goals': '0', 'yellowCards': '0'}, {'playerId': 38377, 'ownGoals': '0', 'redCards': '0', 'goals': '0', 'yellowCards': '0'}, {'playerId': 9280, 'ownGoals': '0', 'redCards': '0', 'goals': '0', 'yellowCards': '0'}, {'playerId': 173, 'ownGoals': '0', 'redCards': '0', 'goals': '0', 'yellowCards': '0'}, {'playerId': 15823, 'ownGoals': '0', 'redCards': '0', 'goals': '0', 'yellowCards': '0'}, {'playerId': 16436, 'ownGoals': '0', 'redCards': '0', 'goals': '0', 'yellowCards': '0'}], 'lineup': [{'playerId': 11078, 'ownGoals': '0', 'redCards': '0', 'goals': '0', 'yellowCards': '28'}, {'playerId': 279709, 'ownGoals': '0', 'redCards': '0', 'goals': '0', 'yellowCards': '60'}, {'playerId': 214654, 'ownGoals': '0', 'redCards': '0', 'goals': '2', 'yellowCards': '73'}, {'playerId': 15215, 'ownGoals': '0', 'redCards': '0', 'goals': '0', 'yellowCards': '0'}, {'playerId': 62389, 'ownGoals': '0', 'redCards': '0', 'goals': '0', 'yellowCards': '0'}, {'playerId': 55979, 'ownGoals': '0', 'redCards': '0', 'goals': '0', 'yellowCards': '0'}, {'playerId': 303357, 'ownGoals': '0', 'redCards': '0', 'goals': '0', 'yellowCards': '0'}, {'playerId': 16122, 'ownGoals': '0', 'redCards': '0', 'goals': '0', 'yellowCards': '0'}, {'playerId': 56038, 'ownGoals': '0', 'redCards': '0', 'goals': '0', 'yellowCards': '0'}, {'playerId': 14796, 'ownGoals': '0', 'redCards': '0', 'goals': '0', 'yellowCards': '0'}, {'playerId': 9419, 'ownGoals': '0', 'redCards': '0', 'goals': '0', 'yellowCards': '0'}], 'substitutions': [{'playerIn': 15823, 'playerOut': 11078, 'minute': 57}, {'playerIn': 16436, 'playerOut': 279709, 'minute': 72}, {'playerIn': 173, 'playerOut': 214654, 'minute': 86}]}, 'scoreHT': 2}}, 'seasonId': 181150, 'dateutc': '2017-08-12 14:00:00', 'winner': 1673, 'venue': 'Selhurst Park', 'wyId': 2499722, 'label': 'Crystal Palace - Huddersfield Town, 0 - 3', 'date': 'August 12, 2017 at 4:00:00 PM GMT+2', 'referees': [{'refereeId': 381851, 'role': 'referee'}, {'refereeId': 385923, 'role': 'firstAssistant'}, {'refereeId': 385918, 'role': 'secondAssistant'}, {'refereeId': 408156, 'role': 'fourthOfficial'}], 'duration': 'Regular', 'competitionId': 364}


from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType

schema = StructType([
    StructField("name", StringType()),
    StructField("birthArea", StringType()),
    StructField("birthDate", StringType()),
    StructField("foot", StringType()),
    StructField("role", StringType()),
    StructField("height", IntegerType()),
    StructField("passportArea", StringType()),
    StructField("weight", IntegerType()),
    StructField("Id", IntegerType()),
    StructField("Contribution",IntegerType())
])

cols = ("name", "birthArea", "birthDate", "foot", "role", "height", "passportArea", "weight")

sql_sc = SQLContext(sc)
df = (sql_sc
	.read
	.schema(schema)
	.option("dateFormat", "yyyy-mm-dd")
	.option("header", "true")
	.csv("players.csv"))

df2=df.drop(*cols)
df2.show()

import csv


def func_play_id(x):
	f = open('test.csv', 'w')
	play = list()
	dat = x['teamsData']
	teamid = list(dat.keys())
	
	for team in teamid:
		form = dat[team]['formation']
		for i in form['bench']:
			li = dict()
			li["playerId"] = i['playerId']
			li["contri"]=0
			li["teamId"] = team
			li["goals"] = i['goals']
			play.append(li)
		for i in form['lineup']:
			li = dict()
			li["playerId"]= i['playerId']
			li["contri"]= 1.05
			li["teamId"] = team
			li["goals"] = i['goals']
			play.append(li)
		for i in form['substitutions']:
			for j in play:
				if i["playerIn"] == j["playerId"]:
					j["contri"] = (90-i['minute'])/90
				if i['playerOut'] == j["playerId"]:
					j["contri"]=(i['minute'])/90
	for li in play:
		for key in li.keys():
				f.write("%s,"%(li[key]))
		f.write("\n")
	f.close()
	return play

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf


lines = dataStream.map(lambda x:x.split('\n'))

records = lines.map(lambda x: json.loads(x[0]))

events=records.filter(lambda x: len(x) == 12)#FILTERS the event records

def func_match(x):
	if len(x)!=12:
		return x

match = records.filter(func_match)

def normalizerize(x):
	f = open("test.csv", "r")
	reader=csv.reader(f)
	for row in reader:
		print(row)
		return row
		'''
		if int(row[0]) == x[0]:
			return (x[0], int(row[1])*x[1])
		
	f.close()
	
	return x
	'''
	
r=contribution.map(normalizerize)	
r.pprint()
#-------#
#time.sleep(2)



play_id = match.map(func_play_id)
play_id.pprint()



ssc.start()
ssc.awaitTermination()
ssc.stop()
