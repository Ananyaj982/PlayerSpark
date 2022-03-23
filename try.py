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

ssc=StreamingContext(sc,2)
ssc.checkpoint("checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",6100)

lines = dataStream.map(lambda x:x.split('\n'))

records = lines.map(lambda x: json.loads(x[0]))

def func_3(x):
	if (len(x)==12):
		if (x['eventId']==3):
			return x


def func_1802(x):
		for i in x['tags']:
			if i['id'] == 1802:
				return (x['eventId'],1)
		return (x['eventId'], 0)

def func_1801(x):
		for i in x['tags']:
			if i['id'] == 1801:
				return (x['eventId'],1)
		return (x['eventId'],0)	


def func_1801_2(x):
		for i in x['tags']:
			if i['id'] == 1801:
				return x

def func_pengoal(x):
	if x['subEventId']==35:
			for i in x['tags']:
				if i['id']==101:
					return (x['eventId'],1)
	else:
		return (x['eventId'], 0)
		
free_kick = records.filter(func_3)

#Inaccurate passes
a = free_kick.map(func_1802)
a_inter1 = a.reduceByKey(lambda x,y: x+y)

#accurate passes
b = free_kick.map(func_1801)
b_inter1 = b.reduceByKey(lambda x,y: x+y)


# total free kick = accurate + inaccurate passes
a_b_inter = a_inter1.join(b_inter1)
a_b = a_b_inter.map(lambda x:(x[0], x[1][0] + x[1][1]))

# Penalties which are goal
b2 = free_kick.filter(func_1801_2)
b_1 = b2.map(func_pengoal)
b_1_inter1 = b_1.reduceByKey(lambda x,y: x+y)


# number of penalties + number of effective free kicks
b_b_1 = b_inter1.join(b_1_inter1)
b_b_1_fin = b_b_1.map(lambda x:(x[0], x[1][0] + x[1][1]))


#division
free_effect_inter = b_b_1_fin.join(a_b)
free_effect = free_effect_inter.map(lambda x:(x[0], x[1][0] / x[1][1]))
free_effect.pprint()

ssc.start()
ssc.awaitTermination()
ssc.stop()
