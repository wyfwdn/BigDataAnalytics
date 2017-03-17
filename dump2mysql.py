import csv
from sqlalchemy import *

def usenull(lst):
	dt = str(lst[0]).split('/')
	lst[0] = str(dt[2]) + '-' + str(dt[0]) + '-' + str(dt[1])
	rownew=()
	for i in lst:
		if i=='':
			i= None
		rownew+=(i,)
	return rownew

collisionDATABASEURI = "mysql+pymysql://root:940611@localhost/project"

engine = create_engine(DATABASEURI)

try:
	conn = engine.connect()
except:
	print "uh oh, problem connecting to database"
	conn = None

if conn != None:	
	conn.execute('''
	DROP TABLE IF EXISTS collision;
	CREATE TABLE collision(
	cDATE DATE,
	cTIME TEXT,
	BOROUGH TEXT,
	ZIP_CODE INT,
	LATITUDE FLOAT(9,7),
	LONGITUDE FLOAT(9,7),
	LOCATION TEXT,
	ON_STREET_NAME TEXT,CROSS_STREET_NAME TEXT,OFF_STREET_NAME TEXT,
	NUMBER_OF_PERSONS_INJURED INT,NUMBER_OF_PERSONS_KILLED INT,
	NUMBER_OF_PEDESTRIANS_INJURED INT,NUMBER_OF_PEDESTRIANS_KILLED INT,	
	NUMBER_OF_CYCLIST_INJURED INT,NUMBER_OF_CYCLIST_KILLED INT,
	NUMBER_OF_MOTORIST_INJURED INT,NUMBER_OF_MOTORIST_KILLED INT,
	CONTRIBUTING_FACTOR_VEHICLE_1 TEXT,CONTRIBUTING_FACTOR_VEHICLE_2 TEXT,CONTRIBUTING_FACTOR_VEHICLE_3 TEXT,
	CONTRIBUTING_FACTOR_VEHICLE_4 TEXT,CONTRIBUTING_FACTOR_VEHICLE_5 TEXT,
	UNIQUE_KEY INT,
	VEHICLE_TYPE_CODE_1 TEXT,VEHICLE_TYPE_CODE_2 TEXT,VEHICLE_TYPE_CODE_3 TEXT,
	VEHICLE_TYPE_CODE_4 TEXT,VEHICLE_TYPE_CODE_5 TEXT
	);
	''')
		
	Generaldata = csv.reader(file('NYPD_Motor_Vehicle_Collisions.csv'))
	i=1
	for row in list(Generaldata)[1:]:
		print i
		i = i + 1
		#print usenull((row))
		conn.execute('''INSERT INTO collision
		VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
		usenull((row))
		) 

conn.close()