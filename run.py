import os,sys
from sqlalchemy import *

DATABASEURI = "mysql+pymysql://root:940611@localhost/project"

engine = create_engine(DATABASEURI)

###### choose a year 2012~2016
year = int(raw_input('Enter year(2012~2016):'))
print year

bro = ['ALL','BRONX','BROOKLYN','MANHATTAN','QUEENS','STATEN ISLAND']
borough = bro[int(raw_input('''
| ALL boroughs |-------0
| BRONX         |-------1
| BROOKLYN      |-------2
| MANHATTAN     |-------3
| QUEENS        |-------4
| STATEN ISLAND |-------5
Enter borough number:(0~5):'''))]
print borough

types = ['All','killed','injured']
type = types[int(raw_input('''
| ALL           |-------0
| killed        |-------1
| injured       |-------2
Enter type:(0~2):'''))]
print type

numCenter = int(raw_input('Enter K:'))
print numCenter

try:
	conn = engine.connect()
except:
	print "uh oh, problem connecting to database"
	conn = None
	
if type == 'ALL': 
	injure = 0
	kill = 0
elif type == 'killed' :
	injure = 0
	kill = 1
else :
	injure = 1
	kill = 0

	
if conn != None:	

	if borough == 'ALL':
		cur = conn.execute('''
		SELECT LOCATION
		FROM collision c
		WHERE c.cDATE >= %s AND c.cDATE < %s AND c.NUMBER_OF_PERSONS_INJURED>=%s AND c.NUMBER_OF_PERSONS_KILLED>=%s;
		''',(str(year)+'-01-01',str(year+1)+'-01-01',injure,kill))
	else :
		cur = conn.execute('''
		SELECT LOCATION
		FROM collision c
		WHERE c.cDATE >= %s AND c.cDATE < %s AND c.NUMBER_OF_PERSONS_INJURED>=%s AND c.NUMBER_OF_PERSONS_KILLED>=%s AND c.BOROUGH=%s;
		''',(str(year)+'-01-01',str(year+1)+'-01-01',injure,kill,borough))		

	print 'There are ', cur.rowcount, 'accidents being analyzed. Please wait for seconds.'
	totalRecords = int(cur.rowcount)
	if numCenter>totalRecords:
		numCenter = totalRecords	
		print "K>#records, change K to no need for clustering."
	filename = './location/'+'T'+str(year)+borough.replace(' ','_')+type+'.txt'
	print 'Saving to file:', filename
	f = open(filename,'w')	
	i = -1
	for result in cur:
		#print result
		if result != (None,) :
			i = i + 1
			t = str(result).strip("('")
			t = t.strip("',)").split(',')
			ele = str(i) + ' 1:' + t[0] + ' 2:' + t[1].lstrip(' ') + '\n'
			f.write(ele)
	f.close()
	print 'Try to put on hadoop.'
	try: os.system("hadoop dfs -put " + filename +' /usr/collision/location')
	except: print "file exists,don't have to put it again"
	print 'Start clustering with spark.'
	os.system("spark-submit kmeanCluster.py " + str(year) + " " + str(type)  + " " + str(borough).replace(' ','_')  + " " + str(numCenter) + " " +str(totalRecords))
	print 'Check on maps.'
	os.system("open ./mapmarker/where.html")
conn.close()	