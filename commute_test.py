import datetime as dt
import gpxpy
import gpxpy.gpx
import argparse
import happy

lon=[]
lat=[]
t=[]
gpx_file = open('1217Caltrain.gpx', 'r')
gpx = gpxpy.parse(gpx_file)
for track in gpx.tracks:
    for segment in track.segments:
        for point in segment.points:
			timestamp = int(point.time.strftime('%s')) - int(dt.datetime(1970, 1, 1).strftime('%s'))
			lon.append(str(point.longitude))
			lat.append(str(point.latitude))
			t.append(timestamp)


row_key = 'Chloe'+str(t[-1])
table_name='trains'
family_names=['locations']
max_versions_list=[10]
hostname=[]
tstamp = t
data=[lat,lon]
col_names=['lat','lon']



#Open connection to HBase and create table

connection=happy.make_connection(hostname)
happy.delete_table('trains',connection)
print "making table" 
try:
	table=happy.create_table(table_name, family_names, max_versions_list, connection)  
except:
	print 'table already exists'

#Put data into HBase 

print "test put function"    
happy.hbase_put(row_key, family_names, col_names, data, table, tstamp)


#Get data out of HBase 

print 'test hbase_scan'
keys, data = happy.hbase_scan('Chloe',table)
print keys, data

print 'test hbase_get'
keys, data = happy.hbase_get([row_key],table)
print keys, data
