import happybase

def make_connection(hostname):
	if hostname:
		connection = happybase.Connection(hostname)
	else:
		connection = happybase.Connection()
	return connection


def create_table(table_name, family_names, max_versions_list, connection):
	families={}
	for i,family_name in enumerate(family_names):
		families[family_name]=dict(max_versions=max_versions_list[i])
	connection.create_table(table_name,families)
	table=connection.table(table_name)
	return table



# def hbase_put(row_key, lat, lon, acc, table, tstamp):
# 	# Writes data for a single row_key but multiple lat/lon/accs
# 	# Single column family "locations"
# 	# Column qualifiers are lat+timestamp, lon_timestamp, acc+timestamp
# 	# Applies the last timestamp as timestamp for all entries 
# 	data={}
# 	for i in range(0,len(lat)):
# 		data['locations:lat'+str(tstamp[i])]=lat[i]
# 		data['locations:lon'+str(tstamp[i])]=lon[i]
# 		data['locations:acc'+str(tstamp[i])]=acc[i]
# 	b = table.batch(timestamp=tstamp[-1])
# 	b.put(row_key, data)
# 	b.send()

def hbase_put(row_key, family_names, col_names, data, table, tstamp):
	# Writes data for a single row_key but multiple data
	# Column qualifiers are col_qual_names+tstamp
	# Applies the last timestamp as timestamp for all entries 
	data_entry={}
	for family in family_names:
		for i in range(0,len(col_names)):
			for j in range(0,len(col_names[0])):
				data_entry[family+':'+col_names[i]+str(tstamp[j])]=data[i][j]
	b = table.batch(timestamp=tstamp[-1])
	b.put(row_key, data_entry)
	b.send()

def hbase_scan(prefix, table):
	# retrieves data for rows beginning in prefix.
	keys=[]
	data=[]
	for key,datum in table.scan(row_prefix=prefix):
		keys.append(key)
		data.append(datum)
	return keys, data


def hbase_get(row_names, table):
	# retrieves data from one or multiple rows. row_names must be a list
	keys=[]
	data=[]
	for key,datum in table.rows(row_names):
		keys.append(key)
		data.append(datum)
	return keys, data

def delete_row(row_names,table):
	for row_name in row_names:
		table.delete(row_name)

def delete_table(table_name,connection):
	connection.delete_table(table_name,disable=True)

