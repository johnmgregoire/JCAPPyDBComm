import MySQLdb, MySQLdb.cursors
import pickle, time

tstart=time.time()
print tstart

dlist = []
data_ids = []

db = MySQLdb.connect('hte-dbsv-01.caltech.edu','mmarcin','lab231','hte_uvis_proto', cursorclass = MySQLdb.cursors.SSCursor)
cursor = db.cursor()
query = 'select id from data where plate_id = %d and created_at >= "%s" and created_at < "%s"' % (1, '2012-11-19 11:20:41','2012-11-19 17:21:39' ) 
cursor.execute(query)
for row in cursor:
  data_ids.append(row[0])
    
fields = ['id', 'plate_id', 'sample_no', 'sample_code', 'uv_data__l_r', 'created_at']
for data_id in data_ids:
  query = 'select %s from data where id = %d;' % (', '.join(fields), data_id)

  cursor.execute(query)
  for row in cursor:
    dlist.append(dict([tup for tup in zip(fields,row)]))

#f = open('./plate1.pck', mode='w')
#pickle.dump(dlist, f)
#f.close()

print time.time()-tstart
