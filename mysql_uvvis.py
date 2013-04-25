import MySQLdb, MySQLdb.cursors
import numpy, pylab
import time, datetime
import pickle

class dbcomm():
    def __init__(self):
        self.db=MySQLdb.connect('hte-dbsv-01.caltech.edu','mmarcin','lab231','hte_uvis_proto', cursorclass = MySQLdb.cursors.SSCursor)
        self.cursor=self.db.cursor()

    def execute_getsinglerow(self, query):
        self.cursor.execute(query)
        for row in self.cursor:continue#assumes only 1 row
        return row

    def dbstrcvt(self, val, cvtstr=None):
        if not isinstance(val, str):
            if not cvtstr is None:
                val= cvtstr %val
            else:
                val=`val`
        return val
        
    def getnumrecords(self, filterfield, filterval, valcvtcstr=None):
        filterval=self.dbstrcvt(filterval, valcvtcstr)
        query = 'select count(*) from data where %s = %s' %(filterfield, filterval)
        row=self.execute_getsinglerow(query)
        return row[0]

    def getrowdict_fields(self, filterfield, filterval, fields, recordnum, valcvtcstr=None):
        #print recordnum
        filterval=self.dbstrcvt(filterval, valcvtcstr)
        query='select %s from data where %s = %s limit %d, 1;'% (', '.join(fields), filterfield, filterval, recordnum)
        row=self.execute_getsinglerow(query)
        d=dict([tup for tup in zip(fields,row)])
        return d
    
    def getdlist_fields(self, filterfield, filterval, fields, recnums=None, valcvtcstr=None):
        filterval=self.dbstrcvt(filterval, valcvtcstr)
        if recnums is None:
            n=self.getnumrecords(filterfield, filterval)
            recnums=range(n)
        dlist=[self.getrowdict_fields(filterfield, filterval, fields, i) for i in recnums]
        return dlist




def getrecnums_created(plateid, dt0, dt1):
    dlist=dbc.getdlist_fields('plate_id', plateid, ['created_at'])
    return [count for count, d in enumerate(dlist) if d['created_at']>=dt0 and d['created_at']<dt1]
    
#db, cursor=dbconnect()
dbc=dbcomm()

tstart=time.time()
print tstart

if 0:#test
    fields=['sample_no','created_at']
    n=dbc.getnumrecords('plate_id', 3)
    d=dbc.getrowdict_fields('plate_id', 3, fields, 7)
elif 1:#print created_at to find data
    fields=['sample_no','created_at']
    dlist=dbc.getdlist_fields('plate_id', 3, fields)    
    for count,d in enumerate(dlist):
        print '\t'.join([`count`, `d['sample_no']`,`d['created_at']`])
elif 0:#get plate3 data
    recnums=getrecnums_created(3, datetime.datetime(2012, 11, 20, 8, 48, 56), datetime.datetime(2012, 11, 21, 8, 48, 56))
    fields=['id', 'plate_id', 'sample_no', 'sample_code', 'uv_data__l_r', 'created_at']
    recnums=recnums[:4]
    dlist=dbc.getdlist_fields('plate_id', 3, fields, recnums=recnums)
    p='C:/Users/Gregoire/Documents/CaltechWork/UVvisdata/201210NiFeCoTi_P/plate3.pck'
    print len(dlist)
elif 0:#get plate2 data
    recnums=getrecnums_created(2, datetime.datetime(2012, 11, 19, 18, 9, 57), datetime.datetime(2012, 11, 20, 0, 12, 25))
    fields=['id', 'plate_id', 'sample_no', 'sample_code', 'uv_data__l_r', 'created_at']
    #recnums=recnums[:40]
    dlist=dbc.getdlist_fields('plate_id', 2, fields, recnums=recnums)
    p='C:/Users/Gregoire/Documents/CaltechWork/UVvisdata/201210NiFeCoTi_P/plate2.pck'
    print len(dlist)
elif 0:#get plate1 data
    recnums=getrecnums_created(1, datetime.datetime(2012, 11, 19, 11, 20, 41), datetime.datetime(2012, 11, 19, 17, 21, 39))
    fields=['id', 'plate_id', 'sample_no', 'sample_code', 'uv_data__l_r', 'created_at']
    #recnums=recnums[:4]
    dlist=dbc.getdlist_fields('plate_id', 1, fields, recnums=recnums)
    p='C:/Users/Gregoire/Documents/CaltechWork/UVvisdata/201210NiFeCoTi_P/plate1.pck'    
    print len(dlist)

    
dbc.db.close()

    
if 0:
    f=open(p, mode='w')
    pickle.dump(dlist, f)
    f.close()

print time.time(), time.time()-tstart
