import MySQLdb, MySQLdb.cursors
import numpy, pylab
import time, datetime
import pickle, os


    
    
class dbcomm():
    def __init__(self, url='hte-dbsv-01.caltech.edu',user='',password='',db='hte_echemdrop_proto'):
        self.db=MySQLdb.connect(url, user, password, db, cursorclass = MySQLdb.cursors.SSCursor)
        self.cursor=self.db.cursor()

    def execute_getsinglerow(self, query):
        self.cursor.execute(query)
        for row in self.cursor:break#assumes only 1 row
        for xrow in self.cursor:continue
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

    def getallrecordids(self, filterfield, filterval, valcvtcstr=None):
        filterval=self.dbstrcvt(filterval, valcvtcstr)
        data_ids=[]
        if filterfield is None:
            query = 'select id from data'
        else:
            query = 'select id from data where %s = %s' %(filterfield, filterval)
        self.cursor.execute(query)
        for row in self.cursor:
            data_ids.append(row[0])
        return data_ids
    
    def getallplateids(self, filterfield, filterval, valcvtcstr=None):
        filterval=self.dbstrcvt(filterval, valcvtcstr)
        data_ids=[]
        if filterfield is None:
            query = 'select plate_id from data'
        else:
            query = 'select plate_id from data where %s = %s' %(filterfield, filterval)
        self.cursor.execute(query)
        for row in self.cursor:
            data_ids.append(row[0])
        return data_ids

    def getrecordids_created(self, plateid, dt0, dt1):
        data_ids=[]
        query = 'select id from data where plate_id = %d and created_at >= "%s" and created_at < "%s"' % (plateid, str(dt0),str(dt1)) 
        self.cursor.execute(query)
        for row in self.cursor:
            data_ids.append(row[0])
        return data_ids
        
    def getrowdict_fields(self, filterfield, filterval, fields, recordnum=0, valcvtcstr=None):
        #print recordnum
        filterval=self.dbstrcvt(filterval, valcvtcstr)
        query='select %s from data where %s = %s limit %d, 1;'% (', '.join(fields), filterfield, filterval, recordnum)
        row=self.execute_getsinglerow(query)
        d=dict([tup for tup in zip(fields,row)])
        return d
    
    def getdlist_fields(self, filterfield, filterval, fields, recnums=None, valcvtcstr=None):
        filterval=self.dbstrcvt(filterval, valcvtcstr)
        dlist=[]
        if recnums is None:
            recnums=self.getallrecordids(filterfield, filterval)
        for data_id in recnums:
            query = 'select %s from data where id = %d;' % (', '.join(fields), data_id)
            self.cursor.execute(query)
            for row in self.cursor:
                dlist.append(dict([tup for tup in zip(fields,row)]))
        return dlist

    def getarrd_scalarfields(self, filterfield, filterval, fields, recnums=None, valcvtcstr=None):
        filterval=self.dbstrcvt(filterval, valcvtcstr)
        arrd=dict([(f, []) for f in fields])
        if recnums is None:
            recnums=self.getallrecordids(filterfield, filterval)
        for data_id in recnums:
            query = 'select %s from data where id = %d;' % (', '.join(fields), data_id)
            self.cursor.execute(query)
            garbage=[arrd[f].append(v) for row in self.cursor for f, v in zip(fields,row)]
        for k, v in arrd.iteritems():
            arrd[k]=numpy.array(v)
        return arrd
        
    def getdatapath_id(self, id):
        d=self.getrowdict_fields('id', id, ['plate_id','dc_data__t_v_a_c_i'], valcvtcstr='%d')
        p=os.path.join(os.path.join('J:/hte_echemdrop_proto/data','%d' %d['plate_id']),d['dc_data__t_v_a_c_i'])
        return p



#def getrecnums_created(plateid, dt0, dt1):
#    dlist=dbc.getdlist_fields('plate_id', plateid, ['created_at'])
#    return [count for count, d in enumerate(dlist) if d['created_at']>=dt0 and d['created_at']<dt1]
#    
#db, cursor=dbconnect()
dbc=dbcomm(user='',password='',db='hte_echemdrop_proto')

tstart=time.time()
print tstart


if 0:
    plate_ids=dbc.getallplateids(None, None)
elif 0:
    ids=dbc.getallrecordids(None, None)
    d=dbc.getrowdict_fields('id', ids[0], ['plate_id','dc_data__t_v_a_c_i'], valcvtcstr='%d')
elif 1:
    ids=dbc.getallrecordids(None, None)
    d=dbc.getrowdict_fields('id', 40380, ['plate_id','dc_data__t_v_a_c_i'], valcvtcstr='%d')
    p=os.path.join(os.path.join('J:/hte_echemdrop_proto/data','%d' %d['plate_id']),d['dc_data__t_v_a_c_i'])
    f=open(p, mode='r')
    s=f.read()
    f.close()
    
elif 0:#test
    fields=['sample_no','created_at']
    n=dbc.getnumrecords('plate_id', 3)
    d=dbc.getrowdict_fields('plate_id', 3, fields, 7)
elif 0:#print created_at to find data
    fields=['sample_no','created_at']
    dlist=dbc.getdlist_fields('plate_id', 3, fields)    
    for count,d in enumerate(dlist):
        print '\t'.join([`count`, `d['sample_no']`,`d['created_at']`])
elif 1:#get no plate data
    fields=['id', 'plate_id', 'sample_no', 'sample_code', 'uv_data__l_r', 'created_at']
    dlist=dbc.getdlist_fields('sample_code', -2, fields)    
    for count,d in enumerate(dlist):
        print '\t'.join([`count`, `d['sample_no']`,`d['created_at']`])
    p='C:/Users/Gregoire/Documents/CaltechWork/UVvisdata/201210NiFeCoTi_P/noplate.pck'
elif 0:#get plate3 data
    recnums=dbc.getrecordids_created(3, datetime.datetime(2012, 11, 20, 8, 48, 56), datetime.datetime(2012, 11, 20, 14, 21, 31))
    fields=['id', 'plate_id', 'sample_no', 'sample_code', 'uv_data__l_r', 'created_at']
    #recnums=recnums[:4]
    dlist=dbc.getdlist_fields('plate_id', 3, fields, recnums=recnums)
    p='C:/Users/Gregoire/Documents/CaltechWork/UVvisdata/201210NiFeCoTi_P/plate3.pck'
    print len(dlist)
elif 0:#get plate2 data
    recnums=dbc.getrecordids_created(2, datetime.datetime(2012, 11, 19, 18, 9, 57), datetime.datetime(2012, 11, 20, 0, 12, 25))
    fields=['id', 'plate_id', 'sample_no', 'sample_code', 'uv_data__l_r', 'created_at']
    #recnums=recnums[:40]
    dlist=dbc.getdlist_fields('plate_id', 2, fields, recnums=recnums)
    p='C:/Users/Gregoire/Documents/CaltechWork/UVvisdata/201210NiFeCoTi_P/plate2.pck'
    print len(dlist)
elif 0:#get plate1 data
    recnums=dbc.getrecordids_created(1, datetime.datetime(2012, 11, 19, 11, 20, 41), datetime.datetime(2012, 11, 19, 17, 21, 39))
    fields=['id', 'plate_id', 'sample_no', 'sample_code', 'uv_data__l_r', 'created_at']
    #recnums=recnums[:4]
    dlist=dbc.getdlist_fields('plate_id', 1, fields, recnums=recnums)
    p='C:/Users/Gregoire/Documents/CaltechWork/UVvisdata/201210NiFeCoTi_P/plate1.pck'    
    print len(dlist)
elif 0:#get plate3 1strow data
    recnums=dbc.getrecordids_created(3, datetime.datetime(2012, 11, 20, 15, 21, 28), datetime.datetime(2012, 11, 20, 15, 37, 30))
    fields=['id', 'plate_id', 'sample_no', 'sample_code', 'uv_data__l_r', 'created_at']
    #recnums=recnums[:4]
    dlist=dbc.getdlist_fields('plate_id', 3, fields, recnums=recnums)
    p='C:/Users/Gregoire/Documents/CaltechWork/UVvisdata/201210NiFeCoTi_P/plate3_firstrowneedstobeedited.pck'
    print len(dlist)
    l=numpy.array([d['sample_no'] for d in dlist])
    s=set([d['sample_no'] for d in dlist])

    for count, sv in enumerate(s):
        if count%8!=7 and ((l==sv).sum())>3:
            num=0
            for d in dlist:
                if d['sample_no']==sv:
                    if num<3:
                        d['sample_code']=0
                    num+=1
dbc.db.close()

    
if 0:
    f=open(p, mode='w')
    pickle.dump(dlist, f)
    f.close()

print time.time(), time.time()-tstart

