# -*- coding: utf-8 -*-
"""
Created on Sun Aug 12 21:45:19 2018

@author: WUXIAOLEI LIMING
"""
import os
import sys
import datetime
import argparse
import traceback
from pyspark.sql import HiveContext
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql.types import *


fct_rowname = ['tano', 'cserialno', 'oricserialno', 'customerid', 'custtype', 'sk_fundaccount',
'sk_tradeaccount', 'agencyno', 'netno', 'bk_fundcode', 'sharetype', 'sk_agency',
'regdate', 'effective_from', 'effective_to', 'orinetvalue', 'orishares',
'oricost', 'lastshares', 'shrchg', 'shares', 'sharesofnocost', 'cost', 'incmofnocost',
'income', 'costofincome',
'totalcost', 'totalincmofnocost', 'totalincome', 'totalcostofincome', 'bourseflag',
'srcsys', 'batchno', 'sk_audit',
'inserttime', 'updatetime', 'shrchgofnocost', 'bk_tradetype', 'ori_bk_tradetype']

fct_schema = StructType([StructField(row, StringType())for row in fct_rowname])

#sys.path.append(os.path.abspath(sys.path[0]) + os.path.sep + "../utils")
import funcinc


conf = SparkConf().setAppName("Yingkui pyspark")
sc = SparkContext(conf=conf)
hiveContext = HiveContext(sc)
sc.setLogLevel("WARN")


class Driver(object):

    def __init__(self, startdate, enddate, tano):
        self.startdate = startdate
        self.enddate = enddate
        self.tano = tano
        self.database = "cmfods"

    def task(self):
        try:
            print 'program start ',str(self.startdate)
            # 一，用户存在目标表
            res1 = self.tmp_exist_fact()
            # 二，用户不存在
            res2 = self.tmp_not_exist_fact()
            
            res = res1.union(res2)
            print 'res...............................'
            print res.collect()
            
            # truncate table
            sql_truncate = "truncate table {database}.mid_fact_custincomechg_detail_inc".format(database=self.database)
            print "Exec sql:{}".format(sql_truncate)
            hiveContext.sql(sql_truncate)
            print 'rdd to dataframe...'
            res = res.toDF(schema=fct_schema)
            print "Insert overwrite table {database}.mid_fact_custincomechg_detail_inc".format(database=self.database)
            mid_table='{database}.mid_fact_custincomechg_detail_inc'.format(database=self.database)
            # tmp表用户在fact表里存在
            res.write.saveAsTable(mid_table, mode='append')
            
            sql_4 = '''INSERT overwrite TABLE {database}.FACT_CUSTINCOMECHG_DETAIL 
                      SELECT  * from {database}.MID_FACT_CUSTINCOMECHG_DETAIL_INC '''.format(
            database=self.database)
            print "Exec sql:{}".format(sql_4)
            hiveContext.sql(sql_4)  ##将扣减新增及未扣减数据插入到最终的目标表
            print 'program finish...'
            
        except Exception:
            exe = traceback.format_exc()
            print exe
            sys.exit(1)    
            
    def tmp_exist_fact(self):
        print 'tmp_exist_fact..........................'
        try:
            # 把FACT_CUSTINCOMECHG_DETAIL(客户盈亏明细)按给出的起始日期和终止日期把满足条件的数据先插入一张零时表
            sql_1 = "INSERT overwrite table {database}.TMP_FCT_CUSTINCMCHG_DTL_DEL      " \
                    "         SELECT A.TANO                                  " \
                    "               ,A.CSERIALNO                             " \
                    "               ,A.ORICSERIALNO                          " \
                    "               ,A.EFFECTIVE_FROM                        " \
                    "           FROM {database}.FACT_CUSTINCOMECHG_DETAIL A             " \
                    "          WHERE A.EFFECTIVE_FROM >= '{startdate}' AND A.TANO = '{tano}' ".format(
                database=self.database, startdate=self.startdate, tano=self.tano)
            print "Exec sql:{}".format(sql_1)
            hiveContext.sql(sql_1)
            # 查询零时表里是否有数据
            sql_2 = "select 1 from {database}.TMP_FCT_CUSTINCMCHG_DTL_DEL limit 1".format(database=self.database)
            print "Exec sql:{}".format(sql_2)
            tmpdf = hiveContext.sql(sql_2)
            V_ROWCNT = tmpdf.count()
            print "V_ROWCNT="+str(V_ROWCNT)
            if V_ROWCNT > 0:
                print "rowcount > 0"
                sql_3 = '''SELECT tano,
                                  cserialno,                                   
                                  oricserialno,                           
                                  customerid,                             
                                  custtype,                               
                                  sk_fundaccount,                         
                                  sk_tradeaccount,                        
                                  agencyno,                               
                                  netno,                                  
                                  bk_fundcode,                            
                                  sharetype,                              
                                  sk_agency,                              
                                  regdate,                                
                                  effective_from,                         
                                  CASE WHEN effective_to BETWEEN '{startdate}'  
                                       and '{enddate}' and tano = '{tano}'        
                                            THEN 99991231                       
                                           ELSE effective_to                    
                                       END AS effective_to,                     
                                       orinetvalue,                             
                                       orishares,                               
                                       oricost,                                 
                                       lastshares,                              
                                       shrchg,                                  
                                       shares,                                  
                                       sharesofnocost,                          
                                       cost,
                                       incmofnocost,                            
                                       income,                                  
                                       costofincome,                            
                                       totalcost,                               
                                       totalincmofnocost,                       
                                       totalincome,                             
                                       totalcostofincome,                       
                                       bourseflag,                              
                                       srcsys,                                  
                                       batchno,                                 
                                       sk_audit,                                
                                       inserttime,                              
                                       updatetime,                              
                                       shrchgofnocost,                          
                                       bk_tradetype,                            
                                       ori_bk_tradetype                         
                                FROM {database}.fact_custincomechg_detail      
                                  WHERE effective_from < '{startdate}'  
                                union all     
                                   SELECT tano,                                
                                       cserialno,                              
                                       oricserialno,                           
                                       customerid,                             
                                       custtype,                               
                                       sk_fundaccount,                         
                                       sk_tradeaccount,                        
                                       agencyno,                               
                                       netno,                                  
                                       bk_fundcode,                            
                                       sharetype,                              
                                       sk_agency,                              
                                       regdate,                                
                                       effective_from,                         
                                       effective_to,                           
                                       orinetvalue,                            
                                       orishares,                              
                                       oricost,                                
                                       lastshares,                             
                                       shrchg,                                 
                                       shares,                                 
                                       sharesofnocost,                         
                                       cost,
                                       incmofnocost,                           
                                       income,                                 
                                       costofincome,                           
                                       totalcost,                              
                                       totalincmofnocost,                      
                                       totalincome,                            
                                       totalcostofincome,                      
                                       bourseflag,                             
                                       srcsys,                                 
                                       batchno,                                
                                       sk_audit,                               
                                       inserttime,                             
                                       updatetime,                             
                                       shrchgofnocost,                         
                                       bk_tradetype,                           
                                       ori_bk_tradetype   
                                from {database}.fact_custincomechg_detail 
                                where effective_from >=  '{startdate}' 
                                   and tano <> '{tano}' '''.format(
                                database=self.database, startdate=self.startdate, enddate=self.enddate, tano=self.tano)
            else:
                sql_3 = '''SELECT tano,                                        
                                       cserialno,                              
                                       oricserialno,                           
                                       customerid,                             
                                       custtype,                               
                                       sk_fundaccount,                         
                                       sk_tradeaccount,                        
                                       agencyno,                               
                                       netno,                                  
                                       bk_fundcode,                            
                                       sharetype,                              
                                       sk_agency,                              
                                       regdate,                                
                                       effective_from,                         
                                       effective_to,                           
                                       orinetvalue,                            
                                       orishares,                              
                                       oricost,                                
                                       lastshares,                             
                                       shrchg,                                 
                                       shares,                                 
                                       sharesofnocost,                         
                                       cost,
                                       incmofnocost,                           
                                       income,                                 
                                       costofincome,                           
                                       totalcost,                              
                                       totalincmofnocost,                      
                                       totalincome,                            
                                       totalcostofincome,                      
                                       bourseflag,                             
                                       srcsys,                                 
                                       batchno,                                
                                       sk_audit,                               
                                       inserttime,                             
                                       updatetime,                             
                                       shrchgofnocost,                         
                                       bk_tradetype,                           
                                       ori_bk_tradetype   
                                from {database}.fact_custincomechg_detail
                '''.format(database=self.database)
            rdd_fct = hiveContext.sql(sql_3).rdd
            print rdd_fct.collect()
            sql_2 = '''select tano, 
                                  cserialno, 
                                  customerid, 
                                  custtype, 
                                  sk_fundaccount, 
                                  sk_tradeaccount, 
                                  agencyno, 
                                  netno, 
                                  bk_fundcode, 
                                  sharetype, 
                                  bonustype,
                                  sk_agency, 
                                  sk_confirmdate, 
                                  shares,
                                  bourseflag, 
                                  amount, 
                                  netvalue, 
                                  incflag, 
                                  bk_tradetype, 
                                  incomerule, 
                                  srcsys, 
                                  batchno                                           
                                from {database}.tmp_fact_custincomechg_detail t 
                              where t.tano='{tano}'
                              and t.sk_confirmdate between '{startdate}' and '{enddate}'
                              '''.format(database=self.database, startdate=self.startdate, enddate=self.enddate, tano=self.tano)
            ###################tmp表用户在fact表里存在表############
            print "Exec sql2:{}".format(sql_2)
            print '***********'
            print hiveContext.sql(sql_2).show
            print '***********'
            wf = hiveContext.sql(sql_2).rdd
            if wf.isEmpty():
                print 'tmp_exist_fact isEmpty.'
                return rdd_fct
            print wf.collect()
                        
            print "Rdd-wf1 transform:filter..."
            #份额变化类型为增加，且为分红交易
            wf1 = wf.filter(funcinc.fil1)
            print wf1.collect()
            print "Rdd-wf2 transform:filter..."
            #份额变化类型为增加，且不为分红交易
            wf2 = wf.filter(funcinc.fil2)
            print wf2.collect()
            print "Rdd-wf3 transform:filter..."
            #份额变化类型为减少
            wf3 = wf.filter(funcinc.fil3)
            print wf3.collect()
            
            print "Rdd-wf1 transform:groupByKey..."
            wf1 = wf1.map(
                lambda x: ((x.sk_fundaccount, x.agencyno, x.sk_tradeaccount, x.bk_fundcode, x.sharetype), x)
            ).groupByKey()
            print "Rdd-wf2 transform:groupByKey..."
            wf2 = wf2.map(
                lambda x: ((x.sk_fundaccount, x.agencyno, x.sk_tradeaccount, x.bk_fundcode, x.sharetype), x)
            ).groupByKey()
            print "Rdd-wf3 transform:groupByKey..."
            wf3 = wf3.map(
                lambda x: ((x.sk_fundaccount, x.agencyno, x.sk_tradeaccount, x.bk_fundcode, x.sharetype), x)
            ).groupByKey()
            print '######',wf3.collect()
            ###########################END##########################
            
            for i in range(1, 4):
                print 'Run.Driver.task.for loop',i,datetime.datetime.today()
                if i == 1:
                    if wf1.isEmpty():
                        print 'isEmpty.Run.Driver.task.for loop', i, datetime.datetime.today()
                        continue
                    else:
                        print "Rdd-wf1 action:flatMapValues, function:funcinc.func1..."
                        result = wf1.flatMapValues(funcinc.func1).values()
                        print "Append data to table {database}.fact_custincomechg_detail".format(database=self.database)
                        rdd_fct = rdd_fct.union(result)
                        print 'Finish.Run.Driver.task.for loop', i, datetime.datetime.today()
                elif i == 2:
                    if wf2.isEmpty():
                        print 'isEmpty.Run.Driver.task.for loop', i, datetime.datetime.today()
                        continue
                    else:
                        print "Rdd-wf2 action:flatMapValues, function:funcinc.func2..."
                        result2 = wf2.flatMapValues(funcinc.func2).values()
                        print "Append data to table {database}.fact_custincomechg_detail".format(database=self.database)
                        rdd_fct = rdd_fct.union(result2)
                        print 'Finish.Run.Driver.task.for loop', i, datetime.datetime.today()
                elif i == 3:
                    if wf3.isEmpty():
                        print 'isEmpty.Run.Driver.task.for loop', i, datetime.datetime.today()
                        continue
                    else:
                        print '3333333333333333333333333333'
                        print rdd_fct.collect()
                        print "Rdd-fct transform:groupByKey..."
                        ############################################################
                        df = rdd_fct.map(lambda x: ((x.sk_fundaccount, x.agencyno, x.sk_tradeaccount, x.bk_fundcode, x.sharetype), x)).groupByKey()
                        ##############################################################
                        print '######',df.collect()
                        print "Rdd-df-wf3 transform:Join..."
                        dwf = df.join(wf3)
                        print dwf.collect()
                        print "Rdd-dwf transform:reduceByKey, function:funcinc.red1..."
                        rdd1 = dwf.reduceByKey(funcinc.red1)
                        print rdd1.collect()
                        print "Rdd-dwf action:flatMapValues, function:funcinc.func3..."
                        rdd_fct = rdd1.flatMapValues(funcinc.func3).values()
                        print 'Finish.Run.Driver.task.for loop', i, datetime.datetime.today()
            return rdd_fct        
        except Exception:
            exe = traceback.format_exc()
            print exe
            sys.exit(1)
            
    def tmp_not_exist_fact(self):
        print 'tmp_not_exist_fact..........................'
        try:
            sql_3 = '''SELECT  t1.tano,                                        
                               t1.cserialno,                              
                               t1.oricserialno,                           
                               t1.customerid,                             
                               t1.custtype,                               
                               t1.sk_fundaccount,                         
                               t1.sk_tradeaccount,                        
                               t1.agencyno,                               
                               t1.netno,                                  
                               t1.bk_fundcode,                            
                               t1.sharetype,                              
                               t1.sk_agency,                              
                               t1.regdate,                                
                               t1.effective_from,                         
                               t1.effective_to,                           
                               t1.orinetvalue,                            
                               t1.orishares,                              
                               t1.oricost,                                
                               t1.lastshares,                             
                               t1.shrchg,                                 
                               t1.shares,                                 
                               t1.sharesofnocost,                         
                               t1.cost,
                               t1.incmofnocost,                           
                               t1.income,                                 
                               t1.costofincome,                           
                               t1.totalcost,                              
                               t1.totalincmofnocost,                      
                               t1.totalincome,                            
                               t1.totalcostofincome,                      
                               t1.bourseflag,                             
                               t1.srcsys,                                 
                               t1.batchno,                                
                               t1.sk_audit,                               
                               t1.inserttime,                             
                               t1.updatetime,                             
                               t1.shrchgofnocost,                         
                               t1.bk_tradetype,                           
                               t1.ori_bk_tradetype   
                from {database}.fact_custincomechg_detail t1
                left join {database}.tmp_fact_custincomechg_detail t2
                on t1.sk_fundaccount=t2.sk_fundaccount
                and t1.agencyno=t2.agencyno
                and t1.sk_tradeaccount=t2.sk_tradeaccount
                and t1.bk_fundcode=t2.bk_fundcode
                and t1.sharetype=t2.sharetype
                where (t2.sk_fundaccount is null
                or t2.agencyno is null
                or t2.sk_tradeaccount is null
                or t2.bk_fundcode is null
                or t2.sharetype is null)'''.format(database=self.database)
            rdd_fct = hiveContext.sql(sql_3).rdd
            
            sql_22='''select t1.tano, 
                      t1.cserialno, 
                      t1.customerid, 
                      t1.custtype, 
                      t1.sk_fundaccount, 
                      t1.sk_tradeaccount, 
                      t1.agencyno, 
                      t1.netno, 
                      t1.bk_fundcode, 
                      t1.sharetype, 
                      t1.bonustype,
                      t1.sk_agency, 
                      t1.sk_confirmdate, 
                      t1.shares,
                      t1.bourseflag, 
                      t1.amount, 
                      t1.netvalue, 
                      t1.incflag, 
                      t1.bk_tradetype, 
                      t1.incomerule, 
                      t1.srcsys, 
                      t1.batchno                                           
                    from {database}.tmp_fact_custincomechg_detail t1
                    left join {database}.fact_custincomechg_detail t2
                    on t1.sk_fundaccount=t2.sk_fundaccount
                    and t1.agencyno=t2.agencyno
                    and t1.sk_tradeaccount=t2.sk_tradeaccount
                    and t1.bk_fundcode=t2.bk_fundcode
                    and t1.sharetype=t2.sharetype
                  where t1.tano='{tano}'
                  and t1.sk_confirmdate between '{startdate}' and '{enddate}'
                  and (t2.sk_fundaccount is null
                  or t2.agencyno is null
                  or t2.sk_tradeaccount is null
                  or t2.bk_fundcode is null
                  or t2.sharetype is null)
                    '''.format(database=self.database, startdate=self.startdate, enddate=self.enddate, tano=self.tano)
            ###################tmp表用户不在fact表里存在表##########
            print "Exec sql22:{}".format(sql_22)
            wf_2 = hiveContext.sql(sql_22).rdd
            print '*********'
            print wf_2.collect()
            print '*********'
            if wf_2.isEmpty():
                print 'tmp_not_exist_fact isEmpty.'
                return rdd_fct
            print wf_2.collect()
            
            print "Rdd-wf1_2 transform:filter..."
            #份额变化类型为增加，且为分红交易
            wf1_2 = wf_2.filter(funcinc.fil1)
            print wf1_2.collect()
            print "Rdd-wf2_2 transform:filter..."
            #份额变化类型为增加，且不为分红交易
            wf2_2 = wf_2.filter(funcinc.fil2)
            print wf2_2.collect()
            print "Rdd-wf3_2 transform:filter..."
            #份额变化类型为减少
            wf3_2 = wf_2.filter(funcinc.fil3)
            print wf3_2.collect()
            
            print "Rdd-wf1_2 transform:groupByKey..."
            wf1_2 = wf1_2.map(
                lambda x: ((x.sk_fundaccount, x.agencyno, x.sk_tradeaccount, x.bk_fundcode, x.sharetype), x)
            ).groupByKey()
            print "Rdd-wf2_2 transform:groupByKey..."
            wf2_2 = wf2_2.map(
                lambda x: ((x.sk_fundaccount, x.agencyno, x.sk_tradeaccount, x.bk_fundcode, x.sharetype), x)
            ).groupByKey()
            print "Rdd-wf3_2 transform:groupByKey..."
            wf3_2 = wf3_2.map(
                lambda x: ((x.sk_fundaccount, x.agencyno, x.sk_tradeaccount, x.bk_fundcode, x.sharetype), x)
            ).groupByKey()
            print '######',wf3_2.collect()
            ##########################END#############################
            
            if wf1_2.isEmpty():
                print 'isEmpty.Run.Driver.task ', datetime.datetime.today()
            else:
                print "Rdd-wf1 action:flatMapValues, function:funcinc.func1..."
                result = wf1_2.flatMapValues(funcinc.func1).values()
                print "Append data to table {database}.fact_custincomechg_detail".format(database=self.database)
                rdd_fct = rdd_fct.union(result)
                print 'Finish.Run.Driver.task ', datetime.datetime.today()
            if wf2_2.isEmpty():
                print 'isEmpty.Run.Driver.task ', datetime.datetime.today()
            else:
                print "Rdd-wf2 action:flatMapValues, function:funcinc.func2..."
                result2 = wf2_2.flatMapValues(funcinc.func2).values()
                print "Append data to table {database}.fact_custincomechg_detail".format(database=self.database)
                rdd_fct = rdd_fct.union(result2)
                print 'Finish.Run.Driver.task ',datetime.datetime.today()
            if wf3_2.isEmpty():
                print 'isEmpty.Run.Driver.task ',datetime.datetime.today()
            else:                    
                print "Rdd-dwf action:flatMapValues, function:funcinc.func3..."
                result3 = wf3_2.flatMapValues(funcinc.func4).values()
                print "Append data to table {database}.fact_custincomechg_detail".format(database=self.database)
                rdd_fct = rdd_fct.union(result3)
                print 'Finish.Run.Driver.task ',datetime.datetime.today()
            return rdd_fct
        except Exception:
            exe = traceback.format_exc()
            print exe
            sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='manual to this script')
    parser.add_argument("--startdate", required=True, type=str, default=None)
    parser.add_argument("--enddate", required=True, type=str, default=None)
    parser.add_argument("--tano", required=True, type=str, default=None)
    args = parser.parse_args()
    startdate = args.startdate
    enddate = args.enddate
    tano = args.tano
    # 定义传参
    Dr = Driver(startdate, enddate, tano)
    d_startdate = str(startdate)
    d_enddate = str(enddate)
    d_sp_startdate = datetime.datetime.strptime(d_startdate, '%Y%m%d')
    d_sp_enddate = datetime.datetime.strptime(d_enddate, '%Y%m%d')
    oneday = datetime.timedelta(days=1)
    while (d_sp_startdate < d_sp_enddate + oneday):
        v_enddate = int(d_sp_startdate.strftime('%Y%m%d'))
        v_startdate = int(d_sp_startdate.strftime('%Y%m%d'))
        Dr.startdate=v_startdate
        Dr.enddate=v_enddate
        Dr.task()
        d_sp_startdate = d_sp_startdate + oneday
