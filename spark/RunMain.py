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
from pyspark.storagelevel import StorageLevel
import gc

os.environ['PATHONPATH']='python2'

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

# print 'conf'
conf = SparkConf().setAppName("Yingkui pyspark")
conf.set('spark.sql.hive.convertMetastoreParquet','false')
conf.set('spark.sql.parquet.writeLegacyFormat','true')
# print 'sc'
sc = SparkContext(conf=conf)
# print 'hiveContext'
hiveContext = HiveContext(sc)
sc.setLogLevel("WARN")
# print 'sc.'
# 自定义异常类
class CustomError(Exception):
    def __init__(self,ErrotInfo):
        super().__init__(self)
        self.ErrotInfo=ErrotInfo
    def __str__(self):
        return self.ErrotInfo

class Driver(object):

    def __init__(self, startdate, enddate, tano):
        self.startdate = startdate
        self.enddate = enddate
        self.tano = tano
        self.database = "az_dcdw"
        self.dim_product_database='az_dcdw'

    def task(self):
        try:
            # 查询零时表里是否有数据
            sql_1 = '''select 1 
                       from {database}.TMP_FACT_CUSTINCOMECHG_DETAIL T
                       where t.sk_confirmdate between {startdate} and {enddate}
                    '''.format(database=self.database,startdate=self.startdate,enddate=self.enddate)
            print("Exec sql_1:{}".format(sql_1))
            tmpdf = hiveContext.sql(sql_1)
            v_count = tmpdf.count()
            del tmpdf

            if v_count > 0:
                # 拆分关联数据和关联不上的数据
                sql_2 = '''from (
                             select t1.*
                                   ,t2.sk_fundaccount AS sk_fundaccount_tmp
                             from {database}.FACT_CUSTINCOMECHG_DETAIL t1
                             left join {database}.TMP_FACT_CUSTINCOMECHG_DETAIL t2
                               on t1.sk_fundaccount  = t2.sk_fundaccount  
                              and t1.agencyno        = t2.agencyno        
                              and t1.sk_tradeaccount = t2.sk_tradeaccount 
                              and t1.bk_fundcode     = t2.bk_fundcode     
                              and t1.sharetype       = t2.sharetype  
                             ) p
                            insert overwrite table {database}.mid_fact_custincomechg_detail_inc
                             select p.tano
                                   ,p.cserialno
                                   ,p.oricserialno
                                   ,p.customerid
                                   ,p.custtype
                                   ,p.sk_fundaccount
                                   ,p.sk_tradeaccount
                                   ,p.agencyno
                                   ,p.netno
                                   ,p.bk_fundcode
                                   ,p.sharetype
                                   ,p.sk_agency
                                   ,p.regdate
                                   ,p.effective_from
                                   ,p.effective_to
                                   ,p.orinetvalue
                                   ,p.orishares
                                   ,p.oricost
                                   ,p.lastshares
                                   ,p.shrchg
                                   ,p.shares
                                   ,p.sharesofnocost
                                   ,p.cost
                                   ,p.incmofnocost
                                   ,p.income
                                   ,p.costofincome
                                   ,p.totalcost
                                   ,p.totalincmofnocost
                                   ,p.totalincome
                                   ,p.totalcostofincome
                                   ,p.bourseflag
                                   ,p.srcsys
                                   ,p.batchno
                                   ,p.sk_audit
                                   ,p.inserttime
                                   ,p.updatetime
                                   ,p.shrchgofnocost
                                   ,p.bk_tradetype
                                   ,p.ori_bk_tradetype
                              where p.sk_fundaccount_tmp is not null
                                and p.shares > 0
                                and p.effective_to = 99991231  
                            insert overwrite table {database}.fact_not_exists_tmp
                             select p.tano
                                   ,p.cserialno
                                   ,p.oricserialno
                                   ,p.customerid
                                   ,p.custtype
                                   ,p.sk_fundaccount
                                   ,p.sk_tradeaccount
                                   ,p.agencyno
                                   ,p.netno
                                   ,p.bk_fundcode
                                   ,p.sharetype
                                   ,p.sk_agency
                                   ,p.regdate
                                   ,p.effective_from
                                   ,p.effective_to
                                   ,p.orinetvalue
                                   ,p.orishares
                                   ,p.oricost
                                   ,p.lastshares
                                   ,p.shrchg
                                   ,p.shares
                                   ,p.sharesofnocost
                                   ,p.cost
                                   ,p.incmofnocost
                                   ,p.income
                                   ,p.costofincome
                                   ,p.totalcost
                                   ,p.totalincmofnocost
                                   ,p.totalincome
                                   ,p.totalcostofincome
                                   ,p.bourseflag
                                   ,p.srcsys
                                   ,p.batchno
                                   ,p.sk_audit
                                   ,p.inserttime
                                   ,p.updatetime
                                   ,p.shrchgofnocost
                                   ,p.bk_tradetype
                                   ,p.ori_bk_tradetype
                              where p.sk_fundaccount_tmp is null
                                 or p.shares <= 0
                                 or p.effective_to <> 99991231
                '''.format(database=self.database)
                print("Exec sql_2:{}".format(sql_2))
                hiveContext.sql(sql_2)
                # 按才分后的加载起始日和截止日调用处理程序
                self.PRC_SPLIT_DEAL()

                # if v_count != v_deal_count:
                #     raise CustomError('核对记录数不一致')
        except CustomError as e:
            print(e)
            sys.exit(1)
        except Exception:
            exe = traceback.format_exc()
            print(exe)
            sys.exit(1)  

    def PRC_SPLIT_DEAL(self):
        try:
            sql_2 = '''  select t.tano, 
                                t.cserialno, 
                                t.customerid, 
                                t.custtype, 
                                t.sk_fundaccount, 
                                t.sk_tradeaccount, 
                                t.agencyno, 
                                t.netno, 
                                t.bk_fundcode, 
                                t.sharetype, 
                                t.bonustype,
                                t.sk_agency, 
                                t.sk_confirmdate, 
                                t.shares,
                                t.bourseflag, 
                                t.amount, 
                                t.netvalue, 
                                t.incflag, 
                                t.bk_tradetype, 
                                t.incomerule, 
                                t.srcsys, 
                                t.batchno                                           
                            from {database}.tmp_fact_custincomechg_detail t 
                            inner join {dim_product_database}.dim_product t1
                              on  t1.bk_fundcode=t.bk_fundcode
                              and t1.property = '2'
                          where t.tano='{tano}'
                          and t.sk_confirmdate between {startdate} and {enddate}
                    '''.format(database=self.database,dim_product_database=self.dim_product_database, startdate=self.startdate, enddate=self.enddate, tano=self.tano)
            print("Exec sql_2:{}".format(sql_2))
            wf = hiveContext.sql(sql_2).rdd
            wf.persist(StorageLevel.MEMORY_AND_DISK_SER)

            print("Rdd-wf1 transform:filter...")
            #份额变化类型为增加，且为分红交易
            wf1 = wf.filter(funcinc.fil1)
            wf1.persist(StorageLevel.MEMORY_AND_DISK_SER)
            # print wf1.collect()
            print("Rdd-wf2 transform:filter...")
            #份额变化类型为增加，且不为分红交易
            wf2 = wf.filter(funcinc.fil2)
            wf2.persist(StorageLevel.MEMORY_AND_DISK_SER)
            # print wf2.collect()
            print("Rdd-wf3 transform:filter...")
            #份额变化类型为减少
            wf3 = wf.filter(funcinc.fil3)
            wf3.persist(StorageLevel.MEMORY_AND_DISK_SER)

            print("Rdd-wf1 transform:groupByKey...")
            wf1 = wf1.map(
                lambda x: ((x.sk_fundaccount, x.agencyno, x.sk_tradeaccount, x.bk_fundcode, x.sharetype), x)
            ).groupByKey()
            print("Rdd-wf2 transform:groupByKey...")
            wf2 = wf2.map(
                lambda x: ((x.sk_fundaccount, x.agencyno, x.sk_tradeaccount, x.bk_fundcode, x.sharetype), x)
            ).groupByKey()
            print("Rdd-wf3 transform:groupByKey...")
            wf3 = wf3.map(
                lambda x: ((x.sk_fundaccount, x.agencyno, x.sk_tradeaccount, x.bk_fundcode, x.sharetype), x)
            ).groupByKey()

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
                        from {database}.mid_fact_custincomechg_detail_inc
                '''.format(database=self.database)
            print("Exec sql:{}".format(sql_3))
            rdd_fct = hiveContext.sql(sql_3).rdd
            rdd_fct.persist(StorageLevel.MEMORY_AND_DISK_SER)
            # 
            # truncate table
            # sql_truncate = "truncate table {database}.mid_fact_custincomechg_detail_inc".format(database=self.database)
            # print("Exec sql:{}".format(sql_truncate))
            # hiveContext.sql(sql_truncate)
            #
            for i in range(1, 4):
                print('Run.Driver.task.for loop',i,datetime.datetime.today())
                result = []
                if i == 1:
                    if wf1.isEmpty():
                        del wf1
                        gc.collect()
                        print('isEmpty.Run.Driver.task.for loop', i, datetime.datetime.today())
                        continue
                    else:
                        print("Rdd-wf1 action:flatMapValues, function:funcinc.func1...")
                        result = wf1.flatMapValues(funcinc.func1).values()
                        del wf1
                        gc.collect()
                        
                elif i == 2:
                    if wf2.isEmpty():
                        del wf2
                        gc.collect()
                        print('isEmpty.Run.Driver.task.for loop', i, datetime.datetime.today())
                        continue
                    else:
                        print("Rdd-wf2 action:flatMapValues, function:funcinc.func2...")
                        result = wf2.flatMapValues(funcinc.func2).values()
                        del wf2
                        gc.collect()
                        
                elif i == 3:
                    if wf3.isEmpty():
                        del wf3
                        gc.collect()
                        print('isEmpty.Run.Driver.task.for loop', i, datetime.datetime.today())
                        result = rdd_fct
                        print('fact table data persist.')
                    else:
                        print("Rdd-fct transform:groupByKey...")
                        ############################################################
                        df = rdd_fct.map(lambda x: ((x.sk_fundaccount, x.agencyno, x.sk_tradeaccount, x.bk_fundcode, x.sharetype), x)).groupByKey()
                        ##############################################################
                        print("Rdd-df-wf3 transform:Join...")
                        # 临时交易流水表的数据在交易表存在，且需要处理的数据
                        dwf = df.join(wf3)
                        # 交易表找不到tmp临时表数据，数据保留
                        dwf_not_exist_tmp = df.subtract(wf3)
                        # # tmp临时交易表中不在交易表，数据插入
                        # tmp_not_exist_dwf = wf3.subtract(df)
                        del wf3

                        gc.collect()

                        print("Rdd-dwf transform:reduceByKey, function:funcinc.red1...")
                        rdd1 = dwf.reduceByKey(funcinc.red1)
                        del dwf
                        gc.collect()

                        print("Rdd-dwf action:flatMapValues, function:funcinc.func3...")
                        res1 = rdd1.flatMapValues(funcinc.func3).values()
                        del rdd1
                        gc.collect()
                        # 
                        res2 = dwf_not_exist_tmp.flatMapValues(funcinc.func4).values()

                        result = res1.union(res2)
                        del res1,res2
                        gc.collect()
                        #
                        # res3 = tmp_not_exist_dwf.flatMapValues(funcinc.func5).values()
                        # result = result.union(res3)
                        # 
                        # del res3
                        # gc.collect()
                        print('Finish.Run.Driver.task.for loop', i, datetime.datetime.today())
                if result:
                    print("Append data to table {database}.mid_fact_custincomechg_detail_inc".format(database=self.database))
                    result = result.toDF(schema=fct_schema)
                    mid_table='{database}.mid_fact_custincomechg_detail_inc'.format(database=self.database)
                    result.write.format('hive').saveAsTable(mid_table, mode='append')
                    print('loop ',i,',Finish append data.')

            # 把所有处理好的数据从tmp表抽到fact表
            sql_4 = '''INSERT overwrite TABLE {database}.FACT_CUSTINCOMECHG_DETAIL 
                  SELECT  * from {database}.MID_FACT_CUSTINCOMECHG_DETAIL_INC 
                  '''.format(database=self.database)
            print("Exec sql:{}".format(sql_4))
            hiveContext.sql(sql_4)  ##将扣减新增及未扣减数据插入到最终的目标表
            print('program finish...')

        except CustomError as e:
            print(e)
            sys.exit(1)
        except Exception:
            exe = traceback.format_exc()
            print(exe)
            sys.exit(1)  

    def fact_del_update(self):
        try:
        # 把FACT_CUSTINCOMECHG_DETAIL(客户盈亏明细)按给出的起始日期和终止日期把满足条件的数据先插入一张零时表
            sql_1 = '''INSERT overwrite TABLE {database}.TMP_FCT_CUSTINCMCHG_DTL_DEL
                            SELECT A.TANO                                  
                                  ,A.CSERIALNO                             
                                  ,A.ORICSERIALNO                         
                                  ,A.EFFECTIVE_FROM                      
                              FROM {database}.FACT_CUSTINCOMECHG_DETAIL A    
                             WHERE A.EFFECTIVE_FROM >= '{startdate}' AND A.TANO = '{tano}'
                    '''.format(database=self.database, startdate=self.startdate, tano=self.tano)
            print("Exec sql:{}".format(sql_1))
            hiveContext.sql(sql_1)
            # 查询零时表里是否有数据
            sql_2 = "select 1 from {database}.TMP_FCT_CUSTINCMCHG_DTL_DEL limit 1".format(database=self.database)
            print("Exec sql:{}".format(sql_2))
            tmpdf = hiveContext.sql(sql_2)
            V_ROWCNT = tmpdf.count()
            tmpdf.unpersist()
            print("V_ROWCNT="+str(V_ROWCNT))
            if V_ROWCNT > 0:
                # 判断是否需要重载
                # 删除当天加载过的记录
                sql_4 = ''' INSERT OVERWRITE TABLE {database}.FACT_CUSTINCOMECHG_DETAIL
                                 select t.tano,                                
                                       t.cserialno,                              
                                       t.oricserialno,                           
                                       t.customerid,                             
                                       t.custtype,                               
                                       t.sk_fundaccount,                         
                                       t.sk_tradeaccount,                        
                                       t.agencyno,                               
                                       t.netno,                                  
                                       t.bk_fundcode,                            
                                       t.sharetype,                              
                                       t.sk_agency,                              
                                       t.regdate,                                
                                       t.effective_from,                         
                                       t.effective_to,                           
                                       t.orinetvalue,                            
                                       t.orishares,                              
                                       t.oricost,                                
                                       t.lastshares,                             
                                       t.shrchg,                                 
                                       t.shares,                                 
                                       t.sharesofnocost,                         
                                       t.cost,
                                       t.incmofnocost,                           
                                       t.income,                                 
                                       t.costofincome,                           
                                       t.totalcost,                              
                                       t.totalincmofnocost,                      
                                       t.totalincome,                            
                                       t.totalcostofincome,                      
                                       t.bourseflag,                             
                                       t.srcsys,                                 
                                       t.batchno,                                
                                       t.sk_audit,                               
                                       t.inserttime,                             
                                       t.updatetime,                             
                                       t.shrchgofnocost,                         
                                       t.bk_tradetype,                           
                                       t.ori_bk_tradetype 
                                  from {database}.FACT_CUSTINCOMECHG_DETAIL t
                                  left join {database}.TMP_FCT_CUSTINCMCHG_DTL_DEL t1
                                     on t.cserialno = t1.cserialno
                                    and t.oricserialno = t1.oricserialno
                                    and t.tano = t1.tano
                                 where t1.cserialno is null
                                    and t1.oricserialno is null
                                    and t1.tano is null
                        '''.format(database=self.database)
                print("Exec sql:{}".format(sql_4))
                hiveContext.sql(sql_4)
                # 将被ET截断的记录恢复
                sql_5 = '''INSERT OVERWRITE TABLE {database}.FACT_CUSTINCOMECHG_DETAIL
                                 select t.tano,                                
                                       t.cserialno,                              
                                       t.oricserialno,                           
                                       t.customerid,                             
                                       t.custtype,                               
                                       t.sk_fundaccount,                         
                                       t.sk_tradeaccount,                        
                                       t.agencyno,                               
                                       t.netno,                                  
                                       t.bk_fundcode,                            
                                       t.sharetype,                              
                                       t.sk_agency,                              
                                       t.regdate,                                
                                       t.effective_from,                         
                                       case when t1.oricserialno is null 
                                              then t.effective_to
                                            else 
                                              99991231
                                       end as effective_to,                           
                                       t.orinetvalue,                            
                                       t.orishares,                              
                                       t.oricost,                                
                                       t.lastshares,                             
                                       t.shrchg,                                 
                                       t.shares,                                 
                                       t.sharesofnocost,                         
                                       t.cost,
                                       t.incmofnocost,                           
                                       t.income,                                 
                                       t.costofincome,                           
                                       t.totalcost,                              
                                       t.totalincmofnocost,                      
                                       t.totalincome,                            
                                       t.totalcostofincome,                      
                                       t.bourseflag,                             
                                       t.srcsys,                                 
                                       case when t1.oricserialno is null 
                                              then t.batchno
                                            else 
                                              '{batchno}'
                                       end as batchno,                                
                                       t.sk_audit,                               
                                       t.inserttime,                             
                                       case when t1.oricserialno is null 
                                              then t.updatetime
                                            else 
                                              '{updatetime}'
                                       end as updatetime,                             
                                       t.shrchgofnocost,                         
                                       t.bk_tradetype,                           
                                       t.ori_bk_tradetype 
                                 from {database}.FACT_CUSTINCOMECHG_DETAIL t
                                  left join {database}.TMP_FCT_CUSTINCMCHG_DTL_DEL t1
                                     on t.effective_to = t1.effective_from
                                    and t.oricserialno = t1.oricserialno
                                    and t.tano = t1.tano
                         '''.format(database=self.database, batchno=self.batchno,updatetime=datetime.datetime.now())
                print("Exec sql:{}".format(sql_5))
                hiveContext.sql(sql_5)
        except Exception:
            e = traceback.format_exc()
            print(e)
            sys.exit(1)

if __name__ == "__main__":
    print('__main__')
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
    # print 'Dr'
    Dr.fact_del_update()##
    # print 'Dr.fact_del_update'
    d_startdate = str(startdate)
    d_enddate = str(enddate)
    # 转换传入起始日期和截止日
    v_d_startdate = datetime.datetime.strptime(d_startdate, '%Y%m%d')
    v_d_enddate = datetime.datetime.strptime(d_enddate, '%Y%m%d')
    # 初始化本次加载起始日
    v_d_sp_startdate = v_d_startdate

    oneday = datetime.timedelta(days=1)
    day30 = datetime.timedelta(days=30)
    while (v_d_sp_startdate < v_d_enddate + oneday):
        # 计算本次加载起始日
        v_sp_startdate = int(v_d_sp_startdate.strftime('%Y%m%d'))
        # 计算本次加载截止日
        v_d_sp_enddate = v_d_sp_startdate + day30

        # 如果本次加载截止日大于传入截止日，再取传入截止日
        if v_d_sp_enddate > v_d_enddate:
            v_sp_enddate = int(v_d_enddate.strftime('%Y%m%d'))
        else:
            v_sp_enddate = int(v_d_sp_enddate.strftime('%Y%m%d'))

        Dr.startdate=v_sp_startdate
        Dr.enddate=v_sp_enddate
        Dr.task()   
        # 将上次加载截止日+1赋给下次加载起始日
        v_d_sp_startdate = v_d_sp_enddate + oneday