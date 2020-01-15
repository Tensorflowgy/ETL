# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     tools
   Description :    通用工具包
   Author :       yangming
   date：          2018/9/3
-------------------------------------------------
   Change Activity:
                   2019/1/8: modify for opff
-------------------------------------------------
"""
import sys
import os
import datetime
import subprocess
import time
from dateutils import relativedelta

from dateutil.parser import parse
from multiprocessing import cpu_count
from concurrent import futures
from os.path import abspath
from pyspark.sql import SparkSession
from pyspark import SparkConf
# from pyspark.sql.functions import udf
# from pyspark.sql.types import StringType
from .config import ETLConfig
from .config import DecisionCycle
from .config import DATE_STR_FORMAT


class TaskProcessPoolExecutors(object):
    """
        使用concurrent.futures包多进程去异步执行任务
    """

    def __init__(self, task_list, handler):

        # 任务列表
        self._task_list = task_list
        # 处理逻辑
        self._handler = handler

        # 如果当前任务数大于cpu核数，则取CPU核数；否则使用当前任务数作为最大并行任务数；
        if len(task_list) > cpu_count():
            max_task_num = cpu_count()
        else:
            max_task_num = len(task_list)

        self._executor = futures.ProcessPoolExecutor(max_task_num)
        self._process()

    def _process(self):
        # 任务结果
        self._result = self._executor.map(self._handler, self._task_list)

    @property
    def result(self):
        # 将结果处理成一个数组返回
        return [elem for elem in self._result]


class TaskThreadPoolExecutors(object):
    """
        使用concurrent.futures包多线程去异步执行任务
    """

    def __init__(self, handler, max_task_num, *task_list):
        # 任务列表，通常来说是参数列表
        self._task_list = task_list
        # 任务的核心处理逻辑
        self._handler = handler
        # 最大并行的任务数量
        self._max_task_num = max_task_num
        self._executor = futures.ThreadPoolExecutor(max_task_num)
        self._process()

    def _process(self):
        # 任务结果
        self._result = self._executor.map(self._handler, *self._task_list)

    @property
    def result(self):
        # 将结果处理成一个数组返回
        return [elem for elem in self._result]


class SparkInit(object):
    def __init__(self, file_name,):
        warehouse_location = abspath('hdfs://hacluster/user/hive/warehouse')
        # app的名称

        app_name = "".join(["PySpark-", file_name])

        # config 配置
        spark_conf = SparkConf()
        spark_conf.set("spark.sql.warehouse.dir", warehouse_location)
        spark_conf.set("hive.exec.dynamic.partition.mode", 'nonstrict')
        # spark_conf.set("spark.driver.maxResultSize","4g")
        # spark_conf.set("spark.executor.memory", '10g') #无效
        # spark_conf.set("spark.driver.memory", '10g') #无效
        # 目的是解决报错：Detected cartesian product for INNER join between logical plans
        spark_conf.set("spark.sql.crossJoin.enabled", 'true')
        spark_conf.set("spark.sql.hive.convertMetastoreParquet", "false")


        self.spark = SparkSession\
            .builder.appName(app_name).\
            config(conf=spark_conf).\
            enableHiveSupport().\
            getOrCreate()

        # TODO udf需要再次调试
        self.register_udf()

        # 获取脚本执行需要的参数
        self.params_dict = self.get_params()

    def get_spark(self):
        return self.spark

    def register_udf(self):
        """
        注册udf使用
        :return:
        """
        # # 注册udf
        self.spark.udf.register('udf_date_trunc', date_trunc)

    @staticmethod
    def get_params():
        """
        获取参数, 返回python脚本的参数字典,
        :return: params_dict 默认返回输入日期和输入日期截断到周一的日期, 正价率的字段，畅销款比率
        """
        param_list = sys.argv
        param_len = len(param_list)
        if param_len <= 1:
            raise Exception('参数错误')
        else:
            p_input_date = parse(param_list[-1])
            p_input_date_str = p_input_date.strftime(DATE_STR_FORMAT)
            p_input_date_day_before_str = (p_input_date - relativedelta(days=1)).strftime(DATE_STR_FORMAT)
            p_input_date_day_sixty_str = (p_input_date - relativedelta(days=60)).strftime(DATE_STR_FORMAT)
            p_input_date_day_sixty_month_str = datetime.datetime(parse(p_input_date_day_sixty_str).year, parse(p_input_date_day_sixty_str).month, day=1).strftime(DATE_STR_FORMAT)

            p_lst2_year = (p_input_date - relativedelta(months=25)).strftime('%Y')
            p_lst2_year_month = (p_input_date - relativedelta(months=25)).strftime('%m')

            # 日期所在周的周一对应的日期
            p_input_date_monday_str = date_trunc(DecisionCycle.week, p_input_date_str)

            # 日期前一周的周一对应的日期
            p_input_date_week_before_monday_str = date_trunc(DecisionCycle.week, p_input_date_str, -6)

            decision_cycle = ETLConfig.decision_cycle
            if decision_cycle == DecisionCycle.week:
                # 决策日期
                p_decision_date_str = date_trunc(DecisionCycle.week, p_input_date_str,
                                                 ETLConfig.decision_start_day_num)
                p_decision_date = parse(p_decision_date_str)
                p_pre_decision_date = p_decision_date - relativedelta(days=7)
                p_pre_decision_date_str = p_pre_decision_date.strftime(DATE_STR_FORMAT)
                p_decision_cycle_days = 7
            elif decision_cycle == DecisionCycle.month:
                p_decision_date_str = date_trunc(DecisionCycle.month, p_input_date_str,
                                                 ETLConfig.decision_start_day_num)
                p_decision_date = parse(p_decision_date_str)
                p_pre_decision_date = p_decision_date - relativedelta(months=1)
                p_pre_decision_date_str = p_pre_decision_date.strftime(DATE_STR_FORMAT)
                p_decision_cycle_days = p_decision_date - p_pre_decision_date
            elif decision_cycle == DecisionCycle.year:
                p_decision_date_str = date_trunc(DecisionCycle.year, p_input_date_str,
                                                 ETLConfig.decision_start_day_num,
                                                 ETLConfig.decision_start_month_num)
                p_decision_date = parse(p_decision_date_str)
                p_pre_decision_date = p_decision_date - relativedelta(months=12)
                p_pre_decision_date_str = p_pre_decision_date.strftime(DATE_STR_FORMAT)
                p_decision_cycle_days = p_decision_date - p_pre_decision_date
            else:
                p_decision_date = p_input_date
                p_pre_decision_date = p_decision_date - relativedelta(days=1)
                p_pre_decision_date_str = p_pre_decision_date.strftime(DATE_STR_FORMAT)
                p_decision_cycle_days = 1


            p_ods_schema = ETLConfig.ODS_SCHEMA
            p_ods_standard_schema = ETLConfig.ODS_STANDARD_SCHEMA
            p_edw_schema = ETLConfig.EDW_SCHEMA
            p_edw_ai_schema = ETLConfig.EDW_AI_SCHEMA
            p_dm_schema = ETLConfig.DM_SCHEMA
            p_rst_schema = ETLConfig.RST_SCHEMA
            p_pg_rst_schema = ETLConfig.PG_RST_SCHEMA

            params_dict = {'p_input_date': p_input_date_str,  # 本次决策日期 即调度日期
                           'p_input_date_day_before': p_input_date_day_before_str,  # 本次决策日期前一天
                           'p_input_date_monday': p_input_date_monday_str,  # 本周一
                           'p_input_date_week_before_monday': p_input_date_week_before_monday_str,  # 上周一
                           'p_input_date_day_sixty': p_input_date_day_sixty_str,         #当前日期前60天
                           'p_input_date_day_sixty_month': p_input_date_day_sixty_month_str, #当前日期减60天的月份的第一天
                           'p_input_lst2_year': p_lst2_year,
                           'p_input_lst2_year_month': p_lst2_year_month,
                           'p_pre_decision_date': p_pre_decision_date_str,  # 上次决策日期
                           'p_decision_cycle_days': p_decision_cycle_days,  # 决策周期
                           'p_ods_schema': p_ods_schema,
                           'p_ods_standard_schema': p_ods_standard_schema,
                           'p_edw_schema': p_edw_schema,
                           'p_edw_ai_schema': p_edw_ai_schema,
                           'p_dm_schema': p_dm_schema,
                           'p_rst_schema': p_rst_schema,
                           'p_pg_rst_schema': p_pg_rst_schema}
            return params_dict

    def create_temp_table(self, sql, table_name):
        """
        创建临时表
        :param sql: sql语句
        :param table_name: 临时表表名
        :return:
        """
        sql_temp = sql.format(**self.params_dict)
        temp_table = self.spark.sql(sql_temp).createOrReplaceTempView(table_name)
        return temp_table

    def drop_temp_table(self, table_name):
        """
        drop临时表
        :param table_name:
        :return:
        """
        self.spark.catalog.dropTempView(table_name)

    def execute_sql(self, sql, other_params=None):
        """
        spark引擎执行sql语句
        :param sql:
        :param other_params: 当有其他参数时传入
        :return:
        """
        self.params_dict.update(other_params or {})
        sql_to_execute = sql.format(**self.params_dict)
        print(sql_to_execute)
        self.spark.sql(sql_to_execute)

    def return_df(self, sql, other_params=None):
        """
        spark引擎执行sql语句并返回dataframe
        :param
        sql:
        :return:
        df
        """
        self.params_dict.update(other_params or {})
        sql_to_execute = sql.format(**self.params_dict)
        print(sql_to_execute)
        df = self.spark.sql(sql_to_execute)
        return df


def date_trunc(interval, date_str, start_day=1, start_month=1):
    """
    截断到指定精度，返回相应的日期字符串
    :param interval: DecisionCycle
    :param date_str:
    :param start_day:
    :param start_month:
    :param return_str:
    :return: after_trunc_date_str
    """
    date_obj = parse(date_str)
    # date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d')
    if interval == DecisionCycle.week:
        # 日期所在周的周一对应的日期
        res = date_obj - relativedelta(days=(date_obj.isocalendar()[2] - start_day))
    elif interval == DecisionCycle.month:
        res = datetime.date(date_obj.year, date_obj.month, start_day)
    elif interval == DecisionCycle.year:
        res = datetime.date(date_obj.year, start_month, start_day)
    else:
        raise Exception("interval must be DecisionCycle")
    return res.strftime(DATE_STR_FORMAT)


def extract(interval, date_str):
    """
    返回所取日期的子域
    :param interval: ['day', 'weekday', 'week', 'month', 'year']
    :param date_str:
    :return: int
    """
    date_obj = parse(date_str)
    if interval == 'day':
        res = date_obj.day
    elif interval == 'weekday':
        # 周的情况返回周几的数字，
        res = date_obj.isocalendar()[2]
    elif interval == 'week':
        # 返回日期的自然周数
        res = date_obj.isocalendar()[1]
    elif interval == 'month':
        res = date_obj.month
    elif interval == 'year':
        res = date_obj.year
    else:
        raise Exception("interval must be ['day', 'weekday', 'week', 'month', 'year']")
    return res


def try_pipe_cmd(cmd, try_num=1):
    while True:
        count = 1
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE,shell=True)
        output, err = p.communicate()
        p_status = p.wait()

        if p_status != 0:
            if count > try_num:
                return output, p_status
            else:
                count += 1
                time.sleep(2)
                continue
        else:
            return output, p_status


def try_system_cmd(cmd, try_num=1):
    while True:
        count = 1
        p_status = os.system(cmd)
        output = "os.system"
        if p_status != 0:
            if count > try_num:
                return output, p_status
            else:
                count += 1
                time.sleep(2)
                continue
        else:
            return output, p_status

# LZSpark = SparkInit("lz")
# spark = LZSpark.get_spark()


if __name__ == '__main__':
    print(extract('week', '2018-09-09'))
    print(date_trunc('week', '2017-01-01'))
    print('p_input_date_day_sixty_str','2019-11-12')
    print('p_input_date_day_sixty_month_str','2019-11-12')
    foo = SparkInit('ym_test')
    foo.spark.sql("select udf_date_trunc('week', '2018-09-12')").show()
