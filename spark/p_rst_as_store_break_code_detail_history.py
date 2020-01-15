# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_rst_as_store_break_code_detail_current
   Description :  断码分析全量表
   Author :       zhl
   date：         2019/09/09
-------------------------------------------------
   Change Activity: liuhaojie 2019/10/15 改变主码关联条件，将dim_stockorg表中的city改为main_size_org_code

-------------------------------------------------
"""
import os

from spark_code.utils.tools import SparkInit

if __name__ == '__main__':
    # app_name
    file_name = os.path.basename(__file__)

    # 初始化spark环境
    lz_spark = SparkInit(file_name)

    sql = """
        select b.sku_sk,
               b.brand_code,
               b.category_code,
               b.big_class,
               b.product_year,
               b.product_quarter,
               b.gender,
               b.product_code,
               b.color_code,
               b.size_code
        from {p_edw_schema}.dim_sku b 
        where '{p_input_date}'>=b.etl_begin and '{p_input_date}'<b.etl_end 
        """
    tmp_sku = lz_spark.return_df(sql).createOrReplaceTempView('tmp_sku')

    sql = """
        select date_string
        from {p_edw_schema}.dim_date b 
        where date_string>=date_sub('{p_input_date}',460) and date_string<'{p_input_date}'            
        """
    tmp_date = lz_spark.return_df(sql).createOrReplaceTempView('tmp_date')

    # 取上7天销量
    sql = """
        select a.sku_sk,
               store_sk,
               b.brand_code,
               b.big_class,
               b.product_year,
               b.product_quarter,
               date_string as day_date,
               sum(case when a.sale_date >= date_sub(date_string, 7) and sale_date<date_string then a.sale_qty else 0 end) as qty
        from tmp_date c
        left join  {p_edw_schema}.fct_sales a on a.sale_date<c.date_string and a.sale_date>= date_sub(date_string, 7)
        inner join tmp_sku b on a.sku_sk=b.sku_sk
        GROUP BY a.sku_sk,store_sk,b.brand_code, b.big_class,
               b.product_year,b.product_quarter,date_string
    """
    tmp_sku_sales = lz_spark.return_df(sql).createOrReplaceTempView('tmp_sku_sales')

    # 取上7天销量sku数量
    sql = """
        select brand_code,
               big_class,
               product_year,
               product_quarter,
               store_sk,
               day_date,
               sum(qty) as day_sales_qty,
               count(1) as sku_count
        from tmp_sku_sales a
        where a.qty>0
        group by a.store_sk,big_class,product_year,product_quarter,a.brand_code,day_date
        """
    tmp_store_sales = lz_spark.return_df(sql).createOrReplaceTempView('tmp_store_sales')

    # 取期初库存
    sql = """
        select a.sku_sk, 
               b.date_string as day_date,
               a.stockorg_sk,
               stock_qty as qty
        from {p_edw_schema}.fct_stock_zipper a
        inner join tmp_date b on a.stock_date_begin<=date_string and a.stock_date_end>date_string
        
        """
    tmp_sku_store_stock = lz_spark.return_df(sql).createOrReplaceTempView('tmp_sku_store_stock')

    # 计算决策日期的所有门店所有skc所有尺码的列表,并且stock_qty>0表示这些skc是具有库存量的
    sql = """
        select a.stockorg_sk as store_sk,
               b.product_code,
               b.color_code,
               b.product_year,
               b.product_quarter,
               b.brand_code,
               b.big_class,
               b.category_code,
               b.gender,
               day_date,
               c.main_size_org_code as main_size_org_code,
               sum(qty) as qty,
               collect_list(b.size_code) as org_size
        from tmp_sku_store_stock a
        inner join tmp_sku b on a.sku_sk=b.sku_sk
        inner join {p_edw_schema}.dim_stockorg c on a.stockorg_sk=c.stockorg_sk 
        where a.qty>0 and c.status='正常' and c.org_flag='1'
            and '{p_input_date}'>=c.etl_begin and '{p_input_date}'<c.etl_end
        group by a.stockorg_sk,b.product_code,b.color_code,b.brand_code,day_date,
        b.category_code,b.gender,c.main_size_org_code,b.product_year,b.product_quarter,
        b.big_class
        """
    tmp_sku_stock = lz_spark.return_df(sql).createOrReplaceTempView('tmp_sku_stock')
    # 关联生成所有门店有货skc齐码中间表
    sql = """
        select a.store_sk,
               a.product_code,
               a.color_code,
               a.brand_code,
               a.big_class,
               product_year,
               product_quarter,
               day_date,
               qty,
               cardinality(array_except(e.size_code, a.org_size))=0 as is_fullsize
        from tmp_sku_stock as a
        inner join {p_edw_schema}.dim_main_size e
        on a.brand_code = e.brand_code
            and a.category_code=e.category_code
            and a.gender=e.gender and a.main_size_org_code = e.main_size_org_code
                """
    mid_full_size = lz_spark.return_df(sql).createOrReplaceTempView('mid_full_size')

    # 计算门店大类的齐码skc数
    sql = """
        select a.store_sk,
               big_class,
               product_year,
               product_quarter,
               a.brand_code,
               day_date,
               count(1) as skc_count,
               sum(case when is_fullsize = true then 1 else 0 end) as skc_fullsize_count
        from mid_full_size a
        where qty>0
        group by a.store_sk,big_class,product_year,product_quarter,a.brand_code,day_date
                    """
    tmp_full_size = lz_spark.return_df(sql).createOrReplaceTempView('tmp_full_size')
    lz_spark.drop_temp_table('mid_full_size')

    # 计算门店大类的有销量且库存为0的sku数
    sql = """
       select a.store_sk,
              big_class,
              product_year,
              product_quarter,
              a.day_date,
              a.brand_code,
              count(1) as sku_emptystock_count
       from tmp_sku_sales a 
       left join tmp_sku_store_stock b on a.store_sk=b.stockorg_sk
              and a.sku_sk=b.sku_sk and a.day_date=b.day_date
       where a.qty>0 and b.qty<=0
       group by a.store_sk,big_class,product_year,product_quarter,a.brand_code,a.day_date
                       """
    tmp_sku_emptystock_count = lz_spark.return_df(sql).createOrReplaceTempView('tmp_sku_emptystock_count')

    # 计算门店大类的有销量且库存为0的sku数
    sql = """
       select a.store_sk,
              a.big_class,
              a.product_year,
              a.product_quarter,
              a.brand_code,
              day_sales_qty,
              sku_count,
              a.day_date,
              skc_count,
              skc_fullsize_count
       from tmp_full_size a 
       left join tmp_store_sales b on a.store_sk=b.store_sk and a.day_date=b.day_date
              and a.big_class=b.big_class and a.product_year=b.product_year
              and a.product_quarter=b.product_quarter and a.brand_code=b.brand_code
       union
       select a.store_sk,
              a.big_class,
              a.product_year,
              a.product_quarter,
              a.brand_code,
              day_sales_qty,
              sku_count,
              a.day_date,
              skc_count,
              skc_fullsize_count
       from tmp_store_sales  a 
       left join tmp_full_size b on b.store_sk=a.store_sk and a.day_date=b.day_date
              and a.big_class=b.big_class and a.product_year=b.product_year
              and a.product_quarter=b.product_quarter and a.brand_code=b.brand_code
                       """
    tmp_result = lz_spark.return_df(sql).createOrReplaceTempView('tmp_result')
    lz_spark.drop_temp_table('tmp_store_sales')
    lz_spark.drop_temp_table('tmp_full_size')
    #
    sql = """
       select distinct brand_code,brand_name
       from {p_edw_schema}.dim_skp d
       where '{p_input_date}'>=d.etl_begin and '{p_input_date}'<d.etl_end               
                           """
    tmp_skc = lz_spark.return_df(sql).createOrReplaceTempView('tmp_skc')

    # 计算门店大类的有销量且库存为0的sku数
    sql = """
        insert overwrite table {p_rst_schema}.rst_as_store_break_code_detail
            partition(day_date)
        select  a.brand_code,
                c.brand_name,
                a.store_sk,
                a.product_year as product_year,
                a.product_quarter as product_quarter,
                a.big_class,
                d.manager_org_code,
                day_sales_qty as last_7days_sales_qty,
                skc_fullsize_count,
                skc_count,
                sku_emptystock_count,
                sku_count,
                current_timestamp as etl_time,
                null as reserved1,
                null as reserved2,
                null as reserved3,
                null as reserved4,
                null as reserved5,
                a.day_date
        from tmp_result a 
        inner join tmp_skc c on a.brand_code=c.brand_code
        inner join {p_edw_schema}.dim_manager_org_store d on a.store_sk=d.store_sk
        left join tmp_sku_emptystock_count b on a.store_sk=b.store_sk
              and a.big_class=b.big_class and a.product_year=b.product_year and a.day_date=b.day_date
              and a.product_quarter=b.product_quarter and a.brand_code=b.brand_code
        where '{p_input_date}'>=d.etl_begin and '{p_input_date}'<d.etl_end    
        distribute by a.day_date           
                           """
    lz_spark.execute_sql(sql)
    lz_spark.drop_temp_table('tmp_skc')
    lz_spark.drop_temp_table('tmp_result')
    lz_spark.drop_temp_table('tmp_sku_emptystock_count')


