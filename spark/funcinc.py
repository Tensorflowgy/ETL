# -*- coding: utf-8 -*-
import datetime
from decimal import Decimal


def nvl(x, y):
    if type(x) == str:
        if x.strip() == '':
            return y
        else:
            return x
    elif x is None:
        return y
    else:
        return x


def red1(x, y):
    return (set([x[0], y[0]]), x[1].union(y[1]))

def fil1(x):
    if x.incflag == 1 and x.bk_tradetype != '143':
        return x


def fil2(x):
    if x.incflag == 1 and x.bk_tradetype == '143':
        return x


def fil3(x):
    if x.incflag == -1:
        return x


def func1(v):
    list1 = list(v)
    list2 = []
    for j in range(len(list1)):
        dclItem = list1[j]
        typeRow = type(list1[0])('tano', 'cserialno', 'oricserialno', 'customerid', 'custtype', 'sk_fundaccount',
                                 'sk_tradeaccount', 'agencyno', 'netno', 'bk_fundcode', 'sharetype', 'sk_agency',
                                 'regdate', 'effective_from', 'effective_to', 'orinetvalue', 'orishares',
                                 'oricost', 'lastshares', 'shrchg', 'shares', 'sharesofnocost', 'cost', 'incmofnocost',
                                 'income', 'costofincome',
                                 'totalcost', 'totalincmofnocost', 'totalincome', 'totalcostofincome', 'bourseflag',
                                 'srcsys', 'batchno', 'sk_audit',
                                 'inserttime', 'updatetime', 'shrchgofnocost', 'bk_tradetype', 'ori_bk_tradetype')
        if dclItem.incomerule == -1:
            c_oricost = dclItem.amount
            c_cost = dclItem.amount
            c_totalcost = dclItem.amount
            c_costofincome = Decimal(0)
            c_income = Decimal(0)
        elif dclItem.incomerule == -2:
            c_oricost = dclItem.shares * nvl(dclItem.netvalue, Decimal(1))
            c_cost = dclItem.shares * nvl(dclItem.netvalue, Decimal(1))
            c_totalcost = dclItem.shares * nvl(dclItem.netvalue, Decimal(1))
            c_costofincome = Decimal(0)
            c_income = Decimal(0)
        else:
            c_oricost = Decimal(0)
            c_cost = Decimal(0)
            c_totalcost = Decimal(0)
            c_costofincome = Decimal(0)
            c_income = Decimal(0)
        list2.append(typeRow(dclItem.tano, dclItem.cserialno, dclItem.cserialno, dclItem.customerid, dclItem.custtype,
                             dclItem.sk_fundaccount, dclItem.sk_tradeaccount, dclItem.agencyno, dclItem.netno,
                             dclItem.bk_fundcode, dclItem.sharetype, dclItem.sk_agency, dclItem.sk_confirmdate,
                             dclItem.sk_confirmdate, Decimal(99991231), dclItem.netvalue, dclItem.shares,
                             c_oricost, Decimal(0), dclItem.shares, dclItem.shares, Decimal(0), c_cost, Decimal(0),
                             c_income, c_costofincome,
                             c_totalcost, Decimal(0), Decimal(0), Decimal(0), dclItem.bourseflag, dclItem.srcsys,
                             dclItem.batchno, Decimal(0),
                             datetime.datetime.now(), datetime.datetime.now(), Decimal(0), dclItem.bk_tradetype,
                             dclItem.bk_tradetype))
    return list2


def func2(v):
    list1 = list(v)
    list2 = []
    for j in range(len(list1)):
        dclItem = list1[j]
        typeRow = type(list1[0])('tano', 'cserialno', 'oricserialno', 'customerid', 'custtype', 'sk_fundaccount',
                                 'sk_tradeaccount', 'agencyno', 'netno', 'bk_fundcode', 'sharetype', 'sk_agency',
                                 'regdate', 'effective_from', 'effective_to', 'orinetvalue', 'orishares',
                                 'oricost', 'lastshares', 'shrchg', 'shares', 'sharesofnocost', 'cost',
                                 'incmofnocost', 'income', 'costofincome',
                                 'totalcost', 'totalincmofnocost', 'totalincome', 'totalcostofincome', 'bourseflag',
                                 'srcsys', 'batchno', 'sk_audit',
                                 'inserttime', 'updatetime', 'shrchgofnocost', 'bk_tradetype', 'ori_bk_tradetype')
        if dclItem.bonustype == '1':
            c_income = dclItem.amount
            c_totalincome = dclItem.amount
            c_incmofnocost = dclItem.amount
            c_totalincmofnocost = dclItem.amount
            c_shares = Decimal(0)
            c_shrchg = Decimal(0)
            c_shrchgofnocost = Decimal(0)
            c_sharesofnocost = Decimal(0)
        else:
        # elif dclItem.bonustype == '0':
            c_income = Decimal(0)
            c_totalincome = Decimal(0)
            c_incmofnocost = Decimal(0)
            c_totalincmofnocost = Decimal(0)
            c_shares = dclItem.shares
            c_shrchg = dclItem.shares
            c_shrchgofnocost = dclItem.shares
            c_sharesofnocost = dclItem.shares

        list2.append(
            typeRow(dclItem.tano, dclItem.cserialno, dclItem.cserialno, dclItem.customerid, dclItem.custtype,
                    dclItem.sk_fundaccount, dclItem.sk_tradeaccount, dclItem.agencyno, dclItem.netno,
                    dclItem.bk_fundcode, dclItem.sharetype, dclItem.sk_agency, dclItem.sk_confirmdate,
                    dclItem.sk_confirmdate, Decimal(99991231), dclItem.netvalue, dclItem.shares,
                    Decimal(0), Decimal(0), c_shrchg, c_shares, c_sharesofnocost, Decimal(0), c_incmofnocost,
                    c_income, Decimal(0),
                    Decimal(0), c_totalincmofnocost, c_totalincome, Decimal(0), dclItem.bourseflag, dclItem.srcsys,
                    dclItem.batchno, Decimal(0),
                    datetime.datetime.now(), datetime.datetime.now(), Decimal(0), dclItem.bk_tradetype,
                    dclItem.bk_tradetype))
    return list2


def func3(v):
    list2 = sorted(list(v[1]), key=lambda x: x.cserialno)
    #try:
    #    list2 = sorted(list(v[1]), key=lambda x: x.cserialno)
    #except EnvironmentError as e:
    #    list2 = sorted(list(v[0]), key=lambda x: x.cserialno)
    list1 = list(v[0])
    list1 = sorted(list1, key=lambda x: x.regdate)
    list1 = sorted(list1, key=lambda x: x.oricserialno)
    typeRow = type(list1[0])('tano', 'cserialno', 'oricserialno', 'customerid', 'custtype', 'sk_fundaccount',
                             'sk_tradeaccount', 'agencyno', 'netno', 'bk_fundcode', 'sharetype', 'sk_agency', 'regdate',
                             'effective_from', 'effective_to', 'orinetvalue', 'orishares',
                             'oricost', 'lastshares', 'shrchg', 'shares', 'sharesofnocost', 'cost', 'incmofnocost',
                             'income', 'costofincome',
                             'totalcost', 'totalincmofnocost', 'totalincome', 'totalcostofincome', 'bourseflag',
                             'srcsys', 'batchno', 'sk_audit',
                             'inserttime', 'updatetime', 'shrchgofnocost', 'bk_tradetype', 'ori_bk_tradetype')
    for dclItem in list2:
        dclshares = nvl(dclItem.shares,0)
        c_tano = nvl(dclItem.tano,0)
        c_cserialno = nvl(dclItem.cserialno,0)
        c_effective_from = nvl(dclItem.sk_confirmdate,0)
        c_srcsys = nvl(dclItem.srcsys,0)
        c_batchno = nvl(dclItem.batchno,0)
        c_bk_tradetype = nvl(dclItem.bk_tradetype,0)
        b_shares = nvl(dclItem.shares,0)
        b_incomerule = nvl(dclItem.incomerule,0)
        b_amount = nvl(dclItem.amount,0)
        v_tmptotalincome = Decimal(0)
        b_netvalue = nvl(dclItem.netvalue,0)
        upLimit = len(list1)
        for i in range(upLimit):
            if list1[i].effective_to == 99991231 and list1[i].shares > 0 and dclshares > 0:
                # tmp=dclshares
                a_shares = nvl(list1[i].shares,0)
                if (dclshares - a_shares > 0):
                    c_costofincome = Decimal(0)
                    c_income = Decimal(0)
                    c_shrchg = - a_shares
                    c_shares = Decimal(0)
                    c_cost = Decimal(0)
                    c_effective_to = Decimal(99991231)
                    v_shrchg = - c_shrchg
                    v_rmshares_r = b_shares - a_shares
                    a_sharesofnocost = nvl(list1[i].sharesofnocost,0)
                    c_incmofnocost = Decimal(0)
                    a_oricost = nvl(list1[i].oricost,0)
                    a_totalcostofincome = nvl(list1[i].totalcostofincome,0)
                    a_orishares = nvl(list1[i].orishares,0)
                    a_orinetvalue = nvl(list1[i].orinetvalue,0)
                    if c_shares - a_sharesofnocost <= 0:
                        v_shrchgofnocost = (a_sharesofnocost - c_shares)
                        c_shrchgofnocost = Decimal(-1) * v_shrchgofnocost
                        c_sharesofnocost = c_shares
                    # elif c_shares - a_sharesofnocost > 0:
                    else:
                        v_shrchgofnocost = Decimal(0)
                        c_shrchgofnocost = v_shrchgofnocost
                        c_sharesofnocost = a_sharesofnocost
                    if b_incomerule == 1:
                        c_cost = Decimal(0)
                        if c_shares - a_sharesofnocost <= 0:
                            c_costofincome = a_oricost - a_totalcostofincome
                        elif a_oricost - a_totalcostofincome > 0:
                            c_costofincome = Decimal(v_shrchg / a_orishares * a_oricost, 2)
                        if v_rmshares_r > 0:
                            c_income = v_shrchg / b_shares * b_amount
                            v_tmptotalincome = v_tmptotalincome + c_income
                        elif v_rmshares_r == 0:
                            c_income = b_amount - v_tmptotalincome
                        # end if;
                        c_incmofnocost = v_shrchgofnocost / b_shares * b_amount

                    elif b_incomerule == 2:
                        c_cost = Decimal(0)
                        if c_shares - a_sharesofnocost <= 0:
                            c_costofincome = (v_shrchg - (a_sharesofnocost - c_shares)) * a_orinetvalue
                        else:
                            c_costofincome = v_shrchg * a_orinetvalue

                        c_income = v_shrchg * b_netvalue
                        c_incmofnocost = v_shrchgofnocost * b_netvalue

                    elif b_incomerule == 0:
                        c_cost = Decimal(0)
                        c_costofincome = Decimal(0)
                        c_income = Decimal(0)
                        c_incmofnocost = Decimal(0)
                    c_oricserialno = list1[i].oricserialno
                    c_customerid = list1[i].customerid
                    c_custtype = list1[i].custtype
                    c_sk_fundaccount = list1[i].sk_fundaccount
                    c_sk_tradeaccount = list1[i].sk_tradeaccount
                    c_agencyno = list1[i].agencyno
                    c_netno = list1[i].netno
                    c_bk_fundcode = list1[i].bk_fundcode
                    c_sharetype = list1[i].sharetype
                    c_sk_agency = list1[i].sk_agency
                    c_regdate = list1[i].regdate
                    c_orinetvalue = list1[i].orinetvalue
                    c_orishares = list1[i].orishares
                    c_oricost = list1[i].oricost
                    c_lastshares = list1[i].shares
                    c_totalcost = list1[i].totalcost
                    #                            c_costofincome = Decimal(c_costofincome)
                    #                            c_income = Decimal(c_income)
                    c_totalincmofnocost = list1[i].totalincmofnocost + Decimal(c_incmofnocost)
                    c_ori_bk_tradetype = list1[i].ori_bk_tradetype
                    c_bourseflag = list1[i].bourseflag
                    c_totalincome = list1[i].totalincome + Decimal(c_income)
                    c_totalcostofincome = list1[i].totalcostofincome + Decimal(c_costofincome)
                    c_sk_audit = Decimal(0)

                    dclshares = dclshares - list1[i].shares
                    list1[i] = typeRow(list1[i].tano, list1[i].cserialno, list1[i].oricserialno,
                                       list1[i].customerid, list1[i].custtype, list1[i].sk_fundaccount,
                                       list1[i].sk_tradeaccount, list1[i].agencyno, list1[i].netno,
                                       list1[i].bk_fundcode, list1[i].sharetype, list1[i].sk_agency,
                                       list1[i].regdate, list1[i].effective_from, Decimal(dclItem.sk_confirmdate),
                                       list1[i].orinetvalue, list1[i].orishares,
                                       list1[i].oricost, list1[i].lastshares, list1[i].shrchg, list1[i].shares,
                                       list1[i].sharesofnocost, list1[i].cost, list1[i].incmofnocost,
                                       list1[i].income, list1[i].costofincome,
                                       list1[i].totalcost, list1[i].totalincmofnocost, list1[i].totalincome,
                                       list1[i].totalcostofincome, list1[i].bourseflag, list1[i].srcsys,
                                       list1[i].batchno, list1[i].sk_audit,
                                       list1[i].inserttime, list1[i].updatetime, list1[i].shrchgofnocost,
                                       list1[i].bk_tradetype, list1[i].ori_bk_tradetype)
                    c_updatetime = datetime.datetime.now()
                    c_inserttime = datetime.datetime.now()
                    list1.append(
                        typeRow(c_tano, c_cserialno, c_oricserialno, c_customerid, c_custtype, c_sk_fundaccount,
                                c_sk_tradeaccount, c_agencyno, c_netno, c_bk_fundcode, c_sharetype, c_sk_agency,
                                c_regdate, c_effective_from, c_effective_to, c_orinetvalue, c_orishares,
                                c_oricost, c_lastshares, c_shrchg, c_shares, c_sharesofnocost, c_cost,
                                c_incmofnocost, c_income, c_costofincome,
                                c_totalcost, c_totalincmofnocost, c_totalincome, c_totalcostofincome, c_bourseflag,
                                c_srcsys, c_batchno, c_sk_audit,
                                c_inserttime, c_updatetime, c_shrchgofnocost, c_bk_tradetype, c_ori_bk_tradetype))
                else:
                    a_shares = nvl(list1[i].shares,0)
                    c_shrchg = - nvl( dclshares,0)
                    c_shares = nvl(a_shares,0) - nvl(dclshares,0)
                    c_effective_to = Decimal(99991231)
                    v_shrchg = - nvl(c_shrchg,0)
                    v_rmshares_r = Decimal(0)
                    c_costofincome = Decimal(0)
                    c_income = Decimal(0)
                    c_cost = Decimal(0)
                    c_incmofnocost = Decimal(0)
                    a_sharesofnocost = nvl(list1[i].sharesofnocost,0)
                    a_oricost = nvl(list1[i].oricost,0)
                    a_totalcostofincome = nvl(list1[i].totalcostofincome,0)
                    a_orishares = nvl(list1[i].orishares,0)
                    a_orinetvalue = nvl(list1[i].orinetvalue,0)
                    if c_shares - a_sharesofnocost <= 0:
                        v_shrchgofnocost = (a_sharesofnocost - c_shares)
                        c_shrchgofnocost = Decimal(-1) * v_shrchgofnocost
                        c_sharesofnocost = c_shares
                    # elif c_shares - a_sharesofnocost > 0:
                    else:
                        v_shrchgofnocost = Decimal(0)
                        c_shrchgofnocost = v_shrchgofnocost
                        c_sharesofnocost = a_sharesofnocost
                    if b_incomerule == 1:
                        c_cost = Decimal(0)
                        if c_shares - a_sharesofnocost <= 0:
                            c_costofincome = a_oricost - a_totalcostofincome
                        elif a_oricost - a_totalcostofincome > 0:
                            c_costofincome = Decimal(v_shrchg / a_orishares * a_oricost, 2)
                        if v_rmshares_r > 0:
                            c_income = v_shrchg / b_shares * b_amount
                            v_tmptotalincome = v_tmptotalincome + c_income
                        elif v_rmshares_r == 0:
                            c_income = b_amount - v_tmptotalincome
                        # end if;
                        c_incmofnocost = v_shrchgofnocost / b_shares * b_amount

                    elif b_incomerule == 2:
                        c_cost = Decimal(0)
                        if c_shares - a_sharesofnocost <= 0:
                            c_costofincome = (v_shrchg - (a_sharesofnocost - c_shares)) * a_orinetvalue
                        else:
                            c_costofincome = v_shrchg * a_orinetvalue

                        c_income = v_shrchg * b_netvalue
                        c_incmofnocost = v_shrchgofnocost * b_netvalue

                    elif b_incomerule == 0:
                        c_cost = Decimal(0)
                        c_costofincome = Decimal(0)
                        c_income = Decimal(0)
                        c_incmofnocost = Decimal(0)
                    c_oricserialno = list1[i].oricserialno
                    c_customerid = list1[i].customerid
                    c_custtype = list1[i].custtype
                    c_sk_fundaccount = list1[i].sk_fundaccount
                    c_sk_tradeaccount = list1[i].sk_tradeaccount
                    c_agencyno = list1[i].agencyno
                    c_netno = list1[i].netno
                    c_bk_fundcode = list1[i].bk_fundcode
                    c_sharetype = list1[i].sharetype
                    c_sk_agency = list1[i].sk_agency
                    c_regdate = list1[i].regdate
                    c_orinetvalue = list1[i].orinetvalue
                    c_orishares = list1[i].orishares
                    c_oricost = list1[i].oricost
                    c_lastshares = list1[i].shares
                    c_totalcost = list1[i].totalcost
                    #                            c_costofincome = Decimal(c_costofincome)
                    #                            c_income = Decimal(c_income)
                    c_totalincmofnocost = list1[i].totalincmofnocost + Decimal(c_incmofnocost)
                    c_ori_bk_tradetype = list1[i].ori_bk_tradetype
                    c_bourseflag = list1[i].bourseflag
                    c_totalincome = list1[i].totalincome + Decimal(c_income)
                    c_totalcostofincome = list1[i].totalcostofincome + Decimal(c_costofincome)
                    c_sk_audit = Decimal(0)
                    dclshares = Decimal(0)
                    list1[i] = typeRow(list1[i].tano, list1[i].cserialno, list1[i].oricserialno,
                                       list1[i].customerid, list1[i].custtype, list1[i].sk_fundaccount,
                                       list1[i].sk_tradeaccount, list1[i].agencyno, list1[i].netno,
                                       list1[i].bk_fundcode, list1[i].sharetype, list1[i].sk_agency,
                                       list1[i].regdate, list1[i].effective_from, Decimal(dclItem.sk_confirmdate),
                                       list1[i].orinetvalue, list1[i].orishares,
                                       list1[i].oricost, list1[i].lastshares, list1[i].shrchg, list1[i].shares,
                                       list1[i].sharesofnocost, list1[i].cost, list1[i].incmofnocost,
                                       list1[i].income, list1[i].costofincome,
                                       list1[i].totalcost, list1[i].totalincmofnocost, list1[i].totalincome,
                                       list1[i].totalcostofincome, list1[i].bourseflag, list1[i].srcsys,
                                       list1[i].batchno, list1[i].sk_audit,
                                       list1[i].inserttime, list1[i].updatetime, list1[i].shrchgofnocost,
                                       list1[i].bk_tradetype, list1[i].ori_bk_tradetype)
                    c_updatetime = datetime.datetime.now()
                    c_inserttime = datetime.datetime.now()
                    list1.append(
                        typeRow(c_tano, c_cserialno, c_oricserialno, c_customerid, c_custtype, c_sk_fundaccount,
                                c_sk_tradeaccount, c_agencyno, c_netno, c_bk_fundcode, c_sharetype, c_sk_agency,
                                c_regdate, c_effective_from, c_effective_to, c_orinetvalue, c_orishares,
                                c_oricost, c_lastshares, c_shrchg, c_shares, c_sharesofnocost, c_cost,
                                c_incmofnocost, c_income, c_costofincome,
                                c_totalcost, c_totalincmofnocost, c_totalincome, c_totalcostofincome, c_bourseflag,
                                c_srcsys, c_batchno, c_sk_audit,
                                c_inserttime, c_updatetime, c_shrchgofnocost, c_bk_tradetype, c_ori_bk_tradetype))
    return list1

# fact 保留的数据
def func4(v):
    list1 = list(v)
    list2 = []
    for j in range(len(list1)):
        dclItem = list1[j]
        typeRow = type(list1[0])('tano', 'cserialno', 'oricserialno', 'customerid', 'custtype', 'sk_fundaccount',
                                 'sk_tradeaccount', 'agencyno', 'netno', 'bk_fundcode', 'sharetype', 'sk_agency',
                                 'regdate', 'effective_from', 'effective_to', 'orinetvalue', 'orishares',
                                 'oricost', 'lastshares', 'shrchg', 'shares', 'sharesofnocost', 'cost',
                                 'incmofnocost', 'income', 'costofincome',
                                 'totalcost', 'totalincmofnocost', 'totalincome', 'totalcostofincome', 'bourseflag',
                                 'srcsys', 'batchno', 'sk_audit',
                                 'inserttime', 'updatetime', 'shrchgofnocost', 'bk_tradetype', 'ori_bk_tradetype')

        list2.append(
            typeRow(dclItem.tano,                     #TA代码
                    dclItem.cserialno,                #交易确认流水号\;对应每笔交易确认的确认流水号
                    dclItem.cserialno,                #原始确认流水号\;对应交易确认份额来源交易对应的确认流水号
                    dclItem.customerid,               #客户号
                    dclItem.custtype,                 #客户类型
                    dclItem.sk_fundaccount,           #基金账户代理键1
                    dclItem.sk_tradeaccount,          #交易账户代理键3
                    dclItem.agencyno,                 #销售机构代码2
                    dclItem.netno,                    #管理网点号
                    dclItem.bk_fundcode,              #基金代码4
                    dclItem.sharetype,                #份额类别5
                    dclItem.sk_agency,                #销售机构代理键\;目前到顶层
                    dclItem.sk_confirmdate,           #份额注册日期
                    dclItem.sk_confirmdate,           #有效起始日
                    Decimal(99991231),                #有效截止日(不含)
                    dclItem.netvalue,                 #买入时点单位净值
                    dclItem.shares,                   #原始基金份额\;不可加
                    Decimal(0),                       #购入时点投入成本\;不可加
                    Decimal(0),                       #上次份额\;不可加
                    dclItem.shares,                   #发生份额\;可加
                    dclItem.shares,                   #基金份额\;不可加
                    dclItem.shares,                   #未付成本产生的累计基金份额\;不可加
                    Decimal(0),                       #成本\;可加
                    Decimal(0),                       #未付成本产生的已实现收益\;可加
                    Decimal(0),                       #已实现收益\;可加
                    Decimal(0),                       #已实现收益对应成本\;可加
                    Decimal(0),                       #累计投资成本\;不可加
                    Decimal(0),                       #累计因未付成本产生的已实现收益\;不可加
                    Decimal(0),                       #累计已实现收益\;不可加
                    Decimal(0),                       #累计已实现收益对应成本\;不可加
                    dclItem.bourseflag,               #交易场所标志
                    dclItem.srcsys,                   #源系统
                    dclItem.batchno,                  #批次号
                    Decimal(0),                       #审计代理键；预留字段，目前为空
                    datetime.datetime.now(),          #插入时间戳
                    datetime.datetime.now(),          #更新时间戳
                    Decimal(0),                       #未付成本产生的发生份额\;可加
                    dclItem.bk_tradetype,             #交易类型
                    dclItem.bk_tradetype))            #原始交易类型
    return list2
    

# # tmp表关联不上fact表的数据
# def func5(v):
#     list1 = list(v)
#     list1 = sorted(list1, key=lambda x: x.tano)# TA代码
#     list1 = sorted(list1, key=lambda x: x.sk_fundaccount)# 基金账户代理键
#     list1 = sorted(list1, key=lambda x: x.sk_tradeaccount)# 交易账户代理键
#     list1 = sorted(list1, key=lambda x: x.bk_fundcode)# 基金代码
#     list1 = sorted(list1, key=lambda x: x.sharetype)# 份额类别
#     list1 = sorted(list1, key=lambda x: x.agencyno)# 销售机构代码
#     list1 = sorted(list1, key=lambda x: x.bk_tradetype) # 基金销售交易类型代码
#     list1 = sorted(list1, key=lambda x: x.sk_confirmdate)# 确认日期

#     typeRow = type(list1[0])('tano', 'cserialno', 'oricserialno', 'customerid', 'custtype', 'sk_fundaccount',
#                                  'sk_tradeaccount', 'agencyno', 'netno', 'bk_fundcode', 'sharetype', 'sk_agency',
#                                  'regdate', 'effective_from', 'effective_to', 'orinetvalue', 'orishares',
#                                  'oricost', 'lastshares', 'shrchg', 'shares', 'sharesofnocost', 'cost',
#                                  'incmofnocost', 'income', 'costofincome',
#                                  'totalcost', 'totalincmofnocost', 'totalincome', 'totalcostofincome', 'bourseflag',
#                                  'srcsys', 'batchno', 'sk_audit',
#                                  'inserttime', 'updatetime', 'shrchgofnocost', 'bk_tradetype', 'ori_bk_tradetype')

#     last_dclItem = list[0] #保存当前行的数据，如果下一条数据相同则参与计算

#     list2 = []
#     for i in range(1,len(list1)):
#         dclItem = list1[i]

#         if  dclItem.tano            == last_dclItem.tano            and
#             dclItem.sk_fundaccount  == last_dclItem.sk_fundaccount  and
#             dclItem.sk_tradeaccount == last_dclItem.sk_tradeaccount and
#             dclItem.bk_fundcode     == last_dclItem.bk_fundcode     and
#             dclItem.sharetype       == last_dclItem.sharetype       and
#             dclItem.agencyno        == last_dclItem.agencyno        and
#             dclItem.bk_tradetype    == last_dclItem.bk_tradetype    and
#             dclItem.sk_confirmdate  == last_dclItem.sk_confirmdate :
 
            
#            pass
#         else:
#             list2.append(
#                 typeRow(    #tano
#                         ,   #cserialno
#                         ,   #cserialno
#                         ,   #customerid
#                         ,   #custtype
#                         ,   #sk_fundaccount
#                         ,   #sk_tradeaccount
#                         ,   #agencyno
#                         ,   #netno
#                         ,   #bk_fundcode
#                         ,   #sharetype
#                         ,   #sk_agency
#                         ,   #regdate
#                         ,   #effective_from
#                         ,   #effective_to
#                         ,   #orinetvalue
#                         ,   #orishares
#                         ,   #oricost
#                         ,   #lastshares
#                         ,   #shrchg
#                         ,   #shares
#                         ,   #sharesofnocost
#                         ,   #cost
#                         ,   #incmofnocost
#                         ,   #income
#                         ,   #costofincome
#                         ,   #totalcost
#                         ,   #totalincmofnocost
#                         ,   #totalincome
#                         ,   #totalcostofincome
#                         ,   #bourseflag
#                         ,   #srcsys
#                         ,   #batchno
#                         ,   #sk_audit
#                         ,   #inserttime
#                         ,   #updatetime
#                         ,   #shrchgofnocost
#                         ,   #bk_tradetype
#                         , ))#ori_bk_tradetype
#     return list2