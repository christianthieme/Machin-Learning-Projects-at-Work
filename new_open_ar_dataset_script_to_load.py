# -*- coding: utf-8 -*-
"""
Created on Wed Oct  2 12:27:48 2019

@author: cthieme
"""

#imports
import pandas as pd
import re
import pyhdb
from pandas import DataFrame
import pyodbc 



#Pulling 2019 Data from Hana #######################################

connection = pyhdb.connect(
host="xx.xx.com",
port=xx,
user="xx",
password="xxx"
)
cursor = connection.cursor()
cursor.execute('SELECT inv."CUSTOMER_NUMBER", cust."CUSTOMER_NAME_1",\
cust."ACCOUNT_GROUP",\
cust."COUNTRY",\
inv."FISCAL_QUARTER",\
inv."FISCAL_PERIOD",\
d."FISCAL_DAY_OF_QUARTER_NUMBER",\
d."FISCAL_DAY_OF_PERIOD_NUMBER",\
d."WORK_WEEK",\
d."WORK_WEEK_DAY_OF_WORK_WEEK_NUMBER",\
"COMPANY_CODE",\
"GL_ACCOUNT_NUMBER",\
"ACCOUNTING_DOCUMENT_TYPE",\
"ACCOUNTING_DOCUMENT_NUMBER",\
"POSTING_DATE",\
"CLEARING_DATE",\
"CREATED_BY",\
"AMOUNT_IN_GROUP_CONSOLIDATED"\
FROM (SELECT \
"CUSTOMER_NUMBER",\
"GL_ACCOUNT_NUMBER",\
"ACCOUNTING_DOCUMENT_TYPE",\
"ACCOUNTING_DOCUMENT_NUMBER",\
"POSTING_DATE",\
"CLEARING_DATE",\
"CREATED_BY",\
"FISCAL_QUARTER",\
"FISCAL_PERIOD",\
"COMPANY_CODE",\
"AMOUNT_IN_GROUP_CONSOLIDATED"\
FROM "_SYS_BIC"."HCDW.IT.FINANCE/R_ACCOUNTING_DOCUMENT"\
(\'PLACEHOLDER\' = (\'$$IP_COMPANY_CODE$$\', \'\'\'ALL\'\'\'),\
 \'PLACEHOLDER\' = (\'$$IP_FISCAL_YEAR$$\', \'\'\'2019\'\'\'),\
 \'PLACEHOLDER\' = (\'$$IP_FISCAL_PERIOD$$\', \'\'\'ALL\'\'\')) \
WHERE POSTING_KEY IN (\'01\')\
AND GL_ACCOUNT_NUMBER NOT IN (\'114000\', \'113000\', \'119800\')\
AND ACCOUNTING_DOCUMENT_TYPE IN (\'ZR\',\'DA\',\'DR\',\'ZM\',\'DG\',\'DZ\',\'RC\',\
\'RD\',\'RV\',\'DS\', \'KN\')\
AND (CLEARING_DOCUMENT_NUMBER IS NOT NULL \
AND CLEARING_DOCUMENT_NUMBER <> \' \'  AND CLEARING_DOCUMENT_NUMBER <> \'\')\
AND (REVERSAL_INDICATOR IS NULL OR REVERSAL_INDICATOR = \'\' OR REVERSAL_INDICATOR = \' \') \
ORDER BY "AMOUNT_IN_GROUP_CONSOLIDATED"\
) as inv \
LEFT JOIN "_SYS_BIC"."HCDW.IT.SHARED/D_CUSTOMER" as cust \
ON inv.CUSTOMER_NUMBER = cust.CUSTOMER_NUMBER \
LEFT JOIN "_SYS_BIC"."HCDW.IT.SHARED/D_ENTERPRISE_DATE" as d on inv.POSTING_DATE = d.full_date_trimmed \
ORDER BY "POSTING_DATE" ')
df = DataFrame(cursor.fetchall())
df.columns = [x[0] for x in cursor.description]
##df.to_csv(r'C:\Users\cthieme\OneDrive - Micron Technology, Inc\Test folder\test_ar_data_export.csv', index = False)
hana_data_2019 = df.copy()


#Pulling 2018 Data from Hana #######################################
connection = pyhdb.connect(
host="xx.xxx.com",
port=xxx,
user="xxx",
password="xxx"
)
cursor = connection.cursor()
cursor.execute('SELECT inv."CUSTOMER_NUMBER", cust."CUSTOMER_NAME_1",\
cust."ACCOUNT_GROUP",\
cust."COUNTRY",\
inv."FISCAL_QUARTER",\
inv."FISCAL_PERIOD",\
d."FISCAL_DAY_OF_QUARTER_NUMBER",\
d."FISCAL_DAY_OF_PERIOD_NUMBER",\
d."WORK_WEEK",\
d."WORK_WEEK_DAY_OF_WORK_WEEK_NUMBER",\
"COMPANY_CODE",\
"GL_ACCOUNT_NUMBER",\
"ACCOUNTING_DOCUMENT_TYPE",\
"ACCOUNTING_DOCUMENT_NUMBER",\
"POSTING_DATE",\
"CLEARING_DATE",\
"CREATED_BY",\
"AMOUNT_IN_GROUP_CONSOLIDATED"\
FROM (SELECT \
"CUSTOMER_NUMBER",\
"GL_ACCOUNT_NUMBER",\
"ACCOUNTING_DOCUMENT_TYPE",\
"ACCOUNTING_DOCUMENT_NUMBER",\
"POSTING_DATE",\
"CLEARING_DATE",\
"CREATED_BY",\
"FISCAL_QUARTER",\
"FISCAL_PERIOD",\
"COMPANY_CODE",\
"AMOUNT_IN_GROUP_CONSOLIDATED"\
FROM "_SYS_BIC"."HCDW.IT.FINANCE/R_ACCOUNTING_DOCUMENT"\
(\'PLACEHOLDER\' = (\'$$IP_COMPANY_CODE$$\', \'\'\'ALL\'\'\'),\
 \'PLACEHOLDER\' = (\'$$IP_FISCAL_YEAR$$\', \'\'\'2018\'\'\'),\
 \'PLACEHOLDER\' = (\'$$IP_FISCAL_PERIOD$$\', \'\'\'ALL\'\'\')) \
WHERE POSTING_KEY IN (\'01\')\
AND GL_ACCOUNT_NUMBER NOT IN (\'114000\', \'113000\', \'119800\')\
AND ACCOUNTING_DOCUMENT_TYPE IN (\'ZR\',\'DA\',\'DR\',\'ZM\',\'DG\',\'DZ\',\'RC\',\
\'RD\',\'RV\',\'DS\', \'KN\')\
AND (CLEARING_DOCUMENT_NUMBER IS NOT NULL \
AND CLEARING_DOCUMENT_NUMBER <> \' \'  AND CLEARING_DOCUMENT_NUMBER <> \'\')\
AND (REVERSAL_INDICATOR IS NULL OR REVERSAL_INDICATOR = \'\' OR REVERSAL_INDICATOR = \' \') \
ORDER BY "AMOUNT_IN_GROUP_CONSOLIDATED"\
) as inv \
LEFT JOIN "_SYS_BIC"."HCDW.IT.SHARED/D_CUSTOMER" as cust \
ON inv.CUSTOMER_NUMBER = cust.CUSTOMER_NUMBER \
LEFT JOIN "_SYS_BIC"."HCDW.IT.SHARED/D_ENTERPRISE_DATE" as d on inv.POSTING_DATE = d.full_date_trimmed \
ORDER BY "POSTING_DATE" ')
df1 = DataFrame(cursor.fetchall())
df1.columns = [x[0] for x in cursor.description]
##df.to_csv(r'C:\Users\cthieme\OneDrive - Micron Technology, Inc\Test folder\test_ar_data_export.csv', index = False)
hana_data_2018 = df1.copy()

##################### Merging Hana Data together ##############################

hana_data = hana_data_2019.append(hana_data_2018)


##################### Cleaning / Feature Engineering of Hana Data #############################################

hana_data["POSTING_DATE"] = pd.to_datetime(hana_data["POSTING_DATE"])
hana_data["CLEARING_DATE"] = pd.to_datetime(hana_data["CLEARING_DATE"])
hana_data["DAYS_TO_PAY"] = hana_data["CLEARING_DATE"] - hana_data["POSTING_DATE"]
hana_data["DAYS_TO_PAY"] = hana_data["DAYS_TO_PAY"].dt.days
hana_data["DAYS_TO_PAY"] = hana_data["DAYS_TO_PAY"].astype('int32')
hana_data["ACCOUNTING_DOCUMENT_NUMBER"] = hana_data["ACCOUNTING_DOCUMENT_NUMBER"].astype(str)
pattern = r'([0-9]+)'
hana_data["ACCOUNTING_DOCUMENT_NUMBER"] = hana_data["ACCOUNTING_DOCUMENT_NUMBER"].str.extract(pattern)

##################### Read in CSV from Ryan Ellis with Due Date and Payment Terms and clean ##########################

pay_terms_n_due_dt = pd.read_csv(r'C:\Users\xxx\BSAD_historical.txt', sep= ",")
pay_terms_n_due_dt.dropna(subset=["terms"], inplace = True)
pay_terms = pay_terms_n_due_dt.drop_duplicates(subset = ["docnum"], keep = 'last')
pt_dd = pay_terms[['docnum', 'terms', 'due_date']]
pay_due = pt_dd.rename(columns = {'docnum':'ACCOUNTING_DOCUMENT_NUMBER'})
pay_due["ACCOUNTING_DOCUMENT_NUMBER"] = pay_due["ACCOUNTING_DOCUMENT_NUMBER"].astype(str)
pay_due["ACCOUNTING_DOCUMENT_NUMBER"] = pay_due["ACCOUNTING_DOCUMENT_NUMBER"].str.replace("^0+", "")
pattern = r'([0-9]+)'
pay_due["ACCOUNTING_DOCUMENT_NUMBER"] = pay_due["ACCOUNTING_DOCUMENT_NUMBER"].str.extract(pattern)


################# Merge Hana and Ryan Ellis Data ####################################################

hana_pt = pd.merge(left = hana_data, right = pay_due,  how = 'left', on = "ACCOUNTING_DOCUMENT_NUMBER")
hana_pt["due_date"] = pd.to_datetime(hana_pt["due_date"])


################# Pulling Date Data from BOMSSPROD66 SHARED D_ENTERPRISE_DATE ###############################


conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=xxx, xxx;'
                      'Database=SHARED;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()
df = pd.read_sql_query('SELECT [FULL_DATE] as [DATE]\
,[FISCAL_DAY_OF_YEAR_NO] AS [FISCAL_DAY_YEAR_NO]\
,[FISCAL_QTR] AS [FQ]\
,[FISCAL_PERIOD] AS [FISCAL_P]\
,[FISCAL_MONTH] AS [FIS MONTH]\
,[FISCAL_DAY_OF_PERIOD_NO] AS [FISC DAY OF PERIOD]\
,[FISCAL_DAY_OF_QTR_NO] AS [FISC DAY OF QTR]\
,[FISCAL_MONTH_END_DATE_FLG] AS [FISCAL MONTH END FLAG]\
,[WORK_WEEK] AS [WW]\
,[WW_DAY_OF_WORK_WEEK_NO] AS [DAY OF WW]\
,[CALENDAR_QTR_END_DATE_FLG] AS [CALENDAR QTR END FLAG]\
,[CALENDAR_MONTH_END_DATE_FLG] AS [CALENDAR MONTH END FLAG]\
,[CALENDAR_WEEK_END_DATE_FLG] AS [CALENDAR WEEK END FLAG]\
,[DAY_NUM_OF_WEEK] AS [DAY OF WEEK]\
,[WEEKEND_FLG] AS [WEEKEND FLAG]\
FROM [SHARED].[dbo].[D_ENTERPRISE_DATE] \
WHERE FULL_DATE > \'2016-01-01\' \
AND FULL_DATE < \'9000-01-01\' \
', conn)
date = df.copy()

conn.close()

date["DATE"] = pd.to_datetime(date["DATE"])
date.rename(columns = {"DATE": "due_date" }, inplace = True)

####################### Merging date table with Hana_PT table ############################################

full_data = pd.merge(left = hana_pt, right = date,  how = 'left', on = "due_date")
full_data["AMOUNT_IN_GROUP_CONSOLIDATED"] = full_data["AMOUNT_IN_GROUP_CONSOLIDATED"].astype(int)


######## BRINGING IN DATA FROM BOMSSPROD66 F_INTEGRATED_SHIPMENTS 2018 ##################################

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=xxxx, xxx;'
                      'Database=SALES;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()
df = pd.read_sql_query('SELECT iship.[SOURCE_DATA_TYPE] AS [SOURCE DATA TYPE] \
,iship.[SALES_DOC_NBR] \
,iship.[PAYMENT_TERMS] \
,iship.BILLING_DOC_NUMBER AS [BILLING DOC NUMBER] \
,od.ORDER_REASON_DESC  AS [ORDER REASON] \
, sold_to_cust.CUSTOMER_NAME AS [SOLD TO CUST NAME] \
, sold_to_cust.COUNTRY_CODE  AS [SOLD TO COUNTRY CODE] \
, CASE WHEN iship.[SOURCE_DATA_TYPE] = \'POS\' \
         THEN dship_to_cust.DISTI_DISPLAY_NAME \
         ELSE ship_to_cust.CUSTOMER_NAME \
         END   AS [SHIP TO CUST NAME] \
  , CASE WHEN iship.[SOURCE_DATA_TYPE] = \'POS\' \
         THEN dship_to_cust.DISTI_COUNTRY \
         ELSE ship_to_cust.COUNTRY_CODE \
         END  AS [SHIP TO COUNTRY CODE] \
FROM SALES.dbo.F_INTEGRATED_SHIPMENT_DTL iship \
INNER JOIN  SALES.dbo.D_ORDER_REASON od ON iship.ORDER_REASON_DWID = od.ORDER_REASON_DWID \
INNER JOIN  SHARED.dbo.D_CUSTOMER sold_to_cust ON iship.SOLD_TO_CUSTOMER_DWID = sold_to_cust.CUSTOMER_DWID \
INNER JOIN  SHARED.dbo.D_CUSTOMER ship_to_cust ON iship.SHIP_TO_CUSTOMER_DWID = ship_to_cust.CUSTOMER_DWID \
INNER JOIN  SHARED.dbo.D_DISTI_CUSTOMER dship_to_cust ON iship.DISTI_SHIP_TO_CUSTOMER_DWID = dship_to_cust.DISTI_CUSTOMER_DWID \
WHERE iship.ACTUAL_GI_DATE BETWEEN \'2017-07-30 00:00:00.000\'  AND \'2018-08-29 00:00:00.000\' \
AND BILLING_DOC_NUMBER <> \'0\' \
ORDER BY [BILLING DOC NUMBER] \
', conn)
f_ship_data_2018 = df.copy()

######## BRINGING IN DATA FROM BOMSSPROD66 F_INTEGRATED_SHIPMENTS 2019 ##################################

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=xxx, xxx;'
                      'Database=SALES;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()
df = pd.read_sql_query('SELECT iship.[SOURCE_DATA_TYPE] AS [SOURCE DATA TYPE] \
,iship.[SALES_DOC_NBR] \
,iship.[PAYMENT_TERMS] \
,iship.BILLING_DOC_NUMBER AS [BILLING DOC NUMBER] \
,od.ORDER_REASON_DESC  AS [ORDER REASON] \
, sold_to_cust.CUSTOMER_NAME AS [SOLD TO CUST NAME] \
, sold_to_cust.COUNTRY_CODE  AS [SOLD TO COUNTRY CODE] \
, CASE WHEN iship.[SOURCE_DATA_TYPE] = \'POS\' \
         THEN dship_to_cust.DISTI_DISPLAY_NAME \
         ELSE ship_to_cust.CUSTOMER_NAME \
         END   AS [SHIP TO CUST NAME] \
  , CASE WHEN iship.[SOURCE_DATA_TYPE] = \'POS\' \
         THEN dship_to_cust.DISTI_COUNTRY \
         ELSE ship_to_cust.COUNTRY_CODE \
         END  AS [SHIP TO COUNTRY CODE] \
FROM SALES.dbo.F_INTEGRATED_SHIPMENT_DTL iship \
INNER JOIN  SALES.dbo.D_ORDER_REASON od ON iship.ORDER_REASON_DWID = od.ORDER_REASON_DWID \
INNER JOIN  SHARED.dbo.D_CUSTOMER sold_to_cust ON iship.SOLD_TO_CUSTOMER_DWID = sold_to_cust.CUSTOMER_DWID \
INNER JOIN  SHARED.dbo.D_CUSTOMER ship_to_cust ON iship.SHIP_TO_CUSTOMER_DWID = ship_to_cust.CUSTOMER_DWID \
INNER JOIN  SHARED.dbo.D_DISTI_CUSTOMER dship_to_cust ON iship.DISTI_SHIP_TO_CUSTOMER_DWID = dship_to_cust.DISTI_CUSTOMER_DWID \
WHERE iship.ACTUAL_GI_DATE > \'2018-08-29 00:00:00.000\' \
AND BILLING_DOC_NUMBER <> \'0\' \
ORDER BY [BILLING DOC NUMBER] \
', conn)
f_ship_data_2019 = df.copy()

conn.close()

#########################################   Merging Shipment Data 2018 & 2019 and Cleaning ###################################

f_ship_data = f_ship_data_2018.append(f_ship_data_2019)
f_ship_data['BILLING DOC NUMBER'] = f_ship_data['BILLING DOC NUMBER'].str[1:]
f_ship_data['BILLING DOC NUMBER'] = f_ship_data['BILLING DOC NUMBER'].str.strip()
f_ship_data = f_ship_data.dropna(subset = ["BILLING DOC NUMBER"])
f_ship_data = f_ship_data.drop_duplicates(subset = ["BILLING DOC NUMBER"], keep = 'last')
f_ship_data.rename(columns = {"BILLING DOC NUMBER": "ACCOUNTING_DOCUMENT_NUMBER" }, inplace = True)
f_ship_data["ORDER REASON"] = f_ship_data["ORDER REASON"].str.replace('?', 'None')
f_ship_data["SOLD TO CUST NAME"] = f_ship_data["SOLD TO CUST NAME"].str.replace('?', 'None')
f_ship_data["SOLD TO COUNTRY CODE"] = f_ship_data["SOLD TO COUNTRY CODE"].str.replace('?', 'Unknown')
f_ship_data["SHIP TO CUST NAME"] = f_ship_data["SHIP TO CUST NAME"].str.replace('?', 'None')
f_ship_data["SHIP TO COUNTRY CODE"] = f_ship_data["SHIP TO COUNTRY CODE"].str.replace('?', 'Unknown')
for col in f_ship_data.columns: 
    f_ship_data[col] = f_ship_data[col].str.replace('NA', 'Unknown')
    f_ship_data[col] = f_ship_data[col].str.replace('N/A', 'Unknown')
    
##################################   Merging all data for final dataset and cleaning #####################################

merged_data = pd.merge(left = full_data, right = f_ship_data,  how = 'left', on = "ACCOUNTING_DOCUMENT_NUMBER")
merged_data["AMOUNT_IN_GROUP_CONSOLIDATED"] = merged_data["AMOUNT_IN_GROUP_CONSOLIDATED"].astype(int)
merged_data["DAYS_TIL_DUE"] = merged_data['due_date'] - merged_data['POSTING_DATE']
merged_data["DAYS_TIL_DUE"] = merged_data["DAYS_TIL_DUE"].dt.days
merged_data["DAYS_TIL_DUE"] = merged_data["DAYS_TIL_DUE"].astype('int32')
merged_data["ACCOUNTING_DOC_START"] = merged_data["ACCOUNTING_DOCUMENT_NUMBER"].str[:2]

nan_columns = ['SOURCE DATA TYPE', 'SALES_DOC_NBR', 'PAYMENT_TERMS',
       'ORDER REASON', 'SOLD TO CUST NAME', 'SOLD TO COUNTRY CODE',
       'SHIP TO CUST NAME', 'SHIP TO COUNTRY CODE']
merged_data[nan_columns] = merged_data[nan_columns].fillna(value = 'No Detail')

hana_nan_columns = ['CUSTOMER_NAME_1', 'ACCOUNT_GROUP', 'COUNTRY']
merged_data[hana_nan_columns] = merged_data[hana_nan_columns].fillna(value = 'No Detail')

merged_data.dropna(subset=['FISCAL_DAY_OF_YEAR_NO', 'FISCAL_QTR',
       'FISCAL_DAY_OF_QTR_NO', 'FISCAL_PERIOD_y', 'FISCAL_MONTH',
       'FISCAL_DAY_OF_PERIOD_NO', 'FISCAL_DAY_OF_PERIOD_NO_REMAIN',
       'FISCAL_MONTH_END_DATE_FLG', 'WORK_WEEK_y', 'WW_DAY_OF_WORK_WEEK_NO',
       'CALENDAR_QTR_END_DATE_FLG', 'CALENDAR_MONTH_END_DATE_FLG',
       'CALENDAR_WEEK_END_DATE_FLG', 'DAY_OF_WEEK_3', 'DAY_NUM_OF_WEEK',
       'WEEKEND_FLG'], inplace= True)

########################## CREATING INVOICE TIME OF MONTH CALCS ############################################
def time_of_month_calc(column):
    if column.isnull(): 
        return "No Detail"
    if column <= 10: 
        return "beginning"
    if column <= 20: 
        return "middle"
    else:
        return "end"
    
merged_data["INV_TIME_OF_MONTH"] = merged_data["FISCAL_DAY_OF_PERIOD_NUMBER"].apply(time_of_month_calc)

def time_of_week_calc(column):
    if column.isnull(): 
        return "No Detail"
    if column <= 2: 
        return "beginning"
    if column <= 4: 
        return "middle"
    else:
        return "end"

merged_data["INV_TIME_OF_WEEK"] = merged_data["WORK_WEEK_DAY_OF_WORK_WEEK_NUMBER"].apply(time_of_week_calc)

def time_of_month_qtr(column):
    if column.isnull(): 
        return "No Detail"
    if column <= 30: 
        return "beginning"
    if column <= 60: 
        return "middle"
    else:
        return "end"
    
merged_data["INV_TIME_OF_QTR"] = merged_data["FISCAL_DAY_OF_QTR_NO"].apply(time_of_month_qtr)

########################## CREATING DUE DATE TIME OF MONTH CALCS ############################################

    
merged_data["DUE_TIME_OF_MONTH"] = merged_data["FISC DAY OF PERIOD"].apply(time_of_month_calc)
merged_data["DUE_TIME_OF_WEEK"] = merged_data["DAY OF WEEK"].apply(time_of_week_calc) 
merged_data["DUE_TIME_OF_QTR"] = merged_data["FISC DAY OF QTR"].apply(time_of_month_qtr)


final_data = merged_data[(merged_data['DAYS_TIL_DUE']>0) & (merged_data['DAYS_TIL_DUE']<100)]
final_data.to_csv(r'C:\Users\xxx\NEW_AR_TRAINING_DATA.csv')