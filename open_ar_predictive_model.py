import pandas as pd
pd.options.display.max_columns = 99 
import matplotlib.pyplot as plt 
import seaborn as sns
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
import numpy as np

#loading the previously cleaned file from the other script in this repo
ar_data = pd.read_csv(r'C:\xxx\ar_training_data_set.csv')

ar_data["POSTING_DATE"] = pd.to_datetime(ar_data["POSTING_DATE"])
ar_data["CLEARING_DATE"] = pd.to_datetime(ar_data["CLEARING_DATE"])

ar_data.drop(['ACCOUNTING_DOCUMENT_NUMBER', 'POSTING_DATE', 'CLEARING_DATE', 'SALES_DOC_NBR'], axis = 1, inplace = True)

ar_cat_columns = ['CUSTOMER_NUMBER', 'CUSTOMER_NAME_1', 'ACCOUNT_GROUP', 'COUNTRY',
       'FISCAL_QUARTER', 'FISCAL_PERIOD', 'FISCAL_DAY_OF_QUARTER_NUMBER',
       'FISCAL_DAY_OF_PERIOD_NUMBER', 'WORK_WEEK',
       'WORK_WEEK_DAY_OF_WORK_WEEK_NUMBER', 'COMPANY_CODE',
       'GL_ACCOUNT_NUMBER', 'ACCOUNTING_DOCUMENT_TYPE', 'CREATED_BY',
        'SOURCE DATA TYPE', 'PAYMENT_TERMS',
       'ORDER REASON', 'SOLD TO CUST NAME', 'SOLD TO COUNTRY CODE',
       'SHIP TO CUST NAME', 'SHIP TO COUNTRY CODE', 'SALES_DOC_START',
       'TIME_OF_MONTH', 'TIME_OF_WEEK']
       
for cat in ar_cat_columns: 
    ar_data[cat] = ar_data[cat].astype('category')
    
for category in ar_cat_columns: 
    ar_data[category] =  ar_data[category].cat.codes
    
jan_mar_data = ar_data[ar_data['FISCAL_QUARTER']==0]
q4_data = ar_data[ar_data['FISCAL_QUARTER']==3]

#%%time

#RUN 4

import time
from sklearn.model_selection import cross_val_score, KFold
import math


columns = ['COMPANY_CODE', 'CUSTOMER_NUMBER', 'AMOUNT_IN_GROUP_CONSOLIDATED', 'ORDER REASON', 'WORK_WEEK', 'COUNTRY', 'ACCOUNTING_DOCUMENT_TYPE', 
                'FISCAL_DAY_OF_PERIOD_NUMBER', 'FISCAL_DAY_OF_QUARTER_NUMBER', 'TIME_OF_MONTH', 'WORK_WEEK_DAY_OF_WORK_WEEK_NUMBER', 'TIME_OF_WEEK', 'SHIP TO CUST NAME',
                'FISCAL_PERIOD', 'SALES_DOC_START', 'GL_ACCOUNT_NUMBER', 'PAYMENT_TERMS']
kf = KFold(n_splits = 4, shuffle= True)

all_x = ar_data[columns]
all_y = ar_data["DAYS_TO_PAY"]
rfr = RandomForestRegressor(n_estimators = 200, max_depth = 110, min_samples_split = 10, min_samples_leaf = 4, bootstrap = True)
# max_depth = 110, min_samples_split = 10, min_samples_leaf = 4, bootstrap = True
# rfr.fit(all_x, all_y)
# prediction = rfr.predict(test[columns])

mses = cross_val_score(rfr, all_x, all_y, scoring='neg_mean_squared_error', cv = kf)
mses_calc = np.sqrt(np.absolute(mses))
avg_rmse = np.mean(mses_calc)
print(avg_rmse)


#### output: 
### 4.3124 days

feature_importances = pd.DataFrame(rfr.feature_importances_,
                                   index = all_x.columns,
                                    columns=['importance']).sort_values('importance',  ascending=False)
