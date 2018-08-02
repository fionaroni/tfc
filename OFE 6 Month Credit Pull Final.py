"""
Created on Jan 29 2018

Purpose of Report
Coaches want to know the last date they pulled a creidit report for any given customer

FT purpose of new script
Cut out STATA. Instead of reading in OFE Clinic Outcomes Report.dta, convert raw touchpoints to dataframe 
"""

import pandas as pd
import datetime as dt
import numpy as np

# read in excel file, convert to df
xls = pd.ExcelFile("/home/fiona/Dropbox (The Financial Clinic)/Data/2 Internal Reports/Biweekly Coaching Reports/RawOFEETO/ClinicCustomersTouchpoints2017.xls")
df = xls.parse()

df.rename(columns={'Last Updated By_135': 'Coach', \
"Client's Monthly Net Wages:_9016": "Customer's Monthly Wages", \
"Client's Monthly Non-Earned Income:_9095": "Customer's Monthly NonEarned Income", \
"Date Taken_135": 'Meeting Date'}, inplace=True)\

# filter for Only Active Coaches
df.where((df['Coach'] == 'Browne, Kristen') | (df['Coach'] == 'Steinberg, Viviana') | (df['Coach'] == 'Cao, Amy') | (df['Coach'] == 'Cappuccitti , Gina'), inplace=True)

# .dropna method drops all of these NaN values, but retains original indices
df.dropna(axis=0, how='all', inplace=True)

# if value of cell is "Pulled...file too thin" or "Pulled...obtained", return True, otherwise return False
def credit_pulled(df):
	if df["Current status of client's credit history:_8979"] == 'Pulled credit report and file too thin to generate TU FICO 4 score':
		return True
	if df["Current status of client's credit history:_8979"] == 'Pulled credit report and obtained TU FICO 4 score':
		return True
	else: 
		return False

# .apply method applies function, 'credit_pulled' on dataframe by row-level
df['Was credit report pulled?'] = df.apply(credit_pulled, axis=1)

# filter out any meeting dates during which credit report was NOT pulled
df = df[df['Was credit report pulled?'] == 1]


# create new column that stores date of last time credit was pulled for a given customer
df['Date of Last Pull'] = df.groupby('Participant Site Identifier')['Meeting Date'].transform('max')

# calculate difference between today and most recent [] pull for that customer
df['Number of Days Since Last Pull Date'] = (pd.datetime.now().date() - df['Date of Last Pull'])/np.timedelta64(1,'D') 

# alter for presentation
df.rename(columns={'Meeting Date': "Customer Pull Dates"}, inplace=True) # 'Date Taken_135/Meeting Date' refers to meeting date, but this csv includes only the meetings during which credit was pulled
today = pd.datetime.now().date()

# write to csv
cols_to_keep = ['Participant Site Identifier','Name','Coach',"Customer's Monthly Wages", "Customer's Monthly NonEarned Income", 'Customer Pull Dates', 'Date of Last Pull', 'Number of Days Since Last Pull Date']
final = df[cols_to_keep]
final.to_csv(f'OFE-creditpulls_{today}.csv', index=False)
