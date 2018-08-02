"""
Author: Fiona
"""

import pandas as pd
import numpy as np
from datetime import datetime
from pandas import ExcelWriter

"""

Before running this code, make sure you've run Sindhi's Python code (relatively recently) to generate:
1) the ClinicCustomersOutcomesPYTHON2018_final.xls file containing OFE outcomes Asset, Credit, Debt, Goals, Taxes
2) the ClinicCustomersCashValuePYTHON2018_final.xls file containing OFE Banking outcome

"""

path = "/home/fiona/Dropbox (The Financial Clinic)/Data/2 Internal Reports/Biweekly Coaching Reports/RawOFEETO/ClinicCustomersOutcomesPYTHON2018_2018-4-2.xls"
df = pd.read_excel(path)

path2 = "/home/fiona/Dropbox (The Financial Clinic)/Data/2 Internal Reports/Biweekly Coaching Reports/RawOFEETO/ClinicCustomersCashValuePYTHON2018_2018-4-2.xls"
df2 = pd.read_excel(path2)

def outcomes_ACDGT(df):
	# drop needless columns
	df.drop(df.ix[:,'Attributed Staff Name_135':'DebtActiveAmount'].head(0).columns, axis=1, inplace=True)
	
	# DateTaken is the most recent meeting date that the customer had; LastUpdatedBy is the name of the coach that the customer met with on that meeting date
	df.rename(columns={"Subject Unique Identifier":"Subject Identifier","LastUpdatedBy":"Coach Met With", "DateTaken":"Most Recent Meeting Date", "Name":"Customer Name"}, inplace=True)
	
	# most recent meeting date should be after 2018-01-01 and before 2018-12-31
	df = df[df['Most Recent Meeting Date'] >= pd.Timestamp("2018-01-01")] 
	
	# drop needless columns
	df = df.drop(['Assigned Staff','Participant Site Identifier','TotalMeetings','MaxTimeWithCoach'], axis=1)

	return df


def outcome_B(df):
	# DateTaken and BankingOutcomeDate both signify the dates on which a banking outcome was reached
	df = df[df['DateTaken'] >= pd.Timestamp("2018-01-01")] 
		
	# drop needless columns
	df = df.drop(df.ix[:,'Subject Name':'BankingBinary'].head(0).columns, axis=1)
	df = df.drop(['BankingOutcomeMinDate'], axis=1)
		
	return df


def outcomes_01(df):
	"""Generate outcomes as 0s and 1s for each customer, to signify outcome was not met/met. Totals can then be used to generate coach outcomes."""
	idx=7
	df.insert(loc=idx, column='asset_outcome', value=(df['AutodeductionDate'].notnull()) | (df['SavedTwiceDate'].notnull()))
	
	idx=9
	df.insert(loc=idx, column='credit_outcome', value=(df['CreditOutcomeDate'].notnull()))
	
	idx=11
	df.insert(loc=idx, column='debt_outcome', value=(df['DebtOutcomeDate'].notnull()))
	
	idx=13
	df.insert(loc=idx, column='goal_outcome', value=(df['GoalOutcomeDate'].notnull()))
	
	idx=15
	df.insert(loc=idx, column='tax_outcome', value=(df['TaxOutcomeDate'].notnull()))
	
	idx=16
	df.insert(loc=idx, column='banking_outcome', value=(df['BankingOutcomeDate'].notnull()))
	
	df = df.drop(['AutodeductionDate','SavedTwiceDate','CreditOutcomeDate','DebtOutcomeDate','GoalOutcomeDate','BankingOutcomeDate','TaxOutcomeDate'], axis=1)
	
	return df

ACDGT = outcomes_ACDGT(df)
B = outcome_B(df2)

# use an outer join on shared "Subject Identifier" columns; resulting dataframe should not exclude ANY customer
all_outcomes = pd.merge(ACDGT, B, how='outer', on='Subject Identifier')

# create zero one outcomes
copy = all_outcomes.copy()
ZeroOne_outcomes = outcomes_01(copy)

# write to csv
today = pd.datetime.now().date()
writer = pd.ExcelWriter(f'OFEOutcomes_ClinicStandards_{today}.xlsx')
all_outcomes.to_excel(writer, 'all_outcomes', index=False) # first sheet
ZeroOne_outcomes.to_excel(writer, '01_outcomes', index=False) # second sheet
writer.save()
