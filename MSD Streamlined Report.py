import os 
os.system('clear')
import psycopg2
import pandas as pd
import numpy as np
import xlrd
from datetime import datetime, timedelta

"""
Part 0: SQL query to pull raw data.
"""

conn = psycopg2.connect(dbname="dbname", user="username", password="pw", host="hostname", port="xxxx")
cur = conn.cursor()

cur.execute("SELECT m.id, m.date_time_c, m.customer_c, m.coach_met_with_c, m.debt_outcomes_c, m.credit_outcome_c, m.banking_outcome_c, m.assets_outcome_c, m.taxes_outcomes_c\
 FROM salesforce.customers AS cu\
 INNER JOIN salesforce.meetings m ON m.customer_c = cu.id\
 WHERE m.status_c = 'Attended'\
 AND m.type_c = 'In-person' OR m.type_c = 'Phone Call' OR m.type_c = 'Taxes' OR m.type_c = 'Inquiry' OR m.type_c = 'Workshop' OR m.type_c = 'Light-touch'\
 AND cu.fake_customer_c='False'")

#Alma 00341000019HTkJ-AAW
#Erica_NHN 0034100000lyDcH-AAU
#Erica_Tier1 0034100000pONu2
#Erica_Tier2 
#Laura 0034100001hBkud-AAC

print(cur.description)

col_names = []
for elt in cur.description:
    print(elt[0])
    col_names.append(elt[0])

All = cur.fetchall()

raw = pd.DataFrame(All, columns=col_names)

raw.to_csv("raw.csv")

"""
Part 1: Clean raw data pulled from SQL query to create "master" df.
"""

def clean_raw(df):
	"""Cleans raw data that was pulled from postgresql database."""
	
	def to_binary(df, idx, NewColName, OriginalCol):
		"""Creates new column: 0 if outcome was not met during meeting, 1 if outcome was met during meeting. Drops original column."""
		df.insert(loc=idx, column=NewColName, value=np.where(df[OriginalCol] >= 1, 1, 0))
		df.drop([OriginalCol], axis=1, inplace=True)

	def boolean_to_binary(df, cols):
		"""Converts Boolean values to zeroes and ones for the columns specified."""
		df[cols] *= 1
		return df
	
	to_binary(df, 5, 'Debt', 'debt_outcomes_c') # insert debt binary values at index 5, under column header titled 'Debt'
	to_binary(df, 8, 'Taxes', 'taxes_outcomes_c') # insert tax binary values at index 10, under column header titled 'Taxes'
	boolean_to_binary(df, ['credit_outcome_c', 'banking_outcome_c', 'assets_outcome_c']) # convert boolean true/false to 0/1
	df['Goals'] = 0 # filler value until goal outcomes are generated from a separate goals object SQL query
	
	df.rename(columns={'coach_met_with_c':'Mgr. of SD','credit_outcome_c': 'Credit', 'banking_outcome_c': 'Banking', 'assets_outcome_c': 'Assets'}, inplace=True)

	# filter df for only the coaches we are interested in
	df = df[df['Mgr. of SD'].isin(['0034100001hBkudAAC','00341000019HTkJAAW','0034100000lyDcHAAU','0034100000pONzGAAW'])]
	
	# replace coach_met_with_c ID to coach name
	df['Mgr. of SD'].replace(['0034100001hBkudAAC'], ['Laura Christensen-Garcia'], inplace=True) 
	df['Mgr. of SD'].replace(['00341000019HTkJAAW'], ['Alma Rojas'], inplace=True) 
	df['Mgr. of SD'].replace(['0034100000lyDcHAAU'], ['Erica Mancinas'], inplace=True) 
	df['Mgr. of SD'].replace(['0034100000pONzGAAW'], ['Erica Mancinas'], inplace=True)
	
	#Alma 00341000019HTkJ-AAW
	#Erica_NHN 0034100000lyDcH-AAU
	#Erica_Tier1 0034100000pONu2
	#Erica_Tier2 
	#Laura 0034100001hBkud-AAC
	
	# create new columns, DeficitOriented, AssetOriented, and AM, in order to prepare for later mapping
	df['DeficitOriented'] = np.nan
	df['AssetOriented'] = np.nan
	df['Achieved'] = np.nan
	
	return df
	
	
"""
Part 2: Create new columns in master df (which is meeting-level), so that the code for table creations later can be easy.
"""

def AchievingMission(df):
	"""Adds each unique customer's achieving mission data to the master dataframe. Modifies the master inplace."""
	
	def DeficitOriented(df):
		Deficit = (df['Debt']==1) | (df['Credit']==1) # if either debt equals 1 or credit equals 1 for any given customer, set "Deficit" variable equal to True
		df['DeficitOriented'] = Deficit
	
	def AssetOriented(df):
		Asset = (df['Banking']==1) | (df['Assets']==1) | (df['Taxes']==1) | (df['Goals']==1)
		df['AssetOriented'] = Asset

	def Achieved(df):
		"""If customer received at least one asset-oriented and at least one deficit oriented outcome, True. Otherwise, False."""
		AM = (df['DeficitOriented']==1) & (df['AssetOriented']==1)
		df['Achieved'] = AM
	
	def AchievedDate(df):
		"""Calculates the date that mission was achieved, and returns the coach met with on that date."""
		
	d = df.groupby('customer_c')['Debt'].sum() 
	# groupby customer, sum meeting-level 0s and 1s for debt outcome; if cust ever reached the outcome, the sum will be exactly 1; if cust never reached, the sum will be exactly 0.
	c = df.groupby('customer_c')['Credit'].sum()
	b = df.groupby('customer_c')['Banking'].sum()
	a = df.groupby('customer_c')['Assets'].sum()
	t = df.groupby('customer_c')['Taxes'].sum()
	g = df.groupby('customer_c')['Goals'].sum()
	AM = [d, c, b, a, t, g] 
	achieved_mission = pd.concat(AM, axis=1)
	
	# calculate Achieving Mission using d, c, b, a, t, and g
	DeficitOriented(achieved_mission)
	AssetOriented(achieved_mission)
	Achieved(achieved_mission) # index of achieved_mission before dict conversion is customer_c
	
	# drop d, c, b, a, t, and g
	x = achieved_mission.drop(['Debt', 'Credit', 'Banking', 'Assets', 'Taxes', 'Goals'], axis=1)

	# set index of df in preparation for join
	df.set_index('customer_c', inplace=True) # index of x already set to customer_c
	df = df.join(x, lsuffix = 'key_df', rsuffix = 'key_x')
	df = df.drop(['DeficitOrientedkey_df','AssetOrientedkey_df','Achievedkey_df'], axis=1)
	df = df.reset_index()
	print("This is the new master df: ", df)

	return df


"""
Part 3: Create tables in the Biweekly Ecosystem Service Delivery Report.
"""

def table_one(df):
	print(df.columns) # the columns in df parameter
	
	debt = df.groupby('Mgr. of SD')['Debt'].sum() # groupby coach met with, sum debt outcomes earned by each coach
	credit = df.groupby('Mgr. of SD')['Credit'].sum()
	banking = df.groupby('Mgr. of SD')['Banking'].sum()
	assets = df.groupby('Mgr. of SD')['Assets'].sum()
	taxes = df.groupby('Mgr. of SD')['Taxes'].sum()
	goals = df.groupby('Mgr. of SD')['Goals'].sum()
	total_uniquecust = df.groupby('Mgr. of SD')['customer_c'].nunique()
	total_meetings = df.groupby('Mgr. of SD')['id'].nunique() # id is the record id for the meeting object; each instance (meeting) has a unique id
	
	def drop_duplicates(df):
		df.drop_duplicates('customer_c')
		am = df.groupby('Mgr. of SD')['Achievedkey_x'].sum()
		return am
		
	achieving_mission = drop_duplicates(df)
	
	data = [debt, credit, banking, assets, taxes, goals, total_uniquecust, total_meetings] # merge series on shared index
	table_one = pd.concat(data, axis=1)
	
	# insert total outcomes at col index 6
	idx = 6
	table_one.insert(loc=idx, column='Total Outcomes', value=table_one[['Debt', 'Credit', 'Banking', 'Assets', 'Taxes', 'Goals']].sum(axis=1))
	
	# insert achieving mission at col index 7
	idx = 7
	table_one.insert(loc=idx, column='Achieving Mission', value=achieving_mission)
	
	table_one.rename(columns={'customer_c':'Total Customers', 'id':'Total Meetings'}, inplace=True)
	
	print("table one: ", table_one)
	return table_one

	
def table_two(df):
	"""Accepts df as an argument, filters for data from last 14 days, and calls table_one() to generate same column headers."""
	N=14
	today = pd.datetime.now().date()
	TwoWeeksAgo = today-timedelta(days=N)
	df = df[df['date_time_c'] >= pd.Timestamp(TwoWeeksAgo)] # shows same data as master, but only data points since 2 weeks ago
	date_filtered = df.to_csv("date_filtered.csv")
	table_two = table_one(df) # puts date-filtered master into the table_one function
	return table_two

def table_three(df):
	m = df['Total Meetings']
	o = df['Total Outcomes']
	data = [m,o]
	table_three = pd.concat(data, axis=1)
	
	# count number of days since first day of current year
	FirstDay = pd.Timestamp(datetime(2018, 1, 1))
	today = pd.Timestamp.today()
	delta = today - FirstDay
	PercentYearPassed = delta.days/365
	
	# targets for 2018; in other words, the "expected" number of outcomes, customers, and meetings by END OF YEAR
	MeetingsTarget = 640
	OutcomesTarget = 235
	
	# the "expected" # of outcomes, customers, and meetings by TODAY
	ByTodayMeetings = PercentYearPassed*MeetingsTarget
	ByTodayOutcomes = PercentYearPassed*OutcomesTarget
	
	# Annual Target (achieved): Progress toward "ByToday" target as a percentage
	table_three.insert(loc=1, column='Meetings Progress', value=table_three['Total Meetings']/ByTodayMeetings)
	table_three.insert(loc=3, column='Outcomes Progress', value=table_three['Total Outcomes']/ByTodayOutcomes)

	# Delta
	table_three.insert(loc=2, column='Meetings Delta', value=table_three['Meetings Progress']-1.0)
	table_three.insert(loc=5, column='Outcomes Delta', value=table_three['Outcomes Progress']-1.0)
	
	# A-O Outcomes
	
	return table_three
	
	
	

cleaned = clean_raw(raw)
cleaned.to_csv('cleaned.csv')

master = AchievingMission(cleaned)
master.to_csv('master.csv')

table1 = table_one(master)
table1.to_csv('table1.csv')

table2 = table_two(master)
table2.to_csv('table2.csv')

table3 = table_three(table1)
table3.to_csv('table3-notdf.csv')






