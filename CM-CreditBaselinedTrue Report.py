#Clearing previous script 
import os 
os.system('clear')

#importing relevent packages postgresql
#how to use psycopg2 http://initd.org/psycopg/docs/cursor.html
import psycopg2
import pandas as pd
import numpy as np
import xlrd

conn = psycopg2.connect(dbname="dbname", user="username", password="pw", host="hostname", port="xxxx")
cur = conn.cursor()
cur.execute("SELECT customers.name, customers.primary_coach_c, customers.credit_score_baseline_meeting_date_c, customers.credit_baseline_amount_c, customers.credit_active_meeting_amount_c, customers.credit_score_change_c\
 FROM salesforce.customers AS customers\
  WHERE customers.primary_coach_c = '003G000002YRsuQIAT'\
  AND fake_customer_c = 'False'")
  
print(cur.description)

col_names = []
for elt in cur.description:
    print(elt[0])
    col_names.append(elt[0])
        

All = cur.fetchall()

df = pd.DataFrame(All, columns=col_names)

df['primary_coach_c'].replace(['003G000002YRsuQIAT'], ['Andy Collado'], inplace=True)

today = pd.datetime.now().date()
df.to_csv(f'ChangeMachine-CreditBaselinedTrue.csv')

"""
credit_score_baseline_meeting_date_c: 
if cell is empty, then credit baseline never happened for this customer
if there is a date, then the credit baseline happened on that date
"""
