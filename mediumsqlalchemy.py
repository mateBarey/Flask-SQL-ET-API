import sqlalchemy as db
from sqlalchemy import create_engine
import os 
import pandas as pd 

####EXTRACT TRANSFORM //// USING CORE METHOD vs ORM method

os.chdir('/home/iceagefarmer/go')
f_name = 'chinook.db'



engine = create_engine('sqlite:///'+f_name).connect()

metadata = db.MetaData()

tracks = db.Table('Tracks',metadata,autoload=True,autoload_with=engine)


'''
SELECT statement
'''
query = db.select([tracks.columns['Name'],tracks.columns['Composer'],tracks.columns['Milliseconds']])

ResultProxy = engine.execute(query)
result = ResultProxy.fetchall()


#put in dataframe
track_data = pd.DataFrame(result,columns=['Name','Composer','Milliseconds'])

#print(track_data)

'''
WHERE clause LIKE operator AND operator
'''

query = query.where(db.and_(tracks.columns['Milliseconds']<=500000,
tracks.columns['Name'].like('%THE')))
rp2 = engine.execute(query)
result2 = rp2.fetchall()

tr_data = pd.DataFrame(result2,columns=['Name','Cprint(dir(customers))omposer','Milliseconds'])

#print(tr_data)


'''
Finding Column names in specific table 
'''

names = db.Table('Tracks',metadata,autoload=True,autoload_with=engine)
col_names = [c.name for c in names.columns]


'''
finding table names in database
'''
engine = create_engine('sqlite:///'+f_name)
tb_names = engine.table_names()


'''
output column names for table Tracks
'''

#print(col_names)


'''
output table names for database 
'''
#print(tb_names)


'''
GROUPBY ORDERBY and COUNT
'''
from sqlalchemy import func 
base_q = db.select([tracks.columns['Composer'],func.count(tracks.columns['Composer']).label('cCount')])

query = base_q.group_by(tracks.columns['Composer']).order_by(db.desc('cCount'))

rp3 = engine.execute(query)
result3= rp3.fetchall()

tr1_data = pd.DataFrame(result3,columns=['Composer','Count'])

#print(tr1_data)


'''
JOIN
'''


from sqlalchemy import join, select

invoice_items = db.Table('invoice_items',metadata,autoload=True, autoload_with=engine)

invoices = db.Table('invoices',metadata,autoload=True, autoload_with=engine)

customers =db.Table('customers',metadata,autoload=True, autoload_with=engine)

col_names1 = [c.name for c in invoice_items.columns]
col_names2 = [c.name for c in invoices.columns]
col_names3 = [c.name for c in customers.columns]

#print(col_names1)
#print(col_names2)
#print(col_names3)

first_join = invoice_items.join(invoices,invoice_items.c.InvoiceId==invoices.c.InvoiceId)
second_join = first_join.join(customers,customers.c.CustomerId==invoice_items.c.InvoiceId)

statement = select([customers.c.FirstName,customers.c.LastName,invoice_items.c.InvoiceId,invoices.c.Total]).select_from(second_join)

query = statement.where(invoices.c.Total >= 5)
rp4 = engine.execute(query)
result3= rp4.fetchall()


tr3_data = pd.DataFrame(result3,columns=['FirstName','LastName','InvoiceId','Total'])
tr3_data = tr3_data.drop_duplicates().sort_values(['Total'])
tr3_data['Total']= tr3_data['Total'].astype(str)
