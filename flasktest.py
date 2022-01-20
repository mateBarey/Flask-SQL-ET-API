
from calendar import c
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import json
from datetime import datetime

from sqlalchemy import Table, Column, Integer, Numeric, String, DateTime, ForeignKey, Boolean, CheckConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref
from mediumsqlalchemy import *

from flask import Flask, request , jsonify

from flask_restful import Api , Resource

app = Flask(__name__)
api = Api(app)



def get_ar(stuff):
    ar = [i for i, in stuff]
    return ar 
def get_val(data):
    return json.JSONEncoder().encode(data.values.tolist())

FirstName = tr3_data.FirstName
LastName = tr3_data.LastName 
InvoiceId = tr3_data.InvoiceId 
Total = tr3_data.Total

class Customer(Resource):
    def get(self):
        
        return jsonify([
            {
                'FirstName': get_val(FirstName),'LastName':get_val(LastName),'InvoiceId':get_val(InvoiceId),'Total':get_val(Total)

            }
        ])

class NewCustomer(Resource):
    def post(self):
        global tr3_data

        postedData = request.get_json(force=True)

        fn = postedData["FirstName"]
        ln = postedData["LastName"]
        id = postedData["InvoiceId"]
        t = postedData["Total"]

        tr3_data = tr3_data.append({'FirstName':fn,'LastName':ln,'InvoiceId':id,'Total':t},ignore_index=True)

        FirstName = tr3_data.FirstName
        LastName = tr3_data.LastName 
        InvoiceId = tr3_data.InvoiceId 
        Total = tr3_data.Total

        return jsonify([
            {
                'FirstName': get_val(FirstName),'LastName':get_val(LastName),'InvoiceId':get_val(InvoiceId),'Total':get_val(Total)

            }
        ])

class Deletecustomer(Resource):
    def delete(self):
        global tr3_data
        postedData = request.get_json(force=True)

        fn = postedData["FirstName"]
        id = postedData["InvoiceId"]
       

        qr2 = "FirstName != '{}'  and InvoiceId != {}".format(fn,id)
        tr3_data = tr3_data.query(qr2)

        FirstName = tr3_data.FirstName
        LastName = tr3_data.LastName 
        InvoiceId = tr3_data.InvoiceId 
        Total = tr3_data.Total

        return jsonify([
            {
                'FirstName': get_val(FirstName),'LastName':get_val(LastName),'InvoiceId':get_val(InvoiceId),'Total':get_val(Total)

            }
        ])


class UpdateCustomerTotal(Resource):
    def put(self):

        global tr3_data 
        postedData = request.get_json(force=True)
        fn = postedData["FirstName"]
        id = postedData["InvoiceId"]
        total = postedData['Total']
        
        #update info
        qr2 = "FirstName == '{}'  and InvoiceId == {}".format(fn,id)
        new_row = tr3_data.query(qr2)
        new_row= new_row.reset_index(drop=True)
        new_row.loc[0,'Total'] = total
        ln = new_row.LastName[0]
        print(ln)
        #delete old row
        qr2 = "FirstName != '{}'  and InvoiceId != {}".format(fn,id)
        tr3_data = pd.DataFrame(tr3_data.query(qr2))


        tr3_data = tr3_data.append({'FirstName':fn,'LastName':ln,'InvoiceId':id,'Total':total},ignore_index=True)


        FirstName = tr3_data.FirstName
        LastName = tr3_data.LastName 
        InvoiceId = tr3_data.InvoiceId
        Total = tr3_data.Total

        return jsonify([
            {
                'FirstName': get_val(FirstName),'LastName':get_val(LastName),'InvoiceId':get_val(InvoiceId),'Total':get_val(Total)

            }
        ])

api.add_resource(Customer, '/customers')
api.add_resource(NewCustomer, '/addcustomer')
api.add_resource(Deletecustomer, '/deletecustomer')
api.add_resource(UpdateCustomerTotal, '/updatecustomertotal')

        



if __name__ =='__main__':
    app.run(host='0.0.0.0')