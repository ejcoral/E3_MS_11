#!/usr/bin/python
# -*- coding: utf-8 -*-
from flask import Flask, request
import pika 
import MySQLdb
import json
import cgi
import boto3
import os

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)


app = Flask(__name__)
@app.route('/microservicio/consultaproductoscatalogo/<catalogo>', methods=['GET'])
def consulta_producto_catalogo(catalogo):

    if request.method == "GET":
            
        try:
            db = MySQLdb.connect(host=os.environ.get('db_host'), user=os.environ.get('db_user'), passwd=os.environ.get('db_pass'),  port=3306, db=os.environ.get('db_name'), charset='utf8',use_unicode=True)        
            cur = db.cursor()
            query = ("SELECT * FROM productoxcatalogo WHERE id_catalogo = %s")
            cur.execute(query, [catalogo])
            rows = cur.fetchall()
            items =[]


            columns = [desc[0] for desc in cur.description]
            result = []
            for row in rows:
                row = dict(zip(columns, row))
                result.append(row)    

            items = json.dumps({'producto':result}, indent=4, sort_keys=True, cls=DecimalEncoder)    

            db.close()
            
            return items
   
        except:
            return  json.dumps({'msg': "No hay productos asociados al catalogo"})

if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=True, port=5010)
