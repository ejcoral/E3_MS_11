#!/usr/bin/python
# -*- coding: utf-8 -*-
from flask import Flask, request
import pika 
import MySQLdb
import json
import cgi
import boto3
import os
from botocore.exceptions import ClientError

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
            dynamodb = boto3.client('dynamodb',aws_access_key_id=os.environ.get('aws_access_key_id'), aws_secret_access_key=os.environ.get('aws_access_key_secret'), region_name=os.environ.get('region'))

            
            cur = db.cursor()
            query = ("SELECT * FROM productoxcatalogo WHERE id_catalogo = %s")
            cur.execute(query, [catalogo])
            rows = cur.fetchall()
            items =[]


            columns = [desc[0] for desc in cur.description]
            result = []
            prod_desc = []
            for row in rows:
                row = dict(zip(columns, row))
                result.append(row)    

                response = dynamodb.get_item(
                    TableName= "Product_Read",
                    Key={
                        'id_producto': {'N': str(row['id_producto'])},
                        
                    }
                )
                prod_desc.append(response['Item']) 

            items.append({'producto':result})
            items.append({'producto_desc': prod_desc})  

            db.close()

            
            return json.dumps(items)
        except ClientError as e:
            return (e.response['Error']['Message'])
        #except:
        #    return  json.dumps({'msg': "No hay productos asociados al catalogo"})

if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=True, port=5010)
