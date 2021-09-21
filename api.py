'''
Description: This file contains all the methods needed to run the back-end of the Contract Management System

This can also be used as an independent api.
'''

'''
Following are the predefined libraries that are being imported.

'''

'''
Following are the methods or classes that are being imported from other files in the package


'''

import fitz
from flask_cors import CORS
from flask import send_from_directory
from flask import jsonify
from flask import request
from flask import Flask
from werkzeug.utils import secure_filename
from pathlib import Path
import flask
from flask import redirect, url_for, flash, redirect, jsonify, request
import json, os, re, requests
import uuid
import util
from hsn_classifier import HSNClassifier
import numpy as np
import os,sys
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth 
from database import Database
from externaldatabase import ExternalDatabase
import gc


app = Flask(__name__)
CORS(app)

__retrain_inprogress__ = False
__database__ = Database()
__externaldatabase__ = ExternalDatabase()

util.create_dir_if_not_exists(os.path.join(os.getcwd(),"static","Display_Files"))

util.delete_dir_files(os.path.join(os.getcwd(),"static","Display_Files"))

# New Code - 2 API ROUTES ---------------------------------------------------------------------

from check_file_format import check_file
import main_model
import client_history_model
import industry_model


app = flask.Flask(__name__)
app.config['DEBUG'] = False
app.config['UPLOADED_EXCEL'] = 'static'

uploads_dir = os.path.join('static')

errors = {
    "error_1":"Xlsx and xls files accepted only!",
    "error_2":"Format is not satisfied"
}

# New Code - 2 API ROUTES ---------------------------------------------------------------------

def send_response_code(response, response_code=200):
    if not response:
        return '', (response_code if not response_code == 200 else 204)
    if response == True:
        return '', response_code
    return jsonify(response), response_code


@app.route("/", methods=['GET'])
def home():
    return app.send_static_file("./index.html") 

@app.route("/favicon.ico", methods=['GET'])
def favicon():
    return app.send_static_file("./favicon.ico")

@app.route("/query", methods=['GET'])
def getAnswer():
    
    query = request.args.get('query')
    print("query    : ",str(query))



    if query is not None and query!='':

        print('before predict')
        __hsn_classifier__ = HSNClassifier()
        prediction_response = __hsn_classifier__.predict(query,group_id,client, industry)
        # gc.collect()
        
        print('predict done')

        if prediction_response is not None:
            
            return jsonify({
                'HSN':str(prediction_response.hsn),
                'HSN_Confidence':str(prediction_response.hsn_confidence),
                'HSN_8':str(prediction_response.hsn_8),
                'HSN_8_Confidence':str(prediction_response.hsn_8_confidence),
                'HSN_industrial':str(prediction_response.industry_hsn),
                'HSN_industrial_Confidence':str(prediction_response.industry_hsn_confidence),
                'HSN_history_code':str(prediction_response.history_hsn),
                'HSN_history_Confidence':str(prediction_response.history_hsn_confidence),
                'HSN_Main_Top_results':prediction_response.top_records,
                'HSN_Main_Top_confidences':prediction_response.top_confidences,
                'HSN_Main_Top_descritions':prediction_response.top_descriptions,
                'Case_Laws':prediction_response.case_laws,
                'WCO_Opinions':prediction_response.wco_opinions
            })

        else:
            return send_response_code({}, 204)
    else:
        return send_response_code({}, 204)


baseURL='https://eximclassifier.eyasp.in/'

@app.route('/bulkUpload', methods=['POST'])
def bulk_upload():

    # excel_file = 'upload.xlsx'

    # df = pd.read_excel(excel_file)

 try: 

   util.write_log("request url "+ request.url_root)
   
   file = request.files["bulkFile"]
   util.write_log("file received at server "+str(file))
   util.create_dir_if_not_exists("BulkUploadFiles")
   fileLocation = os.path.join("BulkUploadFiles",str(file.filename))
   file.save(fileLocation)
   
   dfInput = pd.read_excel(fileLocation) 
    

   # print("1st query",dfInput.iat[0,1])
   # print("2nd query",dfInput.iat[1,1])
   print("number of queries",len(dfInput))

    # apiURL = baseURL+'classifier/query'
    # PARAMS = {'query' : dfInput.iat[0,1]} 
    # r = requests.get(url = apiURL, params = PARAMS, auth = HTTPBasicAuth('admin', 'login@2020')) 

    # resp = r.json()
   df = pd.DataFrame() 
   #for j in range(0,len(dfInput)):
   for row in dfInput.iterrows():

    # resp = resp_array[j]
    try:

        apiURL = 'http://localhost:7000/query'
        #PARAMS = {'query' : dfInput.iat[j,1]} 
        #PARAMS = {'query' : row[1]['Query']} 
        row_query=row[1]['Query']
        PARAMS = {'query' : row[1]['Query'],'groupId' : row[1]['Grouping']} 
        r = requests.get(url = apiURL, params = PARAMS, auth = HTTPBasicAuth('admin', 'login@2020')) 

        resp = r.json()

        #print('api resp :',json.dumps(resp))


        resultDict = {
            #"ID":row[1]['ID'],
            #"Query":row[1]['Query'],
            # "Client history result 1" : [resp["HSN_Main_Top_results"]["History_Top_1"]] if "HSN_Main_Top_results" in resp and "History_Top_1" in resp["HSN_Main_Top_results"] else [],
            # "Client history confidence 1" : [resp["HSN_Main_Top_confidences"]["History_Top_1"]] if "HSN_Main_Top_confidences" in resp and "History_Top_1" in resp["HSN_Main_Top_results"] else [],
            # "Client history result 2" : [resp["HSN_Main_Top_results"]["History_Top_2"]] if "HSN_Main_Top_results" in resp and "History_Top_2" in resp["HSN_Main_Top_results"] else [],
            # "Client history confidence 2" : [resp["HSN_Main_Top_confidences"]["History_Top_2"]] if "HSN_Main_Top_confidences" in resp and "History_Top_2" in resp["HSN_Main_Top_results"] else [],
            # "Client history result 3" : [resp["HSN_Main_Top_results"]["History_Top_3"]] if "HSN_Main_Top_results" in resp and "History_Top_3" in resp["HSN_Main_Top_results"] else [],
            # "Client history confidence 3" : [resp["HSN_Main_Top_confidences"]["History_Top_3"]] if "HSN_Main_Top_confidences" in resp and "History_Top_3" in resp["HSN_Main_Top_results"] else [],

            "Main Tariff result 1" : [resp["HSN_Main_Top_results"]["Main_Top_1"]],
            "Main Tariff confidence 1" : [resp["HSN_Main_Top_confidences"]["Main_Top_1"]],
            "Main Tariff result 2" : [resp["HSN_Main_Top_results"]["Main_Top_2"]],
            "Main Tariff confidence 2" : [resp["HSN_Main_Top_confidences"]["Main_Top_2"]],
            "Main Tariff result 3" : [resp["HSN_Main_Top_results"]["Main_Top_3"]],
            "Main Tariff confidence 3" : [resp["HSN_Main_Top_confidences"]["Main_Top_3"]],
            
            "Industry result 1" : [resp["HSN_Main_Top_results"]["Industry_Top_1"]],
            "Industry confidence 1" : [resp["HSN_Main_Top_confidences"]["Industry_Top_1"]],
            "Industry result 2" : [resp["HSN_Main_Top_results"]["Industry_Top_2"]],
            "Industry confidence 2" : [resp["HSN_Main_Top_confidences"]["Industry_Top_2"]],
            "Industry result 3" : [resp["HSN_Main_Top_results"]["Industry_Top_3"]],
            "Industry confidence 3" : [resp["HSN_Main_Top_confidences"]["Industry_Top_3"]]
        }

        case_laws = resp["Case_Laws"]

        # for i in range(0,len(case_laws)):
        #     resultDict['Case law name '+str(i+1)] = baseURL + 'classifier/static/caselaws/' + case_laws[i][0] 
        #     print("case law name",case_laws[i][0])

        #     if(len(case_laws[i]) == 3):
        #         resultDict['Case law result '+str(i+1)] = case_laws[i][2]['result']
        #         resultDict['Case law confidence '+str(i+1)] = round(case_laws[i][2]['prob'],2)

        #     else:
        #         resultDict['Case law result '+str(i+1)] = "NA"
        #         resultDict['Case law confidence '+str(i+1)] = "NA"


        
        

        insert_query='''INSERT INTO [dbo].[response_cache]
            ([query]
            ,[top_results]
            ,[top_confidences]
            ,[top_descritions]
            ,[case_laws]) VALUES (?,?,?,?,?)'''

        user_query=row[1]['Query']
        top_results=json.dumps(resp["HSN_Main_Top_results"])
        top_confidences=json.dumps(resp["HSN_Main_Top_confidences"])
        top_descritions=json.dumps(resp["HSN_Main_Top_descritions"])
        case_laws=json.dumps(resp["Case_Laws"])

        unique_id = __database__.insert_row_and_get_id(insert_query,user_query,top_results,top_confidences,top_descritions,case_laws)

        resultDict["URL"]= [baseURL + 'classifier/?unique_id=' + str(unique_id)]
        

        dfTemp = pd.DataFrame(resultDict)    
        print("dataframe temp")
        print(dfTemp)    
        df = pd.concat([df,dfTemp], sort=False)
    except Exception as e:
        util.write_log("An exception occurred, query : "+row_query+", Exception : " +str(e))
        resultDict = {
            "Main Tariff result 1" :[],
            "Main Tariff confidence 1" : [],
            "Main Tariff result 2" : [],
            "Main Tariff confidence 2" : [],
            "Main Tariff result 3" : [],
            "Main Tariff confidence 3" : [],
            
            "Industry result 1" : [],
            "Industry confidence 1" : [],
            "Industry result 2" : [],
            "Industry confidence 2" : [],
            "Industry result 3" : [],
            "Industry confidence 3" : []
        }

        resultDict["URL"]= []
        

        dfTemp = pd.DataFrame(resultDict)  
        df = pd.concat([df,dfTemp], sort=False)

   #df=dfInput
   dfInput = pd.read_excel(fileLocation) 
   dfInput.reset_index(drop=True, inplace=True)
   df.reset_index(drop=True, inplace=True)
   print("dfInput",dfInput.shape)
   print("df",df.shape)
   df = pd.concat([dfInput,df], axis=1)

   print("dataframe final")
   print(df.head())
   outputFileDir=os.path.join("static","OutputFiles")
   util.create_dir_if_not_exists(outputFileDir)
   outputFilePath = os.path.join( outputFileDir,file.filename)
   df.to_excel(outputFilePath, index=False)

   util.write_log("Bulk upload completed")

   return send_response_code({'status':200,'message':'Success','path':outputFilePath})

 except:

   util.write_log("An exception occurred," +str(sys.exc_info()))
   return send_response_code({'status':500,'message':'Internal Server Error','path':''})

@app.route('/getResult', methods=['GET'])
def get_result():

    try:

        unique_id = request.args.get('unique_id')

        if not unique_id:
            return send_response_code({'message':'Unique ID missing in request'},204)
        resultDict=None

        util.write_log("unique_id : ",unique_id)
        rows = __database__.get_rows('select * from response_cache where id=?',unique_id)
        util.write_log("rows : ",rows)
        for row in rows:
            user_query=row[1]
            top_records=json.loads(row[2])
            top_confidences=json.loads(row[3])
            top_descritions=json.loads(row[4])
            case_laws=json.loads(row[5])
            wco_opinions=json.loads(row[6])

            resultDict={
                        'query':user_query,
                        'HSN':str(top_records["Main_Top_1"][0][:4]),
                        'HSN_Confidence':str(top_confidences["Main_Top_1"][0]),
                        'HSN_8':str(top_records["Main_Top_1"][0]),
                        'HSN_8_Confidence':str(top_confidences["Main_Top_1"][0]),
                        'HSN_industrial':str(top_records["Industry_Top_1"][0]),
                        'HSN_industrial_Confidence':str(top_confidences["Industry_Top_1"][0]),
                        'HSN_history_code':str(top_records["History_Top_1"][0]) if "History_Top_1" in top_records else '',
                        'HSN_history_Confidence':str(top_confidences["History_Top_1"][0]) if "History_Top_1" in top_confidences else '',
                        'HSN_Main_Top_results':top_records,
                        'HSN_Main_Top_confidences':top_confidences,
                        'HSN_Main_Top_descritions':top_descritions,
                        'Case_Laws':case_laws
                        ,'WCO_Opinions':wco_opinions
                    }

        print("resultDict",resultDict) 

        return send_response_code(resultDict)   
        
    except:

        util.write_log("An exception occurred," +str(sys.exc_info()))
        return ({'status':500,'message':'Internal Server Error'})    

@app.route('/clientDetail', methods=['GET'])   
def get_client_detail():
    res_list = []
    res = __externaldatabase__.get_rows("select top 2 * from  dbo.registered_entities as reg_ent, dbo.comp_groups as comp_grp where comp_grp.id = reg_ent.comp_group_id;")
    for i in res:
        res_list.append({
        "id": i[0],
        "name": i[1],
        "comp_group_id":i[2],
        "temp_id":i[4],
        "code":i[5],
        "comp_group_name":i[6],
        "pace_id":i[7]
    })
    return send_response_code(res_list)
    
@app.route('/getQuerySearch', methods=['GET'])
def get_query_result():

    try:

        q = request.args.get('q')
        print("q*******************************",q)
        unique_id = ""
        if not unique_id:
            return send_response_code({'message':'Unique ID missing in request'},204)
        resultDict=None

        util.write_log("unique_id : ",unique_id)
        rows = __database__.get_rows('select * from response_cache where id=?',unique_id)
        util.write_log("rows : ",rows)
        for row in rows:
            user_query=row[1]
            top_records=json.loads(row[2])
            top_confidences=json.loads(row[3])
            top_descritions=json.loads(row[4])
            case_laws=json.loads(row[5])
            wco_opinions=json.loads(row[6])

            resultDict={
                        'query':user_query,
                        'HSN':str(top_records["Main_Top_1"][0][:4]),
                        'HSN_Confidence':str(top_confidences["Main_Top_1"][0]),
                        'HSN_8':str(top_records["Main_Top_1"][0]),
                        'HSN_8_Confidence':str(top_confidences["Main_Top_1"][0]),
                        'HSN_industrial':str(top_records["Industry_Top_1"][0]),
                        'HSN_industrial_Confidence':str(top_confidences["Industry_Top_1"][0]),
                        'HSN_history_code':str(top_records["History_Top_1"][0]) if "History_Top_1" in top_records else '',
                        'HSN_history_Confidence':str(top_confidences["History_Top_1"][0]) if "History_Top_1" in top_confidences else '',
                        'HSN_Main_Top_results':top_records,
                        'HSN_Main_Top_confidences':top_confidences,
                        'HSN_Main_Top_descritions':top_descritions,
                        'Case_Laws':case_laws
                        ,'WCO_Opinions':wco_opinions
                    }

        print("resultDict",resultDict) 

        return send_response_code(resultDict)   
        

    except:

        util.write_log("An exception occurred," +str(sys.exc_info()))
        return ({'status':500,'message':'Internal Server Error'}) 

@app.route('/hsn_result', methods=['GET'])
def complete_hsn():
    # try:
        two_hsn_list = []
        three_hsn_list = []
        key = request.args.get('key')
        
        data = __database__.get_rows("select one.hsn_code as one_hsn, one.descript as one_des, two.hsn_code as two_hsn, two.descript as two_des, three.hsn_code as three_hsn, three.descript as three_des from dbo.zeroHSN as one left join dbo.oneHSN as two on two.zero_hsn = one.hsn_code left join dbo.twoHSN as three on three.one_hsn = two.hsn_code where one.hsn_code = '{0}';".format(key))

        one_hsn = {
            "one_hsn_key": data[0][0],
            "description": data[0][1],
            "two_hsn": []
        }

        # print("DATA SQL : ",data)
        
        for i in data:
            if i[2] not in two_hsn_list:
                list_ = []
                two_hsn_list.append(i[2])
                for j in data:
                    if i[2] == j[2]:
                        if j[4] != None and j[5] != None:
                            list_.append({
                                "three_hsn_key": j[4],
                                "description": j[5]
                            })
                one_hsn["two_hsn"].append({
                    "two_hsn_key": i[2],
                    "description": i[3],
                    "three_hsn": list_
                })
        # print("one_hsn SQL : ",one_hsn)

        return send_response_code(one_hsn)
    # except:
    #     return "error"
    
# New Code - 2 API ROUTES ---------------------------------------------------------------------

@app.route('/getresponse', methods=['GET'])
def get_level():
    text = request.args.get('text')
    res = main_model.get_prediction_results(text)
    return json.dumps({'index':res[0], 'values': main_model.get_prod_by_hsn(res[0][0])})


@app.route('/check', methods=['GET'])
def check():
    main_model.create_model()
    return 'hey'

# New Code - 2 API ROUTES ---------------------------------------------------------------------

# -----------------------ERROR HANDLERS --------------------------------------------------------

@app.errorhandler(404) 
def page_not_found(e):
    return "<h1> 404 </h1>", 404


@app.after_request
def add_header(r):
    """
    Add headers to both force latest IE rendering engine or Chrome Frame,
    and also to cache the rendered page for 10 minutes.
    """
    util.write_log(f" {request.method} /{request.endpoint} request in ended")

    r.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    r.headers["Pragma"] = "no-cache"
    r.headers["Expires"] = "0"
    r.headers['Cache-Control'] = 'public, max-age=0'
    
    return r

@app.before_request
def info_before_request():    
	
    util.write_log(f" {request.method} /{request.endpoint} request in progress")
    if __retrain_inprogress__ and not (request.endpoint == 'retrain' and request.method == 'GET'):
        return "<h1> System under maintanence </h1>"


# -----------------------END OF ERROR HANDLERS --------------------------------------------------------


if __name__ == '__main__':
    if app.config["DEBUG"]:
        @app.after_request
        def after_request(response):
            response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate, public, max-age=0"
            response.headers["Expires"] = 0
            response.headers["Pragma"] = "no-cache"
            return response
    # app.run(host="0.0.0.0", debug=False, port=443,
    #         ssl_context=('cert.pem', 'key.pem'))
    app.run(host="0.0.0.0", debug=False, port=7000,threaded=True)