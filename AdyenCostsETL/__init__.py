import logging, traceback

import azure.functions as func
from . import ProcessFileFromBlob_DownloadReport as  helpfuncs  # Functions to handle Blob Storage processes
from . import AdyenTransforms as transform
import json
import os
from io import BytesIO

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
   
    report_url = req.params.get('report_url')
    
    if not report_url:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            report_url = req_body.get('report_url')

    if report_url:
        try:
      
       # Read raw data from blob in a BytesIO Object
            InvoiceRawData= helpfuncs.download_report(reporturl = report_url)

       # Process Data 
            ProcessedData = transform.transform_adyen_data(report= InvoiceRawData ) 

       # Extract ReportName from reporturl and compose final blob Path where file will be uploaded
            report_name = report_url[report_url.rfind("/") + 1: ]
            
            container_path = "invoices/PaymentGateways/Adyen/"
            
            blob_name = "".join([container_path,report_name])

       # Upload the Processed Data to blob and return blob name
            blob_data = helpfuncs.UploadFileToBlob(data= ProcessedData, BlobName= blob_name)
            Text= "File %s was copied to container %s." %(blob_data['blob_name'], blob_data['container'])
            
 
            Text = json.dumps({"body": {'File':blob_data['blob_name'],\
                "Supplier" : ProcessedData.loc[0,'PaymentGateway'],\
                "Invoice": report_name,\
                 "Message":  Text }})


            status = 200
        except: 
            ErrorTrace = traceback.format_exc()
            Text= "Error when processing file %s \n Error Message : %s.\n File was not copied."\
             %(report_url,ErrorTrace)
            status = 400
        return func.HttpResponse(Text,status_code=status)
    
    
    else:
        return func.HttpResponse(
             "Please pass a report_url on the query string or in the request body",
             status_code=400
        )
