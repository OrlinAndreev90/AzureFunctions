import logging, traceback

import azure.functions as func
from . import ProcessFileFromBlob as  helpfuncs  # Functions to handle Blob Storage processes
from . import DHLEasyReutnTransformations as  transform
import json
import os

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
   
    FileName = req.params.get('filename')
    
    if not FileName:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            FileName = req_body.get('filename')

    if FileName:
        try:
       # Read raw data from blob in a pandas Data Frame
            InvoiceRawData= helpfuncs.ReadCsvFromBlob(invoicefile= FileName, \
                delimiter= '\t', unstructured= True)
       
       # Process Data 
            ProcessedData = transform.DHLEasyReturnTransformations(data= InvoiceRawData)
       # Upload the Processed Data to blob and return blob name
            blob_data = helpfuncs.UploadFileToBlob(data= ProcessedData, BlobName= FileName)
            Text= "File %s was copied to container %s." %(blob_data['blob_name'], blob_data['container'])
            Text = json.dumps({"body": {'File':blob_data['blob_name'],\
                "Supplier" : ProcessedData.loc[0,'CarrierShortName'],\
                "Invoice": ProcessedData.loc[0,'CarrierInvoiceNumber'],\
                 "Message":  Text }})
            
            status = 200
        except: 
            ErrorTrace = traceback.format_exc()
            Text= "Error when processing file %s \n Error Message : %s.\n File was not copied."\
             %(FileName,ErrorTrace)
            status = 400
        return func.HttpResponse(Text,status_code=status)
    
    
    
    
    else:
        return func.HttpResponse(
             "Please pass a filename on the query string or in the request body",
             status_code=400
        )



