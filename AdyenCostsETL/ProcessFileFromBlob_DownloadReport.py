import  azure.storage.blob as bl
import os
import pandas as pd
from io import StringIO,BytesIO
from datetime import datetime, timedelta

import re
import requests
### Functions to upload  to a  Storage Account , Download the Settlement Details Report from Adyen API

def download_report(reporturl: str) -> pd.DataFrame:
    """ Function to download a Settlement Details report using Adyen`s API 
        and a pre-specified user and parse it in a pandas data frame.

        Input: reporturl: url of the report that will be downloaded

        Assumes "," as the column delimiter and a header row

    """
    report_user = os.environ["AdyenReportUser"]
    pw = os.environ["AdyenReportUserPw"] 
    
    

    api_request= requests.get(reporturl,auth=(report_user,pw)) 

    if api_request.status_code != 200:
        raise BaseException ( "".join(' API Request Error. Status code: {0}.Reason : {1}') \
                                .format(api_request.status_code,api_request.reason))
    data = StringIO(api_request.text)

    ## Take subset of columns
    column_names = [ 'Psp Reference',
        'Payment Method', 'Creation Date','Type',  \
        'Modification Reference', 'Gross Currency', 'Gross Debit (GC)',
       'Gross Credit (GC)', 'Exchange Rate', 'Net Currency', 'Net Debit (NC)',
       'Net Credit (NC)', 'Commission (NC)', 'Markup (NC)', 'Scheme Fees (NC)',
       'Interchange (NC)']
    # make sure Psp Reference is set to string 
    report_data = pd.read_csv(data, sep= ",",usecols= column_names, \
        error_bad_lines= False, dtype={'Psp Reference': str})

    return report_data




## Helper Function to connect to a Blob Sotrage Account :invoicesfilestore
    # Returns A BlobServiceClient
def ConnectToStorageAccount()-> bl.BlobServiceClient:
    """ Function to connect to a blob servie client. using a SAS token.
    Takes Storage account name and account key from the already stored environment variables.
    in function.json(azure func settings) , local.settings.json(local testsing env)
    Output: azure.storage.blob.BlobServiceCluent

    """
    #Generate SAS Token
    sas_token = bl.generate_account_sas(account_name= os.environ["account_name"],\
        account_key= os.environ["account_key"],
        resource_types= bl.ResourceTypes(object= True),\
        permission= bl.AccountSasPermissions(read= True, write= True),\
        expiry= datetime.utcnow() + timedelta(minutes= 10))
    

    blob_service_client= bl.BlobServiceClient(account_url= os.environ["Blob_Url"],  \
        credential=sas_token)
    
    return blob_service_client


    

def UploadFileToBlob(data:pd.DataFrame, BlobName:str) -> dict:  

    """   Function to upload  a file as a csv  ,
    stored in storage аccount : invoicesfilestore , 
    
    Inputs:
    BlobName: (string) Path to the blob . From it the container is extracted
    Outputs:
    python dict object containing : The name of the blob and the container
    """

    # Format Data as csv 
    UploadData= data.to_csv(index= False)

    blob_service_client= ConnectToStorageAccount()
    container_name= BlobName[:str.find(BlobName, '/')]
    blob_name= BlobName[str.find(BlobName, '/') + 1:]

 # How the new File Will be named . CHange extension to .txt so as to not trigger the data factory again
    blob_name = re.sub(pattern= r"(\.\w{0,4})",repl='_forUpload.txt',string= blob_name)
    
    blob = blob_service_client.get_blob_client(container= container_name, blob= blob_name)
    blob.upload_blob(UploadData)

    return {'blob_name': blob_name, 'container': container_name}
