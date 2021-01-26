import  azure.storage.blob as bl
import os
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta
import pdfplumber
import re
### Functions to read CSV ,Excel and PDF files from a  Storage Account



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



def ReadCsvFromBlob(invoicefile: str, delimiter:str ,unstructured: bool) -> pd.DataFrame:
    """   Function to read a delimited file ( as is , i.e. no transformations are applied) file ,
    stored in 
    storage аccount : invoicesfilestore , 
    Container:Invoices
    Inputs:
    invoicefile: (string) Path to the blob
    delimiter: (string) Delimiter of the file
    unstructured:  (boolean) if yes . It assumes that there are some rows in the begining of the file 
    which have a different structure, but necessary data in them. Applies 

    Outputs:
    pandas Dataframe object containing the data
    """
    #Connect to the Blob Storage Account
    blob_service_client = ConnectToStorageAccount()

    #Extract COntainerName and Blob Name from InvoiceFilePath
    container_name= invoicefile[: str.find(invoicefile,'/')]
    blob_name= invoicefile[str.find(invoicefile, '/') +1:]
    
    #Read Csv File directly from Blob
    FileData= blob_service_client.get_container_client(container_name)\
        .get_blob_client(blob_name).download_blob().readall()
    # Convert to String and Read Directly in - memory
    FileData = str(FileData, 'utf-8')

    data = StringIO(FileData) 
    #If file is unstructured : Extract additional data 
    if unstructured:
       HeadersData= pd.read_csv(data, sep= delimiter, header= None, error_bad_lines= False,\
            warn_bad_lines= False)
    #Reopen Stream
       data = StringIO(FileData)
       InvoiceData = pd.read_csv(data,sep= delimiter,header= None,error_bad_lines= False,\
                                  skiprows= len(HeadersData.index) )
       data = pd.concat([HeadersData, InvoiceData])

    else:
        data = pd.read_csv(data, sep= delimiter, header= None)

    return data
    

def ReadExcelFromBlob(invoicefile:str) -> pd.DataFrame:

    """   Function to read a excel file ( as is , i.e. no transformations are applied) file ,
        stored in 
        storage аccount : invoicesfilestore , 
        Container:Invoices
        Inputs:
        invoicefile: (string) Path to the blob
        Outputs:
        pandas Dataframe object containing the data
        """

    #Connect to the Blob Storage Account
    blob_service_client = ConnectToStorageAccount()
    # Extract container and blob names 
    container_name= invoicefile[:str.find(invoicefile, '/')]
    blob_name= invoicefile[str.find(invoicefile, '/') +1:]

    # Create Blob URL
    blob_url_with_sas= blob_service_client.get_container_client(container_name) \
        .get_blob_client(blob_name).url
    data= pd.read_excel(io= blob_url_with_sas, sheets= 0, header= None)
    return data


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
