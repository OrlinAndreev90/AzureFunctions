import re
from collections import namedtuple
import numpy as np
import pdfplumber
import pandas as pd
from . import ProcessFileFromBlob as  helpfuncs
from io import BytesIO

""" Contains Functions to process and extract necessary data from Canada post PDF invoice"""





def ExtractFields(Text: str) -> namedtuple:
    """Function to extract  """

    InvoiceRecord = namedtuple('InvoiceRecord',
                               ['OrderDate', 'ServiceType', 'ReferenceNumber', 'BilledDims', 'FuelSurcharge', \
                                'FuelSurchargeAmount', 'Weight', 'HST_ON', 'Oversize','Subtotal'])
    # Extract Reference Number
    try:
        ReferenceNumber = re.findall(r'(\d{16} \d{1,6}\.{0,1}\d{0,6}x)', Text)[0].split(' ')[0]

    except:
        ReferenceNumber = None
    # Extract Order Date
    try:
        OrderDate = re.findall(r'(\d{4}-\d{2}-\d{2}) ', Text)[0]
    except:
        OrderDate = None

    # Extract BilledDims
    try:
        BilledDims = re.findall(r'(\d{1,6}\.{0,1}\d{0,6}x\d{1,6}\.{0,1}\d{0,6}x\d{1,6}\.{0,1}\d{0,6}) ', Text)[1]
    except:
        BilledDims = None
    # Extract FuelSurcharge Data
    try:
        FuelSurchargeData = re.findall(r' (Fuel Surcharge \d{0,4}\.{0,1}\d{0,4}\% \d{0,4}.{0,1}\d{0,4})', Text)

        FuelSurcharge = re.findall(r'(Fuel Surcharge \d{0,4}\.{0,1}\d{0,4}\%)', FuelSurchargeData[0])[0]

        ## The Amount follows '% '
        FuelSurchargeAmount = re.findall(r'(?<=% )(\d{1,4}\.{0,1}\d{0,4})', FuelSurchargeData[0])[0]

    except:
        FuelSurcharge = None
        FuelSurchargeAmount = None

    # Extract Weight
    try:
        Weight = re.findall(r'(\d{1,4}\.{0,1}\d{0,4})(?= Fuel Surcharge)', Text)[0]
    except:
        Weight = None
    # Extract  Oversize
    try:
        
       
        Oversize = re.findall(r'(?<=Oversize )(\w{0,15}\W{0,1} \d{1,4}\.{0,1}\d{0,4})', Text)[0]
    
    except:
        Oversize = None
    # Extract HST_ON
    try:
        HST_ON = re.findall(r'(?<=HST \(ON\) )(\d{1,4}.{0,1}\d{0,4})', Text)[0]
    except:
        HST_ON = None

    try:
        CorrectedText = re.sub(r'(\d{1,6}\.{0,1}\d{0,6}x\d{1,6}\.{0,1}\d{0,6}x\d{1,6}\.{0,1}\d{0,6})', 'SUB', Text)
        ## Preceeded: Extract pattern : 4 word preceeded by "SUB " and followed by " digit"
        ServiceType = re.findall(r'(?<=SUB )(\w{1,10} \w{0,10} \w{0,10} \w{0,10}) (?=\d)', CorrectedText)[0]

    except:
        ServiceType = None

    try:
        Subtotal = re.findall(r'(?<=Subtotal )(\d{1,6}\.{0,1}\d{0,6})',Text)[0]
    except:
        Subtotal = None



    return InvoiceRecord(OrderDate= OrderDate, ServiceType= ServiceType,
                         ReferenceNumber= ReferenceNumber, BilledDims= BilledDims, FuelSurcharge= FuelSurcharge, \
                         FuelSurchargeAmount= FuelSurchargeAmount, Weight= Weight, HST_ON= HST_ON, Oversize= Oversize, \
                         Subtotal= Subtotal)

## Vectorize the function 
ExtractFieldsVectorized = np.vectorize(ExtractFields, otypes=[tuple], cache=False)



def ExtractDataFromInvoice(file: BytesIO)  -> pd.DataFrame:
    """ Function to parse the Canada post pdf invoice and extract fields :
    ['InvoiceDate','OrderDate','Reference','InvoiceNumber',
    'BilledDims', 'Weight','ServiceDescription','FuelSurcharge',
    'FuelSurchargeAmount','Subtotal', 'HST_ON', 'Oversize'
    ]
    
    
    Input : 
    file (BytesIO object)=  a BytesIO object containing the data 
    Output:
     pandas DataFrame containing the Extracted Fields
    """

    InvoiceData = []
    with pdfplumber.open(file) as pdf:
        pages = pdf.pages

        FirstPage = pages[0].extract_text()
        # Extract invoice Date and Invoice number.
        InvoiceDate = FirstPage[FirstPage.find('Invoice date'):].split('\n')[0].split(' ')[-1]
        InvoiceNumber = FirstPage[FirstPage.find('Invoice number'):].split('\n')[0].split(' ')[-1]
        PagesList= iter(pages)
       #Skip first  and second page .
        next(PagesList)
        next(PagesList)
        # Loop across  rest of Pages
        for page in PagesList:
            InvoiceData.append(page.extract_text())
        # Combine all pages in a list : first concatenate all pages in 1 big string and add a Record Delimiter "|END|"
        # . Then split each record in 1 element of a list

        InvoiceDataCombined = re.sub( r'(Total \$\d{1,4}\.{0,1}\d{0,4})' ,"|END|" ,"".join(InvoiceData)).split("|END|")

        ## named Tuple that will be used to store results

        # Extract Data
        InvoiceData_df = pd.DataFrame(ExtractFieldsVectorized(InvoiceDataCombined).tolist())



             ## Create the FinalOrderDate column
        InvoiceData_df['OrderDateGroup']=  \
            InvoiceData_df.join(pd.concat([
                InvoiceData_df['OrderDate'].map(lambda x: 1 if x != None else 0).cumsum().rename("OrderDateGroup"),
                InvoiceData_df['OrderDate']
                                    ], axis=1, sort=False
                                       ),
                                       how='left', rsuffix='_r')['OrderDateGroup']

        InvoiceData_df['InvoiceDate'],InvoiceData_df['InvoiceNumber'] =InvoiceDate,InvoiceNumber

        InvoiceData_df['OrderDateFinal']= \
            InvoiceData_df.join( InvoiceData_df[['OrderDate','OrderDateGroup']].groupby('OrderDateGroup').first(),\
                                      how='left',on ='OrderDateGroup',rsuffix='_New'
                                      )['OrderDate_New']




        ## Drop redundant columns. Rename and re-arrange columns
        InvoiceData_Final= InvoiceData_df.drop([ 'OrderDate', 'OrderDateGroup' ], axis=1) \
                    .rename(columns={"OrderDateFinal":"OrderDate", "ReferenceNumber": "Reference", \
                                     "ServiceType": "ServiceDescription" })\
                    .reindex(columns=['InvoiceDate','OrderDate','Reference','InvoiceNumber','BilledDims', \
                                      'Weight','ServiceDescription','FuelSurcharge', 'FuelSurchargeAmount','Subtotal', \
                                      'HST_ON', 'Oversize'])


        ## Drop Redundant Records
        InvoiceData_Final.dropna(axis=0, subset=['ServiceDescription', 'Reference'], inplace=True)


        return InvoiceData_Final