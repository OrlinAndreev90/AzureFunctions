import pandas as pd

## Transformations performed on DHL Easy Retun. Expects a pandas dataframe as Input

def DHLEasyReturnTransformations(data:pd.DataFrame) -> pd.DataFrame:
    InvoiceNumber = data[data[0]=='Billing Document'].iloc[0,1]  
    InvoiceDate = data[data[0]=='Billing Date'].iloc[0,1]    
    ColumnNames = data.iloc[8,]
    OutputData =  data.iloc[9:,].dropna(axis=0,how='all')
    OutputData.columns = ColumnNames
   # Attach new Columns 
    OutputData['InvoiceDate'],OutputData['CarrierInvoiceNumber'],OutputData['CarrierShortName'] = [InvoiceDate,InvoiceNumber,'DHL']
    
    OutputData['Parcel Identifier'] = OutputData['Parcel Identifier'].str.lstrip('\'')
    OutputData[['Gross weight','Billed qty','Net value','Toll']]= OutputData[['Gross weight','Billed qty','Net value','Toll']].replace(',','.',regex=True)
    
    OutputData.reset_index(inplace= True, drop= True)
    return OutputData


