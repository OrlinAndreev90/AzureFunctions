import re
from collections import namedtuple
import numpy as np
import pdfplumber
import pandas as pd
from . import ProcessFileFromBlob_DownloadReport as  helpfuncs
from io import StringIO
import requests
import os 


def transform_adyen_data(report: pd.DataFrame) -> pd.DataFrame:
# Replace NaNs from columns with 0
    for col in report.columns:
        if report[col].dtype == np.float64:
            
            report[col] = np.where( report[col].isnull(), \
                                    0, report[col])
    # Calculate Column TotalFees as per mapping 
    report['TotalFees'] =  np.where(report['Type'].str.contains('Fee'), \
                                    report['Net Debit (NC)'], \
                                    report['Commission (NC)'] + report['Markup (NC)'] + \
                                    report['Scheme Fees (NC)'] + report['Interchange (NC)']  
                                    )
    # Calculate Columns :ChargebackCurrency, ChargebackAmount,ChargebackCaseNumber as per mapping
    report[['ChargebackCurrency','ChargebackAmount','ChargebackCaseNumber']] = \
        report[report['Type'].isin(['Chargeback', 'SecondChargeback'])] \
            [ ['Gross Currency','Gross Credit (GC)','Psp Reference'] ]
  
    # Calculate Column ChargebackFeeCurrency ,ChargebackAmount as per mapping 
    report['ChargebackFeeCurrency'],report['ChargebackFeeAmount'] = None,0

    report['PaymentGateway'] = 'Adyen'
    # Drop Irrelevant Columns
    report.drop(['Net Credit (NC)','Gross Debit (GC)','Net Debit (NC)'] \
        , axis=1, inplace=True)

    return report