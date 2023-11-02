import requests
import pyodbc
from datetime import datetime,timedelta
dummy={
        "AppTransactionID": "2022091211380889",
        "ApplicationName": "BuyOnLine",
        "CustomerID": "60211388900",
        "CustomerName": "Suraj Kumar",
        "EmailAddress": "Suraj@gmail.com",
        "Phonenumber": "7903482061",
        "PaymentTypeCode": "02",
        "PaymentType": "NB",
        "RecordtypeCode": "01",
        "Recordtype": "ApplicationNumber",
        "PolicyApplicationNo": "60211388900",
        "TrxnAmount": "100",
        "MandateStartDate": "",
        "MandateEndDate": "",
        "Frequency": "",
        "MaxMandateAmount": "",
        "TrxnType": "Payment",
        "ProductType" : "Group"
    }
URL=r"https://securegateway.sudlife.in/GenerateURL/api/PaymentURL"
def dbconnect():
    

    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                            'Server=10.1.40.55,53205;'
                            'Database=DIGISALES;'
                            'UID=DIGISALES;'
                            'PWD=sud@1234;'
                            )
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM GRP_PGLINK_FROM_DATE")
    xt=cursor.fetchall()
    # print(xt)
    fromDate=(str(xt).replace("[(\'","")).replace("\', )]","")
    # fromDate=(str(xt).replace("[(\'","")).replace("\', )]","")
    xt=datetime.strptime(fromDate,"%Y-%m-%d %H:%M:%S")
    fromDate=str(xt + timedelta(minutes=-3))
    # print(fromDate)
    now = datetime.now() 
    toDate=str(now.strftime("%Y-%m-%d %H:%M:%S"))
    # now = datetime.now() 
    # toDate=str(now.strftime("%m-%d-%Y %H:%M:%S"))
    print(toDate)
    file1.write(toDate+"\n")
    print("Data Extraction from DQ in progress...")
    Query=f"exec PGLINKDATA \'1\',\'{fromDate}\',\'{toDate}\'"     #1 for grp & 2 for GRP
    print(Query) 
    cursor.execute(Query)
    x = cursor.fetchall()
    # print(x)
    if(len(x)>0):
        try:
            API_Call(x,URL)
            updateQuery=f"UPDATE GRP_PGLINK_FROM_DATE SET fromDate = '{str(toDate)}'"
            print(updateQuery)
            cursor.execute(updateQuery)
            cursor.commit()
            print("Datetime updated successfully.")
            cursor.close()
            conn.close
        except Exception as e:
            # print(str(e)+"Error Occured!!!")
            print("Error Occured!!!     "+str(e))
            # print("Error Occured!!!     "+str(e))
            file1.write("Error Occured!!!"+str(e)+"\n")
    else:
        print("****No Data found in th DQ****")
        file1.write("****No Data found in th DQ****"+"\n")
        updateQuery=f"UPDATE GRP_PGLINK_FROM_DATE SET fromDate = '{str(toDate)}'"
        cursor.execute(updateQuery)
        cursor.commit()
        print("Datetime updated successfully.")
        cursor.close()
        conn.close
    

def checksum(AppTransactionID,CustomerID,CustomerName,):
    import hashlib
    f=str(AppTransactionID)+'|'+'DQ'+'|'+str(CustomerID)+'|'+str(CustomerName)+'|'+'01'+'|'+'NB'+'|'+'02'+'|'+'PolicyNo'+'|'+str(CustomerID)
    result = hashlib.md5(f.encode()).hexdigest()
    print(hashlib.md5(f.encode()).hexdigest())
    return result

def InsertData(AppTransactionID,ApplicationName,CustomerID,CustomerName,PaymentTypeCode,PaymentType,RecordtypeCode,Recordtype,PolicyApplicationNo,Amount,Remarks,PaymentURL,TransactionDate,EmailAddress,Phonenumber,CheckSum,AdditionalField1,AdditionalField2,AdditionalField3,AdditionalField4,AdditionalField5,AdditionalField6,AdditionalField7,AdditionalField8):
    TransactionDate=TransactionDate.replace("datetime.date","")

    Insert_Query=f"if not exists (select PolicyApplicationNo from  VFirstDB..TR_Payment_URL where PolicyApplicationNo='"+PolicyApplicationNo+"') begin insert into VFirstDB..TR_Payment_URL(AppTransactionID,ApplicationName,CustomerID,CustomerName,PaymentTypeCode,PaymentType,RecordtypeCode,Recordtype,PolicyApplicationNo,Amount,Remarks,ReturnURL,TransactionDate,EmailAddress,Phonenumber,CheckSum,AdditionalField1,AdditionalField2,AdditionalField3,AdditionalField4,AdditionalField5,AdditionalField6,AdditionalField7,AdditionalField8) values(\'"+AppTransactionID+"\',\'"+ApplicationName+"\',\'"+CustomerID+"\',\'"+CustomerName+"\',\'"+PaymentTypeCode+"\',\'"+PaymentType+"\',\'"+RecordtypeCode+"\',\'"+Recordtype+"\',\'"+PolicyApplicationNo+"\',\'"+Amount+"\',\'"+Remarks+"\',\'"+PaymentURL+"\',\'"+TransactionDate+"\',\'"+EmailAddress+"\',\'"+Phonenumber+"\',\'"+CheckSum+"\',\'"+AdditionalField1+"\',\'"+AdditionalField2+"\',\'"+AdditionalField3+"\',\'"+AdditionalField4+"\',\'"+AdditionalField5+"\',\'"+AdditionalField6+"\',\'"+AdditionalField7+"\',\'"+AdditionalField8+"\') end"
    print("INSERT QUERY: "+Insert_Query)
    file1.write("INSERT QUERY: "+Insert_Query+"\n")
    conn2 = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                            'Server=10.1.6.123,1433;'
                            'Database=VFirstDB;'
                            'UID=recon;'
                            'PWD=recon@123;'
                            )
    # print(conn2)
    cursor2 = conn2.cursor()
    cursor2.execute(Insert_Query)
    cursor2.commit()
    cursor2.close()
    print("Insertion Completed\n")
    file1.write("Insertion Completed\n")
    return 
    
#('2023060613543714321209', 'DQ', '14321209', 'Amarnath .', 'abhishek.gautam7068@outlook.com', '7355956733', '01', 'NB', '02', 'PolicyNo', '14321209', 100000.0, '', '', 'Annual', '100000', 'Payment')
def API_Call(x,URL):
    count=0
    for i in x:
        # try:
            li=[]
            count+=1
            print(str(count)+". "+str(i))
            j=(str(i).replace("(","")).replace(")","")
            li=j.split(",")
            AppTransactionId=li[0].replace("\'","")
            ApplicationName=li[1].replace("\'","")
            CustomerId=li[2].replace("\'","")
            CustomerName=li[3].replace("\'","")[1:]
            # CustomerName='Mayur Budhbhatti'
            EmailAddress=li[4].replace("\'","")
            PhoneNumber=li[5].replace("\'","")
            # PhoneNumber='7208788640'
            PaymentTypeCode=li[6].replace("\'","")
            PaymentType=li[7].replace("\'","")
            RecordTypeCode=li[8].replace("\'","")
            RecordType=li[9].replace("\'","")
            PolicyApplicationNo=li[10].replace("\'","")
            TrxnAmount=li[11].replace("\'","")
            # TrxnAmount='1'
            MandateStartDate=li[12].replace("\'","")
            mandateEndDate=li[13].replace("\'","")
            Frequency=""
            MaxMandateAmount=""
            TrxnType= li[16].replace("\'","")
            # TransactionDate=li[17].replace("\'","")
            year=(li[17].replace("datetime.date(","")).replace(" ","")
            month=li[18].replace(" ","")
            if(int(month)<10):
                monthNo="0"+month
                month=monthNo
            day=(li[19].replace(")","")).replace(" ","")
            if(int(day)<10):
                dayNo="0"+day
                day=dayNo
            TransactionDate=year+"-"+month+"-"+day
            Remarks=""
            AdditionalField1=""
            AdditionalField2=""
            AdditionalField3=""
            AdditionalField4=""
            AdditionalField5=""
            AdditionalField6=""
            AdditionalField7=""
            AdditionalField8=(li[20].replace("\'",""))[1:]
            CheckSum=checksum(AppTransactionId,CustomerId,CustomerName)


            dummy["AppTransactionID"]=AppTransactionId
            dummy["ApplicationName"]=ApplicationName
            dummy["CustomerID"]= CustomerId
            dummy["CustomerName"]=CustomerName
            dummy["EmailAddress"]=EmailAddress
            dummy["Phonenumber"]=PhoneNumber
            dummy["PaymentTypeCode"]=PaymentTypeCode
            dummy["PaymentType"]=PaymentType
            dummy["RecordtypeCode"]=RecordTypeCode
            dummy["Recordtype"]=RecordType
            dummy["PolicyApplicationNo"]=PolicyApplicationNo
            dummy["TrxnAmount"]=TrxnAmount
            dummy["MandateStartDate"]=MandateStartDate
            dummy["MandateEndDate"]=mandateEndDate
            dummy["Frequency"]=Frequency
            dummy["MaxMandateAmount"]=MaxMandateAmount
            dummy["TrxnType"]=TrxnType

            dummy["AppTransactionID"]=dummy['AppTransactionID'].replace(" ","")
            dummy["ApplicationName"]=dummy['ApplicationName'].replace(" ","")
            dummy["CustomerID"]=dummy["CustomerID"].replace(" ","")
            dummy["CustomerName"]=(dummy["CustomerName"])
            dummy["EmailAddress"]=dummy["EmailAddress"].replace(" ","")
            dummy["Phonenumber"]=dummy["Phonenumber"].replace(" ","")
            dummy["PaymentTypeCode"]=dummy["PaymentTypeCode"].replace(" ","")
            dummy["PaymentType"]=dummy["PaymentType"].replace(" ","")
            dummy["RecordtypeCode"]=dummy["RecordtypeCode"].replace(" ","")
            dummy["Recordtype"]=dummy["Recordtype"].replace(" ","")
            dummy["PolicyApplicationNo"]=dummy["PolicyApplicationNo"].replace(" ","")
            dummy["TrxnAmount"]=dummy["TrxnAmount"].replace(" ","")
            dummy["MandateStartDate"]=dummy["MandateStartDate"].replace(" ","")
            dummy["MandateEndDate"]=dummy["MandateEndDate"].replace(" ","")
            dummy["Frequency"]=dummy["Frequency"].replace(" ","")
            dummy["MaxMandateAmount"]=dummy["MaxMandateAmount"].replace(" ","")
            dummy["TrxnType"]=dummy["TrxnType"].replace(" ","")
            print("REQUEST:"+str(dummy))
            file1.write("REQUEST:"+str(dummy)+"\n")
            r=requests.post(URL,data=dummy)
            file1.write(str(i)+"\n")
            response=r.json()
            print("RESPONSE: "+str(response))
            file1.write("RESPONSE: "+str(response)+"\n")
            # file1.write(str(response)+"\n")
            # Status=response['Status']
            # ErrCode=response['Errcode']
            PaymentURL=response['PaymentURL']
            InsertData(str(AppTransactionId).replace(" ",""),str(ApplicationName).replace(" ",""),str (CustomerId).replace(" ",""),str (CustomerName),str (PaymentTypeCode).replace(" ",""),str (PaymentType).replace(" ",""),str (RecordTypeCode).replace(" ",""),str (RecordType).replace(" ",""),str (PolicyApplicationNo).replace(" ",""),str (TrxnAmount).replace(" ",""),str (Remarks).replace(" ",""),str (PaymentURL).replace(" ",""),str (TransactionDate).replace(" ",""),str (EmailAddress).replace(" ",""),str(PhoneNumber).replace(" ",""),str (CheckSum).replace(" ",""),str (AdditionalField1),str (AdditionalField2),str (AdditionalField3),str (AdditionalField4),str (AdditionalField5),str (AdditionalField6),str (AdditionalField7),str (AdditionalField8))
            # break
        # except Exception as e:
        #     # print(str(e)+"Error Occured!!!")
        #     print("Error Occured!!!     "+str(e))
        #     # print("Error Occured!!!     "+str(e))
        #     file1.write("Error Occured!!!"+str(e)+"\n")
        #     continue


import os.path
import time
try:
    now = datetime.now() 
    logdate=str(now.strftime("%Y%m%d_%H%M%S"))
    save_path = 'D:\\PGLinkGenerator\\GRP_Logs'
    if not os.path.exists(save_path):
            os.mkdir(save_path)
    completeName = save_path+"\\"+'GRP_log_'+logdate+'.txt'
    file1 = open(completeName, "a")
    dbconnect()
    file1.close()
except Exception as e:
    print(e)
time.sleep(5)
# if(len(x)>0):
#     API_Call(x,URL)
# else:
#     print("****No Data found in th DQ****")
#     # file1.write("****No Data found in th DQ****"+"\n")