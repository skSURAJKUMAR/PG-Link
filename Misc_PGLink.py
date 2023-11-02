import requests
import pyodbc
from datetime import datetime
import os.path
import time
# import openpyxl
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
        "TrxnType": "Payment"
    }
URL=r"https://securegateway.sudlife.in/GenerateURL/api/PaymentURL"

def dbconnect_grp(appNo):
    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                            'Server=10.1.40.55,53205;'
                            'Database=DIGISALES;'
                            'UID=DIGISALES;'
                            'PWD=sud@1234;'
                            )
    cursor = conn.cursor()
    print("Data Extraction from DQ in progress...")

    Query=f"""select

            FORMAT ( GETDATE(), 'yyyyMMddHHmmss')+prod.PROPOSAL_NUMBER as AppTransactionID,
            'DQ' as ApplicationName,
            prod.proposal_number as CustomerID,

            case prod.proposer_type when 'LA_PROPOSER_PAYOR_SAME' then prod.INS_FULL_NAME +' '+prod.ins_lastname 
            when 'PROPOSER_PAYER_SAME' then prod.PRO_FULL_NAME +' '+prod.pro_lastname  when 'LA_PROPOSER_PAYOR_DIFF' then json_value(prod.PAY_PERSONNEL_DETAILS,'$.pay_firstname')+' '+json_value(prod.PAY_PERSONNEL_DETAILS,'$.pay_lastname')  else json_value(prod.PAY_PERSONNEL_DETAILS,'$.pay_firstname')+' '+json_value(prod.PAY_PERSONNEL_DETAILS,'$.pay_lastname') end as  CustomerName,

            iif((case prod.proposer_type when 'LA_PROPOSER_PAYOR_SAME' then prod.ins_email when 'PROPOSER_PAYER_SAME' then prod.pro_email when 'LA_PROPOSER_PAYOR_DIFF' then json_value(prod.PAY_PERSONNEL_DETAILS,'$.pay_email') else json_value(prod.PAY_PERSONNEL_DETAILS,'$.pay_email') end )!='',
            (case prod.proposer_type when 'LA_PROPOSER_PAYOR_SAME' then prod.ins_email when 'PROPOSER_PAYER_SAME' then prod.pro_email when 'LA_PROPOSER_PAYOR_DIFF' then json_value(prod.PAY_PERSONNEL_DETAILS,'$.pay_email') else json_value(prod.PAY_PERSONNEL_DETAILS,'$.pay_email') end ),'test@sudlife.in') as  EmailAddress ,


            case prod.proposer_type when 'LA_PROPOSER_PAYOR_SAME' then prod.ins_contact_no 
            when 'PROPOSER_PAYER_SAME' then prod.pro_contact_no when 'LA_PROPOSER_PAYOR_DIFF' then json_value(prod.PAY_PERSONNEL_DETAILS,'$.pay_mobile') else json_value(prod.PAY_PERSONNEL_DETAILS,'$.pay_mobile') end as  Phonenumber,

            '01' as PaymentTypeCode,
            'NB'as PaymentType,
            '02' as RecordtypeCode,
            'PolicyNo' as Recordtype,
            prod.proposal_number as PolicyApplicationNo,

            pay.transaction_amount as TrxnAmount,
            '' as MandateStartDate,
            '' as MandateEndDate,

            json_value(prod.PLAN_DETAILS,'$.frequency') as Frequency,

            json_value(prod.BANK_DETAILS,'$.mandate_amount') as MaxMandateAmount,


            'Payment' as TrxnType,

            convert(date,dateadd(mi,000,pay.PAYMENT_DATE)) as TransactionDate,

            prod.PLAN_NAME as  AdditionalField8


            from T_PROPOSAL_DETAILS prod
            left join T_PAYMENT_DETAILS pay on pay.PROPOSAL_NUMBER=prod.PROPOSAL_NUMBER 
            where prod.PROPOSAL_NUMBER='{appNo}'


            """
    
    cursor.execute(Query)
    x = cursor.fetchall()
    if(len(x)>0):
        API_Call(x,URL)
    else:
        print("****No Data found in th DQ****")
        file1.write("****No Data found in th DQ****"+"\n")
    cursor.close()
    conn.close


# def dbconnect_ind(policyNo):
#     conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
#                             'Server=10.1.40.55,53205;'
#                             'Database=DIGISALES;'
#                             'UID=DIGISALES;'
#                             'PWD=sud@1234;'
#                             )
#     cursor = conn.cursor()
#     print("Data Extraction from DQ in progress...")

#     Query=f"""select
#             FORMAT ( GETDATE(), 'yyyyMMddHHmmss')+prod.PROPOSAL_NUMBER as AppTransactionID,
#             'DQ' as ApplicationName,
#             prod.proposal_number as CustomerID,

#             case prod.proposer_type when 'LA_PROPOSER_PAYOR_SAME' then prod.INS_FULL_NAME +' '+prod.ins_lastname 
#             when 'PROPOSER_PAYER_SAME' then prod.PRO_FULL_NAME +' '+prod.pro_lastname  when 'LA_PROPOSER_PAYOR_DIFF' then json_value(prod.PAY_PERSONNEL_DETAILS,'$.pay_firstname')+' '+json_value(prod.PAY_PERSONNEL_DETAILS,'$.pay_lastname')  else json_value(prod.PAY_PERSONNEL_DETAILS,'$.pay_firstname')+' '+json_value(prod.PAY_PERSONNEL_DETAILS,'$.pay_lastname') end as  CustomerName,

#             iif((case prod.proposer_type when 'LA_PROPOSER_PAYOR_SAME' then prod.ins_email when 'PROPOSER_PAYER_SAME' then prod.pro_email when 'LA_PROPOSER_PAYOR_DIFF' then json_value(prod.PAY_PERSONNEL_DETAILS,'$.pay_email') else json_value(prod.PAY_PERSONNEL_DETAILS,'$.pay_email') end )!='',
#             (case prod.proposer_type when 'LA_PROPOSER_PAYOR_SAME' then prod.ins_email when 'PROPOSER_PAYER_SAME' then prod.pro_email when 'LA_PROPOSER_PAYOR_DIFF' then json_value(prod.PAY_PERSONNEL_DETAILS,'$.pay_email') else json_value(prod.PAY_PERSONNEL_DETAILS,'$.pay_email') end ),'test@sudlife.in') as  EmailAddress ,


#             case prod.proposer_type when 'LA_PROPOSER_PAYOR_SAME' then prod.ins_contact_no 
#             when 'PROPOSER_PAYER_SAME' then prod.pro_contact_no when 'LA_PROPOSER_PAYOR_DIFF' then json_value(prod.PAY_PERSONNEL_DETAILS,'$.pay_mobile') else json_value(prod.PAY_PERSONNEL_DETAILS,'$.pay_mobile') end as  Phonenumber,

#             '01' as PaymentTypeCode,
#             'NB'as PaymentType,
#             '02' as RecordtypeCode,
#             'PolicyNo' as Recordtype,
#             prod.proposal_number as PolicyApplicationNo,

#             pay.transaction_amount as TrxnAmount,
#             '' as MandateStartDate,
#             '' as MandateEndDate,

#             json_value(prod.PLAN_DETAILS,'$.frequency') as Frequency,

#             json_value(prod.BANK_DETAILS,'$.mandate_amount') as MaxMandateAmount,


#             'Payment' as TrxnType,

#             convert(date,dateadd(mi,000,pay.PAYMENT_DATE)) as TransactionDate,

#             prod.PLAN_NAME as  AdditionalField8


#             from T_PROPOSAL_DETAILS prod
#             left join T_PAYMENT_DETAILS pay on pay.PROPOSAL_NUMBER=prod.PROPOSAL_NUMBER 
#             where prod.POLICY_STATUS_DESC='submitted' 
#             and ISJSON(prod.PLAN_DETAILS)=1 
#             and ISJSON(prod.BANK_DETAILS)=1 
#             and ISJSON(prod.PAY_PERSONNEL_DETAILS)=1 
#             and prod.PRODUCT_CODE!='1'
#             and pay.PAYMENT_MODE='PG Link'
#             and prod.JOURNEY_STAGE='Submitted'
#             and prod.PROPOSAL_NUMBER='{policyNo}'
#             """
    
#     cursor.execute(Query)
#     x = cursor.fetchall()
#     if(len(x)>0):
#         API_Call(x,URL)
#     else:
#         print("****No Data found in th DQ****")
#         file1.write("****No Data found in th DQ****"+"\n")
#     cursor.close()
#     conn.close

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
        try:
            li=[]
            count+=1
            print(str(count)+". "+str(i))
            j=(str(i).replace("(","")).replace(")","")
            li=j.split(",")
            AppTransactionId=li[0].replace("\'","")
            ApplicationName=li[1].replace("\'","")
            CustomerId=li[2].replace("\'","")
            CustomerName=li[3].replace("\'","")[1:]
            EmailAddress=li[4].replace("\'","")
            PhoneNumber=li[5].replace("\'","")
            PaymentTypeCode=li[6].replace("\'","")
            PaymentType=li[7].replace("\'","")
            RecordTypeCode=li[8].replace("\'","")
            RecordType=li[9].replace("\'","")
            PolicyApplicationNo=li[10].replace("\'","")
            TrxnAmount=li[11].replace("\'","")
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

        except Exception as e:
            # print(str(e)+"Error Occured!!!")
            print("Error Occured!!!     "+str(e))
            # print("Error Occured!!!     "+str(e))
            file1.write("Error Occured!!!"+str(e)+"\n")
            continue

try:
    policy_No_list=[]
    path = "D:\\PGLinkGenerator\\MISC_PGLINK\\Input.txt"
    file1 = open(path,"r+")
    policy_No_list=[]
    for policyNo in file1.readlines():
        policy_No_list.append(policyNo.replace("\n",""))

    now = datetime.now() 
    logdate=str(now.strftime("%Y%m%d_%H%M%S"))
    save_path = 'D:\\PGLinkGenerator\\MISC_PGLINK\\Misc_Logs'
    if not os.path.exists(save_path):
            os.mkdir(save_path)
    completeName = save_path+"\\"+'Misc_log_'+logdate+'.txt'
    file1 = open(completeName, "a")
    for policyNo in policy_No_list:
        if(len(policyNo)>8):
            dbconnect_grp(policyNo)
        else:
            dbconnect_ind(policyNo)
    file1.close()
except Exception as e:
    print(e)
time.sleep(5)