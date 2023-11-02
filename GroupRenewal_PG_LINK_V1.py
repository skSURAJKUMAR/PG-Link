import requests
import pyodbc
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
URL=r'https://securegateway.sudlife.in/GenerateURL/api/PaymentURL'
def dbconnect():
    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                            'Server=10.1.6.123,1433;'
                            'Database=InwardOutward;'
                            'UID=recon;'
                            'PWD=recon@123;'
                            )
    print("Data Extraction from DQ in progress...")
    cursor = conn.cursor()
    # print(conn)
    Query="""exec Grp_Renewal_PGLINK"""
    Query="""set arithabort on 
SELECT ROW_NUMBER() over(order by G2.APLNNO) as [Sr. No.],
                                            G2.APLNNO  as AppTransactionID,
                                            'DQ' as ApplicationName,
                                            C2.CLNTNUM as CustomerID,
                                            Replace((Replace((Replace(Upper(C2.LSURNAME), ',', ' ')), Char(13), ' ')), Char(10), ' ')
                                     + ' '
                                     + Replace((Replace((Replace(Upper(C2.LGIVNAME), ',', ' ')), Char(13), ' ')), Char(10), ' ') as CustomerName,
                                        '02' as [PaymentTypeCode(01/02/03)],
                                        'Renewal' as [PaymentType(NB/Renewal/Revival)],
                                        '01' as    [RecordtypeCode(01/02)],
                                              'Application' as [Recordtype(Policy/Application)],
                                        G2.APLNNO as      PolicyApplicationNo,
                              
                                       Ceiling(Isnull((AC.[Base premium]), 0)
                                     + Isnull(((AC.[Base premium]))*(0.18), 0)
                                     + AC.[Extra Premium]
                                     + Isnull(t12.Tolerence, 0)
                                     + t14.[Member Suspense Balance]) as Amount,
                                                  '' as Remarks,
                                            '' as PaymentURL,
                                            ISSDATE as TransactionDate,
                                           case when len(isnull(CX.RINTERNET,''))=0 then 'Test@test.test' else CX.RINTERNET end as EmailAddress,
                                                   (
                              CASE
                                     WHEN Isnumeric(Len(NULLIF(CX.RMBLPHONE,''))) = '1'          
                                                   AND Len(NULLIF(CX.RMBLPHONE,'')) = '10'          
                                                   AND LEFT(NULLIF(CX.RMBLPHONE,''), 1) IN ( '6','7', '8', '9' )
                                     THEN cast(NULLIF(CX.RMBLPHONE,'') as varchar)    
                              END ) as Phonenumber,
                                            '' as AdditionalField1,
                                            '' as AdditionalField2,
                                            '' as AdditionalField3,
                                            '' as AdditionalField4,
                                            '' as AdditionalField5,
                                            '' as AdditionalField6,
                                            '' as AdditionalField7,
                                            'New Aashiana Suraksha' as AdditionalField8,
                                                  '' as PaymentGatewayType,
                                            '12' as ExpiryDays
                              FROM   [SUD_GA_ODS].[dbo].GMHDPF GM WITH (NOLOCK)
                                     INNER JOIN SUD_GA_ODS..CLNTPF C2 WITH (NOLOCK)
                                             ON C2.CLNTNUM = GM.CLNTNUM
                                                AND C2.VALIDFLAG = 1
                                                AND GM.DPNTNO = '00'
                                     INNER JOIN SUD_GA_ODS..CHDRPF CH WITH (NOLOCK)
                                             ON CH.CHDRNUM = GM.CHDRNUM
                                     INNER JOIN SUD_GA_ODS..CLNTPF C1 WITH (NOLOCK)
                                             ON C1.CLNTNUM = CH.COWNNUM
                                                AND C1.VALIDFLAG = 1
                                     LEFT JOIN SUD_GA_ODS..GMD2PF G2 WITH (NOLOCK)
                                            ON GM.CHDRNUM = G2.CHDRNUM
                                               AND GM.MBRNO = G2.MBRNO
                                               AND GM.DPNTNO = G2.DPNTNO
                              
                                     LEFT JOIN (SELECT B.*,
                                                       A.RLDGACCT,
                                                       A.ORIGAMT
                                                FROM   (SELECT a.RLDGACCT,
                                                               Sum(a.ORIGAMT) ORIGAMT
                                                        FROM   SUD_GA_ODS..ACMVPF a WITH (NOLOCK)
                                                        WHERE  BATCTRCDE IN ( 'T903', 'T922' )
                                                        GROUP  BY a.RLDGACCT) a
                                                       INNER JOIN (SELECT b.CHDRNUM,
                                                                          b.MBRNO,
                                                                          ( a.BPREM )                                                   [Base premium],
                                                                          ( a.BEXTPRM )                                                 [Extra Premium],
                                                                          ( b.GRPGST + b.ZCESSAMT01 + b.ZCESSAMT02
                                                                            + b.ZSWCHAMT + b.ZFUTOAMT + b.ZFUTTAMT )                    [Service Tax],
                                                                          ( GRPSDUTY )                                                  AS [Stamp Duty],
                                                                          Row_number()
                                                                            OVER(
                                                                              PARTITION BY b.CHDRNUM, b.MBRNO
                                                                              ORDER BY b.CHDRNUM, b.MBRNO, BATCACTYR ASC, BATCACTMN ASC)RNO
                                                                   --SUM(Case when a.BPREM > 0 then 1 when a.BPREM < 0 then -1 ELSE 0 END)NoOfInst
                                                                   FROM   SUD_GA_ODS..GBIDPF a WITH (NOLOCK),
                                                                          SUD_GA_ODS..GBIHPF b WITH (NOLOCK)
                                                                   WHERE  a.BILLNO = b.BILLNO
                                                                          AND b.CHDRNUM LIKE( 'MR%' )
                                                                          AND a.BPREM > 0
                                                                  --GROUP BY b.CHDRNUM,b.MBRNO
                                                                  ) b
                                                              ON a.RLDGACCT = b.CHDRNUM + '-' + b.MBRNO + '00'
                                                                  AND B.RNO = 1)AC
                                            ON GM.CHDRNUM + '-' + GM.MBRNO + GM.DPNTNO = AC.RLDGACCT
                                     LEFT JOIN (SELECT a.RLDGACCT,
                                                       Sum(a.SACSCURBAL) [Member Suspense Balance]
                                                FROM   [SUD_GA_ODS]..ACBLPF a WITH (NOLOCK)
                                                WHERE  a.SACSTYP = 'SM'
                                                       AND SACSCODE = 'GM'
                                                GROUP  BY a.RLDGACCT)t14
                                            ON GM.CHDRNUM + '-' + GM.MBRNO + GM.DPNTNO = t14.RLDGACCT
                                     LEFT JOIN (SELECT T5.CHDRNUM,
                                                       T5.MBRNO,
                                                       T5.DPNTNO,
                                                       SUD_MIS.dbo.Ladate_2_datetime(Max(GI.ISSDATE)) AS ISSDATE
                                                FROM   (SELECT CHDRNUM,
                                                               MBRNO,
                                                               DPNTNO,
                                                               BILLNO
                                                        FROM   SUD_GA_ODS..GPMDPF WITH (NOLOCK)
                                                        WHERE  CHDRNUM LIKE 'MR%'
                                                        GROUP  BY CHDRNUM,
                                                                  MBRNO,
                                                                  DPNTNO,
                                                                  BILLNO) t5
                                                       INNER JOIN SUD_GA_ODS..GIDTPF GI WITH (NOLOCK)
                                                               ON t5.CHDRNUM = GI.CHDRNUM
                                                                  AND GI.FRDOCNO <= T5.BILLNO
                                                                  AND TODOCNO >= T5.BILLNO
                                                WHERE  GI.CHDRNUM LIKE 'MR%'
                                                GROUP  BY T5.CHDRNUM,
                                                          T5.MBRNO,
                                                          T5.DPNTNO)GD
                                            ON GD.CHDRNUM = GM.CHDRNUM
                                               AND GD.MBRNO = GM.MBRNO
                                               AND GD.DPNTNO = GM.DPNTNO
                                     LEFT JOIN SUD_GA_ODS..CLEXPF CX WITH (NOLOCK)
                                            ON CX.CLNTNUM = C2.CLNTNUM
                                               AND CX.VALIDFLAG = 1
                                     LEFT JOIN (SELECT a.RLDGACCT,
                                                       Sum(a.SACSCURBAL) [Tolerence]
                                                FROM   [SUD_GA_ODS]..ACBLPF a WITH (NOLOCK)
                                                WHERE  a.SACSTYP = 'TL'
                                                GROUP  BY a.RLDGACCT) t12
                                            ON GM.CHDRNUM + '-' + GM.MBRNO + GM.DPNTNO = t12.RLDGACCT
                              WHERE  GM.CHDRNUM LIKE 'MR%'
                                     AND GM.STATCODE IN ( 'IF')
                                     AND G2.BILLFREQ IN ( '01', '12' ) --  Frequncy Yearly &amp; Monthly
                                     and NULLIF(DATEDIFF(DAY,GETDATE(),SUD_MIS.dbo.Ladate_2_datetime(G2.PTDATE)),0) in (1,3,5,7)
                                      and (Isnull((AC.[Base premium]), 0)+ Isnull(((AC.[Base premium]))*(0.18), 0)+ AC.[Extra Premium]+ Isnull(t12.Tolerence, 0)+ t14.[Member Suspense Balance] )>10
                                       and G2.BILLCHNL in('A','C')
                                             and 1=(
                              CASE
                                     WHEN Isnumeric(Len(NULLIF(CX.RMBLPHONE,''))) = '1'          
                                                   AND Len(NULLIF(CX.RMBLPHONE,'')) = '10'                              
                                                   AND LEFT(NULLIF(CX.RMBLPHONE,''), 1) IN ( '6','7', '8', '9' )
                                     THEN 1
                              END )"""
    
    # print(Query)
    cursor.execute(Query)
    x = cursor.fetchall()
    cursor.close()
    conn.close
    # return rows

    if(len(x)>0):
        try:
            API_Call(x,URL)
            # updateQuery=f"UPDATE GRP_PGLINK_FROM_DATE SET fromDate = '{str(toDate)}' WHERE fromDate='{str(fromDate)}'"
            # cursor.execute(updateQuery)
            # cursor.commit()
            # print("Datetime updated successfully.")
            # cursor.close()
            # conn.close
        except Exception as e:
            # print(str(e)+"Error Occured!!!")
            print("Error Occured!!!     "+str(e))
            # print("Error Occured!!!     "+str(e))
            file1.write("Error Occured!!!"+str(e)+"\n")
            
    else:
        print("****No Data found in th DQ****")
        file1.write("****No Data found in th DQ****"+"\n")
        # updateQuery=f"UPDATE GRP_PGLINK_FROM_DATE SET fromDate = '{str(toDate)}' WHERE fromDate='{str(fromDate)}'"
        # cursor.execute(updateQuery)
        # cursor.commit()
        # print("Datetime updated successfully.")
        # cursor.close()
        # conn.close
    

def checksum(AppTransactionID,CustomerID,CustomerName,):
    import hashlib
    f=str(AppTransactionID)+'|'+'DQ'+'|'+str(CustomerID)+'|'+str(CustomerName)+'|'+'01'+'|'+'NB'+'|'+'02'+'|'+'PolicyNo'+'|'+str(CustomerID)
    result = hashlib.md5(f.encode()).hexdigest()
    # print(hashlib.md5(f.encode()).hexdigest())
    return result


def InsertData(AppTransactionID,ApplicationName,CustomerID,CustomerName,PaymentTypeCode,PaymentType,RecordtypeCode,Recordtype,PolicyApplicationNo,Amount,Remarks,PaymentURL,TransactionDate,EmailAddress,Phonenumber,CheckSum,AdditionalField1,AdditionalField2,AdditionalField3,AdditionalField4,AdditionalField5,AdditionalField6,AdditionalField7,AdditionalField8):
    x=TransactionDate

    Insert_Query=f"Insert into VFirstDB..TR_Renewals_payment_url(AppTransactionID,ApplicationName,CustomerID,CustomerName,PaymentTypeCode,PaymentType,RecordtypeCode,Recordtype,PolicyApplicationNo,Amount,Remarks,ReturnURL,TransactionDate,EmailAddress,Phonenumber,CheckSum,AdditionalField1,AdditionalField2,AdditionalField3,AdditionalField4,AdditionalField5,AdditionalField6,AdditionalField7,AdditionalField8) values(\'"+AppTransactionID+"\',\'"+ApplicationName+"\',\'"+CustomerID+"\',\'"+CustomerName+"\',\'"+PaymentTypeCode+"\',\'"+PaymentType+"\',\'"+RecordtypeCode+"\',\'"+Recordtype+"\',\'"+PolicyApplicationNo+"\',\'"+Amount+"\',\'"+Remarks+"\',\'"+PaymentURL+"\',\'"+TransactionDate+"\',\'"+EmailAddress+"\',\'"+Phonenumber+"\',\'"+CheckSum+"\',\'"+AdditionalField1+"\',\'"+AdditionalField2+"\',\'"+AdditionalField3+"\',\'"+AdditionalField4+"\',\'"+AdditionalField5+"\',\'"+AdditionalField6+"\',\'"+AdditionalField7+"\',\'"+AdditionalField8+"\')"
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
    # cursor2.execute(Insert_Query)
    # cursor2.commit()
    cursor2.close()
    print("Insertion Completed\n")
    file1.write("Insertion Completed\n")


def API_Call(x,URL):
    count=0
    for i in x:
        try:
            count+=1
            # print(str(count)+". "+str(i))
            from datetime import datetime
            now = datetime.now() 
            datetime=str(now.strftime("%Y%m%d%H%M%S"))
            li=[]
            j=str(i)
            li=j.split(",")
            
            AppTransactionId= ((li[1].replace("Decimal('","")).replace("\')","")).replace("\'","")
            InsertAppTransacationId=(str(datetime)+AppTransactionId).strip("")
            ApplicationName= li[2].replace("\'","")
            CustomerName= li[4].replace("\'","").strip()
            # if(count==1):
            #     CustomerName='Anand Puranik'
            #     PhoneNumber= '9820760699'
            # if(count==2):
            #     CustomerName='Jayceline Antony'
            #     PhoneNumber= '9821215454'
            # if(count==3):
            #     CustomerName='Anish Shah'
            #     PhoneNumber= '9833511685'
            # if(count==4):
            #     CustomerName='Sunil Choudhari'
            #     PhoneNumber= '9224332823'
            # if(count==5):
            #     break
            customerID= li[3].replace("\'","")
            # customerID= '987654321'
            EmailAddress= li[18].replace("\'","")
            PhoneNumber= li[19].replace("\'","")
            
            PaymentTypeCode= li[5].replace("\'","")
            InsertPaymentTypeCode=str(int(PaymentTypeCode))
            PaymentType= li[6].replace("\'","")
            RecordTypeCode= li[7].replace("\'","")
            InsertRecordTypeCode= str(int(RecordTypeCode))
            RecordType= li[8].replace("\'","")
            PolicyApplicationNo= li[9].replace("\'","")
            # InsertTrxnAmount=li[10]
            TrxnAmount=((li[10].replace("Decimal(\'","")).replace("\')","")).replace("\'","")
            # TrxnAmount='1'
            MandateStartDate= ""
            mandateEndDate= ""
            Frequency= "" 
            MaxMandateAmount= ""
            TrxnType= "Payment"
            Remarks=li[11].replace("\'","")
            year=(li[13].replace("datetime.datetime(","")).replace(" ","")
            month=li[14].replace(" ","")
            if(int(month)<10):
                monthNo="0"+month
                month=monthNo
            day=(li[15].replace(")","")).replace(" ","")
            if(int(day)<10):
                dayNo="0"+day
                day=dayNo
            InsertTransactionDate=year+"-"+month+"-"+day
            # print(InsertTransactionDate)
            TransactionDate=(li[13]+","+li[14]+","+li[15]).replace("\')","")
            # print(TransactionDate)
            CheckSum=checksum(AppTransactionId,customerID,CustomerName)
            AdditionalField1=""
            AdditionalField2=""
            AdditionalField3=""
            AdditionalField4=""
            AdditionalField5=""
            AdditionalField6=""
            AdditionalField7=""
            AdditionalField8=(li[27].replace("\'","")).strip()
            PaymentGatewayType=li[26].replace("\'","")
            # print(PhoneNumber)
            dummy["AppTransactionID"]=InsertAppTransacationId.replace("\' ","\'")
            dummy["ApplicationName"]=ApplicationName.replace("\' ","\'")
            dummy["CustomerID"]= customerID.replace("\' ","\'")
            dummy["CustomerName"]=CustomerName.replace("\' ","\'")
            dummy["EmailAddress"]=EmailAddress.replace("\' ","\'")
            dummy["Phonenumber"]=PhoneNumber.replace("\' ","\'")
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
            InsertData(str(InsertAppTransacationId).replace(" ",""),str(ApplicationName).replace(" ",""),str (customerID).replace(" ",""),str (CustomerName),str (InsertPaymentTypeCode).replace(" ",""),str (PaymentType).replace(" ",""),str (InsertRecordTypeCode).replace(" ",""),str (RecordType).replace(" ",""),str (PolicyApplicationNo).replace(" ",""),str (TrxnAmount).replace(" ",""),str (Remarks).replace(" ",""),str (PaymentURL).replace(" ",""),str (InsertTransactionDate).replace(" ",""),str (EmailAddress).replace(" ",""),str(PhoneNumber).replace(" ",""),str (CheckSum).replace(" ",""),str (AdditionalField1),str (AdditionalField2),str (AdditionalField3),str (AdditionalField4),str (AdditionalField5),str (AdditionalField6),str (AdditionalField7),str (AdditionalField8))
            # InsertData(str(InsertAppTransacationId).replace(" ",""),str(ApplicationName).replace(" ",""),str (customerID).replace(" ",""),'Sunil Chaudhari',str (InsertPaymentTypeCode).replace(" ",""),str (PaymentType).replace(" ",""),str (InsertRecordTypeCode).replace(" ",""),str (RecordType).replace(" ",""),str (PolicyApplicationNo).replace(" ",""),str (TrxnAmount).replace(" ",""),str (Remarks).replace(" ",""),str (PaymentURL).replace(" ",""),str (InsertTransactionDate).replace(" ",""),str (EmailAddress).replace(" ",""),str(PhoneNumber).replace(" ",""),str (CheckSum).replace(" ",""),str (AdditionalField1),str (AdditionalField2),str (AdditionalField3),str (AdditionalField4),str (AdditionalField5),str (AdditionalField6),str (AdditionalField7),str (AdditionalField8))
            # break
        except Exception as e:
            # print(str(e)+"Error Occured!!!")
            print("Error Occured!!!     "+str(e))
            # print("Error Occured!!!     "+str(e))
            file1.write("Error Occured!!!"+str(e)+"\n")
            continue
        # break
# ------------------------------------------------------------MAIN-------------------------------------------------------------------#
import time
# while(True):
from datetime import datetime
now = datetime.now() 
# datetime=str(now.strftime("%m-%d-%Y %H:%M:%S"))
# print(datetime)
# file1 = open('REN_log.txt', 'a')
# file1.write(datetime+"\n")
# x=dbconnect()
# URL=r"https://securegateway.sudlife.in/GenerateURL/api/PaymentURL"
# if(len(x)>0):
#     API_Call(x,URL)
# else:
#     print("****No Data found in th DQ****")
#     file1.write("****No Data found in th DQ****"+"\n")
#     # break
#     # time.sleep(900)

import os.path
import time
try:
    now = datetime.now() 
    logdate=str(now.strftime("%Y%m%d_%H%M%S"))
    save_path = 'D:\PGLinkGenerator\REN_Logs'
    if not os.path.exists(save_path):
            os.mkdir(save_path)
    completeName = save_path+"\\"+'REN_log_'+logdate+'.txt'
    file1 = open(completeName, "a")
    dbconnect()
except Exception as e:
    print(e)
time.sleep(5)