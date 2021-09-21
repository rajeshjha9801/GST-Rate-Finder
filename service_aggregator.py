import pandas as pd
import sqlite3
import re
from sqlalchemy import create_engine
import pickle
import os
from Utils import load_pickle_file,write_pickle_file
import shutil
from collections import defaultdict
import traceback
def main(db_path):
    try:
        db_engine=create_engine('sqlite:///ratemaster.db')
        df_8=pd.read_sql_query('select * from Notification11',db_engine)
        df_9=pd.read_sql_query('select * from Notification12',db_engine)

        df_9=df_9.fillna(method='ffill')
        df_8=df_8.fillna(method='ffill')
        df_9.drop_duplicates(inplace=True)
        df_8.drop_duplicates(inplace=True)

        df_8["Schedule No."]="Notification No.12 2017"
        df_9["Schedule No."]="Notification No.11 2017"
        df_8=df_8[~df_8["Chapter, Section, Heading, Group or Service Code (Tariff)"].str.contains("Chapter")]
        df_9=df_9[~df_9["Chapter, Section or Heading"].str.contains("Chapter")]
        df_9=df_9[~df_9["Chapter, Section or Heading"].str.contains("Section")]
        df_8.reset_index(inplace=True,drop=True)
        df_9.reset_index(inplace=True,drop=True)

        df_9["Heading"]=df_9["Chapter, Section or Heading"].apply(lambda x:"("+str(x).split(" (")[1] if str(x).find("(")!=-1 else None)
        df_9["Chapter, Section or Heading"]=df_9["Chapter, Section or Heading"].apply(lambda x:str(x).split("(")[0] if str(x).find("(")!=-1 else x)
        df_9["Chapter, Section or Heading"]=df_9["Chapter, Section or Heading"].apply(lambda x:str(x).replace("Heading","") if str(x).find("Heading")!=-1 else x)

        def post_processing(df_func):

            lstB=list(df_func["Sl No."])

            lstA=list(df_func["Chapter, Section or Heading"])
            lstC=list(df_func["Description of Service"])
            lstD=list(df_func["Rate (per cent.)"])
            lstE=list(df_func["Condition"])
            lstF=list(df_func["Schedule No."])
            lstG=list(df_func["Heading"])
            
            lst1=[]


            for i in range(len(lstA)):
                df=pd.DataFrame()
                df["Sl No."]=None
                df["Chapter, Section or Heading"]=None
                df["Description of Service"]=None

                if (str(lstA[i]).find("or")!=-1):
                    lst_1=str(lstA[i]).split("or",maxsplit=50)
                    lst_1=[str(x).replace(" ","") for x in lst_1]
                    df["Chapter, Section or Heading"]=pd.Series(lst_1)
                    df["Sl No."]=lstB[i]
                    df["Description of Service"]=lstC[i]
                    df["Rate (per cent.)"]=lstD[i]
                    df["Condition"]=lstE[i]
                    df["Schedule No."]=lstF[i]
                    df["Heading"]=lstG[i]

         

                else:

                    df["Chapter, Section or Heading"]=pd.Series(lstA[i].replace(" ",""))
                    df["Sl No."]=pd.Series(lstB[i])
                    df["Description of Service"]=pd.Series(lstC[i]) 
                    df["Rate (per cent.)"]=lstD[i]
                    df["Condition"]=lstE[i]
                    df["Schedule No."]=lstF[i]
                    df["Heading"]=lstG[i]


          



                lst1.append(df)
            return(pd.concat(lst1,ignore_index=True))             


        df_9=post_processing(df_9)

        df_8["Heading"]=None
        df_8["Chapter, Section, Heading, Group or Service Code (Tariff)"]=df_8["Chapter, Section, Heading, Group or Service Code (Tariff)"].apply(lambda x:str(x).split("or any other Heading")[0] if str(x).find("or any other Heading")!=-1 else x)
        df_8["Chapter, Section, Heading, Group or Service Code (Tariff)"]=df_8["Chapter, Section, Heading, Group or Service Code (Tariff)"].apply(lambda x:str(x).replace("Heading","") if str(x).find("Heading")!=-1 else x)

        def post_processing_1(df_func):

            lstB=list(df_func["Sl. No."])

            lstA=list(df_func["Chapter, Section, Heading, Group or Service Code (Tariff)"])
            lstC=list(df_func["Description of Services"])
            lstD=list(df_func["Rate (per cent.)"])
            lstE=list(df_func["Condition"])
            lstF=list(df_func["Schedule No."])
            lstG=list(df_func["Heading"])
            
            lst1=[]


            for i in range(len(lstA)):
                df=pd.DataFrame()
                df["Sl. No."]=None
                df["Chapter, Section, Heading, Group or Service Code (Tariff)"]=None
                df["Description of Services"]=None

                if (str(lstA[i]).find("or")!=-1):
                    lst_1=str(lstA[i]).split("or",maxsplit=50)
                    lst_1=[str(x).replace(" ","") for x in lst_1]
                    df["Chapter, Section, Heading, Group or Service Code (Tariff)"]=pd.Series(lst_1)
                    df["Sl. No."]=lstB[i]
                    df["Description of Services"]=lstC[i]
                    df["Rate (per cent.)"]=lstD[i]
                    df["Condition"]=lstE[i]
                    df["Schedule No."]=lstF[i]
                    df["Heading"]=lstG[i]

         

                else:

                    df["Chapter, Section, Heading, Group or Service Code (Tariff)"]=pd.Series(lstA[i].replace(" ",""))
                    df["Sl. No."]=pd.Series(lstB[i])
                    df["Description of Services"]=pd.Series(lstC[i]) 
                    df["Rate (per cent.)"]=lstD[i]
                    df["Condition"]=lstE[i]
                    df["Schedule No."]=lstF[i]
                    df["Heading"]=lstG[i]


          



                lst1.append(df)
            return(pd.concat(lst1,ignore_index=True))             


        df_8=post_processing_1(df_8)

        df_9.columns=["Schedule Entry no.","Chapter","Desc","Rate","Condition","Notification","Heading"]
        df_8.columns=["Schedule Entry no.","Chapter","Desc","Rate","Condition","Notification","Heading"]
        df=pd.concat([df_8,df_9])

        df.sort_values(by="Chapter",inplace=True,ignore_index=True)
        regex='^\d{2,}'
        df_final=df[df["Chapter"].apply(lambda x:True if re.search(regex,str(x)) else False)]
        hsn_dict=defaultdict()
        chapter_lst=list(df_final["Chapter"])
        for i in range(len(df_final)):
            hsn_dict[df_final["Chapter"][i]]=defaultdict()
            hsn_dict[df_final["Chapter"][i]]["Count"]=len(df_final[df_final["Chapter"]==df_final["Chapter"][i]])
            hsn_dict[df_final["Chapter"][i]]["Desc"]=list(df_final[df_final["Chapter"]==df_final["Chapter"][i]]["Desc"])
            hsn_dict[df_final["Chapter"][i]]["Heading"]=list(df_final[df_final["Chapter"]==df_final["Chapter"][i]]["Heading"])
            hsn_dict[df_final["Chapter"][i]]["Condition"]=list(df_final[df_final["Chapter"]==df_final["Chapter"][i]]["Condition"])
            hsn_dict[df_final["Chapter"][i]]["Notification"]=list(df_final[df_final["Chapter"]==df_final["Chapter"][i]]["Notification"])
            hsn_dict[df_final["Chapter"][i]]["Schedule Entry no."]=list(df_final[df_final["Chapter"]==df_final["Chapter"][i]]["Schedule Entry no."])
            hsn_dict[df_final["Chapter"][i]]["Rate"]=list(df_final[df_final["Chapter"]==df_final["Chapter"][i]]["Rate"])
            if df_final["Chapter"][i][0:len(df_final["Chapter"][i])-2] in chapter_lst and len(df_final["Chapter"][i])>2:
                hsn_dict[df_final["Chapter"][i]]["DrillDown"]=df_final["Chapter"][i][0:len(df_final["Chapter"][i])-2]
            else:
                hsn_dict[df_final["Chapter"][i]]["DrillDown"]=None

        if db_path:
            write_pickle_file(hsn_dict,db_path+"\\"+'service_hsn_dict.pkl')
        else:
            write_pickle_file(hsn_dict,f'service_hsn_dict.pkl')
    except Exception as e:
        print(traceback.format_exc())
        
