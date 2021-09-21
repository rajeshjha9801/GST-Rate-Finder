import pandas as pd
import sqlite3
import re
from sqlalchemy import create_engine
import pickle
import os
from Utils import load_pickle_file,write_pickle_file
import shutil
def main(db_path):
    db_engine=create_engine('sqlite:///ratemaster.db')

    df_1=pd.read_sql_query('select * from ScheduleI',db_engine)
    df_2=pd.read_sql_query('select * from ScheduleII',db_engine)
    df_3=pd.read_sql_query('select * from ScheduleIII',db_engine)
    df_4=pd.read_sql_query('select * from ScheduleIV',db_engine)
    df_5=pd.read_sql_query('select * from ScheduleV',db_engine)
    df_6=pd.read_sql_query('select * from ScheduleVI',db_engine)
    df_7=pd.read_sql_query('select * from Notification2',db_engine)
    df_1.columns=["SLNO","chapter","Description"]
    df_2.columns=["SLNO","chapter","Description"]
    df_3.columns=["SLNO","chapter","Description"]
    df_4.columns=["SLNO","chapter","Description"]
    df_5.columns=["SLNO","chapter","Description"]
    df_6.columns=["SLNO","chapter","Description"]
    df_7.columns=["SLNO","chapter","Description"]
    lst=[]
    lst.append(df_1)
    lst.append(df_2)
    lst.append(df_3)
    lst.append(df_4)
    lst.append(df_5)
    lst.append(df_6)
    lst.append(df_7)
    df_1["Schedule No."]="Schedule I"
    df_2["Schedule No."]="Schedule II"
    df_3["Schedule No."]="Schedule III"
    df_4["Schedule No."]="Schedule IV"
    df_5["Schedule No."]="Schedule V"
    df_6["Schedule No."]="Schedule VI"
    df_7["Schedule No."]="NA"
    schedule_rate_mapping={"Schedule I":"2.5%",
      "Schedule II":"6%",
           "Schedule III":"9%",
           "Schedule IV":"14%",
           "Schedule V":"1.5%",
           "Schedule VI":"0.125%",
                           "NA":"0%"
    }
    Notification_mapping={"Schedule I":"Notification No.1 2017",
      "Schedule II":"Notification No.1 2017",
           "Schedule III":"Notification No.1 2017",
           "Schedule IV":"Notification No.1 2017",
           "Schedule V":"Notification No.1 2017",
           "Schedule VI":"Notification No.1 2017",
                           "NA":"Notification No.2 2017"
    }
    df=pd.concat(lst)
    df.reset_index(inplace=True,drop=True)


    def splitter(x):
        if str(x).find("(")!=-1:
            returnee=str(x).split("(")
            return("("+returnee[1])
        elif str(x).find("[")!=-1:
            returnee=str(x).split("[")
            return("["+returnee[1])
        elif str(x).find("or")!=-1:
            returnee=str(x).split("or")
            return("(or"+returnee[1]+")") 
        else:
            return("")
    def splitter_1(x):
        
        if str(x).find("(")!=-1:
            returnee=str(x).split("(")
            return(returnee[0])
        elif str(x).find("[")!=-1:
            returnee=str(x).split("[")
            return(returnee[0])
        elif str(x).find("or")!=-1:
            returnee=str(x).split("or")
            return(returnee[0])
       
        else:
            
            x=str(x).replace("|","")
            x=str(x).replace("-","")
            x=str(x).replace("_","")
            
            return(str(x))    
    df["Description"]=df["chapter"].apply(lambda x:str(splitter(x)))+df["Description"]
    df["chapter"]=df["chapter"].apply(lambda x:str(splitter_1(x)))
    chapter=list(df["chapter"])
    slno=list(df["SLNO"])
    desc=list(df["Description"])
    schdeuleno=list(df["Schedule No."])
    for i in range(len(chapter)):
        if chapter[i].find(",")!=-1:
            x=chapter[i].split(",",maxsplit=10)
            
            
            for j in range(len(x)):
                chapter.append(x[j])
                slno.append(slno[i])
                desc.append(desc[i])
                schdeuleno.append(schdeuleno[i])
            chapter.pop(i)
            slno.pop(i) 
            desc.pop(i) 
            schdeuleno.pop(i)
        elif len(chapter[i])==16: 
            x=[chapter[i][0:8],chapter[i][8:15]]
            
            for j in range(len(x)):
                chapter.append(x[j])
                slno.append(slno[i])
                desc.append(desc[i])
                schdeuleno.append(schdeuleno[i])
            chapter.pop(i)
            slno.pop(i) 
            desc.pop(i)
            schdeuleno.pop(i)
        elif len(chapter[i])==0:
            chapter.pop(i)
            slno.pop(i) 
            desc.pop(i) 
            schdeuleno.pop(i)
    df=pd.DataFrame()

    df["Chapter"]=pd.Series(chapter)
    df["Desc"]=pd.Series(desc)
    df["Schedule No."]=pd.Series(schdeuleno)
    df["Notification"]=df["Schedule No."].map(Notification_mapping)
    df["Schedule Entry no."]=pd.Series(slno)
    df["Rate"]=df["Schedule No."].map(schedule_rate_mapping) 
    df.to_sql("Aggregate",db_engine,if_exists='replace',index=False)
    df.sort_values(by="Chapter",inplace=True,ignore_index=True)
    regex='^\d{2,}'
    df_final=df[df["Chapter"].apply(lambda x:True if re.search(regex,str(x)) else False)]
    df_final.reset_index(inplace=True,drop=True)
    from collections import defaultdict
    hsn_dict=defaultdict()
    chapter_lst=list(df_final["Chapter"])
    for i in range(len(df_final)):
        hsn_dict[df_final["Chapter"][i]]=defaultdict()
        hsn_dict[df_final["Chapter"][i]]["Count"]=len(df_final[df_final["Chapter"]==df_final["Chapter"][i]])
        hsn_dict[df_final["Chapter"][i]]["Desc"]=list(df_final[df_final["Chapter"]==df_final["Chapter"][i]]["Desc"])
        hsn_dict[df_final["Chapter"][i]]["Schedule No."]=list(df_final[df_final["Chapter"]==df_final["Chapter"][i]]["Schedule No."])
        hsn_dict[df_final["Chapter"][i]]["Notification"]=list(df_final[df_final["Chapter"]==df_final["Chapter"][i]]["Notification"])
        hsn_dict[df_final["Chapter"][i]]["Schedule Entry no."]=list(df_final[df_final["Chapter"]==df_final["Chapter"][i]]["Schedule Entry no."])
        hsn_dict[df_final["Chapter"][i]]["Rate"]=list(df_final[df_final["Chapter"]==df_final["Chapter"][i]]["Rate"])
        if df_final["Chapter"][i][0:len(df_final["Chapter"][i])-2] in chapter_lst and len(df_final["Chapter"][i])>2:
            hsn_dict[df_final["Chapter"][i]]["DrillDown"]=df_final["Chapter"][i][0:len(df_final["Chapter"][i])-2]
        else:
            hsn_dict[df_final["Chapter"][i]]["DrillDown"]=None
            
    if db_path:
        write_pickle_file(hsn_dict,db_path+"//"+'hsn_dict.pkl')
    else:
        write_pickle_file(hsn_dict,f'hsn_dict.pkl')

        


#main()      
