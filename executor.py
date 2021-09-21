import os
import pickle
import pandas as pd
from Utils import load_pickle_file
import traceback




def hsn_to_df(hsn,desc,db_path):
    try:
        if db_path:
            hsn_dict_load=load_pickle_file(db_path+"\\"+'hsn_dict.pkl')
        else:

            hsn_dict_load=load_pickle_file(f'hsn_dict.pkl')
        lst_2=[]
        desc_2=[]
        lst_4=[]
        desc_4=[]
        lst_6=[]
        desc_6=[]
        lst_8=[]
        desc_8=[]
        rate=[]
        NotificationNo=[]
        Schedule=[]
        Schedule_entry=[]

        df=pd.DataFrame()
        hsn=str(hsn).strip()
        loop_range=int(len(str(hsn))/2)
        for i in range(loop_range):
            if i==0:
                try:

                    for j in range(int(hsn_dict_load[hsn[0:2]]['Count'])):

                        desc_2.append(hsn_dict_load[hsn[0:2]]["Desc"][j])
                        lst_2.append(hsn[0:2])
                        rate.append(hsn_dict_load[hsn[0:2]]["Rate"][j])
                        NotificationNo.append(hsn_dict_load[hsn[0:2]]["Notification"][j])
                        Schedule.append(hsn_dict_load[hsn[0:2]]["Schedule No."][j])
                        Schedule_entry.append(hsn_dict_load[hsn[0:2]]["Schedule Entry no."][j])
                        desc_4.append(None)
                        lst_4.append(None)                
                        desc_6.append(None)
                        lst_6.append(None)             
                        desc_8.append(None)
                        lst_8.append(None) 
                except Exception as e:
                    pass              

            elif i==1:
                try:
                    for j in range(int(hsn_dict_load[hsn[0:4]]['Count'])):

                        desc_4.append(hsn_dict_load[hsn[0:4]]["Desc"][j])
                        lst_4.append(hsn[0:4])
                        rate.append(hsn_dict_load[hsn[0:4]]["Rate"][j])
                        NotificationNo.append(hsn_dict_load[hsn[0:4]]["Notification"][j])
                        Schedule.append(hsn_dict_load[hsn[0:4]]["Schedule No."][j])
                        Schedule_entry.append(hsn_dict_load[hsn[0:4]]["Schedule Entry no."][j])
                        desc_2.append(None)
                        lst_2.append(None)                
                        desc_6.append(None)
                        lst_6.append(None)             
                        desc_8.append(None)
                        lst_8.append(None) 
                except Exception as e:
                    pass                
            elif i==2:
                try:
                    for j in range(int(hsn_dict_load[hsn[0:6]]['Count'])):

                        desc_6.append(hsn_dict_load[hsn[0:6]]["Desc"][j])
                        lst_6.append(hsn[0:6])
                        rate.append(hsn_dict_load[hsn[0:6]]["Rate"][j])
                        NotificationNo.append(hsn_dict_load[hsn[0:6]]["Notification"][j])
                        Schedule.append(hsn_dict_load[hsn[0:6]]["Schedule No."][j])
                        Schedule_entry.append(hsn_dict_load[hsn[0:6]]["Schedule Entry no."][j])
                        desc_4.append(None)
                        lst_4.append(None)                
                        desc_2.append(None)
                        lst_2.append(None)             
                        desc_8.append(None)
                        lst_8.append(None) 
                except Exception as e:
                    pass                          
            elif i==3:
                try:
                    for j in range(int(hsn_dict_load[hsn[0:8]]['Count'])):

                        desc_8.append(hsn_dict_load[hsn[0:8]]["Desc"][j])
                        lst_8.append(hsn[0:8])
                        rate.append(hsn_dict_load[hsn[0:8]]["Rate"][j])
                        NotificationNo.append(hsn_dict_load[hsn[0:8]]["Notification"][j])
                        Schedule.append(hsn_dict_load[hsn[0:8]]["Schedule No."][j])
                        Schedule_entry.append(hsn_dict_load[hsn[0:8]]["Schedule Entry no."][j])
                        desc_4.append(None)
                        lst_4.append(None)                
                        desc_2.append(None)
                        lst_2.append(None)             
                        desc_6.append(None)
                        lst_6.append(None) 
                except Exception as e:
                    pass     

        lst=[len(lst_2),len(lst_4),len(lst_6),len(lst_8)]
        
        max_length=max(lst)
        if max_length >0:
            df["temp"]=pd.Series([x for x in range(max_length)])
        else:
            df["temp"]=pd.Series(["1"])
        
        
        df["two_HSN"]=pd.Series(lst_2)    
        df["two_Desc"]=pd.Series(desc_2)
        df["four_HSN"]=pd.Series(lst_4)
        df["four_Desc"]=pd.Series(desc_4)
        df["six_HSN"]=pd.Series(lst_6)
        df["six_desc"]=pd.Series(desc_6)
        df["eight_HSN"]=pd.Series(lst_8)
        df["eight_Desc"]=pd.Series(desc_8)
        df["Rate"]=pd.Series(rate)
        df["Notification"]=pd.Series(NotificationNo)
        df["Schedule No."]=pd.Series(Schedule)
        df["Schedule Entry no."]=pd.Series(Schedule_entry)
        df["Client HSN"]=hsn
        df["Client Description"]=desc 
        df.drop("temp",axis=1,inplace=True)
        return(df)
    except Exception as e:
        print(traceback.format_exc())
        return(str(e))
def bulk_processing(file,db_path):
    try:
        if db_path:
            hsn_dict_load=load_pickle_file(db_path+"\\"+'hsn_dict.pkl')
        else:

            hsn_dict_load=load_pickle_file(f'hsn_dict.pkl')

        input=pd.read_excel(file)
        lst_df=[]

        for i in range(len(input)):
            if str(type(hsn_to_df(input["HSN"][i],input["Description"][i],db_path)))=="<class 'pandas.core.frame.DataFrame'>":
                lst_df.append(hsn_to_df(str(input["HSN"][i]),input["Description"][i],db_path))
            else:
                print(hsn_to_df(input["HSN"][i],input["Description"][i],db_path),"error") 
        rate_derived=pd.concat(lst_df)
        rate_derived.reset_index(inplace=True,drop=True)
        rate_derived["Sl.No."]=rate_derived.index+1
        rate_derived=rate_derived[["Sl.No.",'Client HSN', 'Client Description','two_HSN', 'two_Desc', 'four_HSN', 'four_Desc', 'six_HSN', 'six_desc',
               'eight_HSN', 'eight_Desc', 'Rate', 'Notification', 'Schedule No.','Schedule Entry no.']]

        #rate_derived.to_excel("rate_derived.xlsx")
        return(rate_derived)          
    except:
        
        print(traceback.format_exc())
   