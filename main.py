import base64
import tempfile

import streamlit as st
from pdf2image import convert_from_path

from pathlib import Path
import executor
import pandas as pd
from datetime import datetime
import os
import glob
import sales_tax_extarctor
import aggregator
import service_aggregator
import service_executor




def main():
    """Streamlit application
    """

    st.title("GST Rate Finder")
    st.sidebar.title("Parameter Selectors")
    st.sidebar.markdown("Select the Period")
    database_path=os.path.join(os.getcwd()+"//"+"DB")
    folder_lst=list(os.listdir(database_path))

    folder_lst.append("Real Time")






    select = st.sidebar.selectbox('Select the Database',folder_lst)
    if select!="Real Time":
        if os.path.exists(database_path+"//"+select):
            db_path=database_path+"//"+str(select)
            pass
        else:
            
            sales_tax_extarctor.main()
            db_path=database_path+"//"+str(select)
            aggregator.main(db_path)
            service_aggregator.main(db_path)

    else :
        db_path=None
        with st.form(key='Run Real Time'):
            
            submit_button_1 = st.form_submit_button(label='Extract in Real Time') 
        if submit_button_1:
            sales_tax_extarctor.main()
            db_path=None
            aggregator.main(db_path)
            service_aggregator.main(db_path)
    st.sidebar.markdown("Select the Type")
    select_1 = st.sidebar.selectbox('Select Type',["Goods","Services"]) 
    df_single=pd.DataFrame()   
    with st.form(key='Individual HSN Search'):
        text_input = st.text_input(label='Search HSN')
        submit_button = st.form_submit_button(label='Submit') 
        
        if submit_button:
            if select_1=="Goods":
                
                df=executor.hsn_to_df(int(text_input),None,db_path)
                df.drop(["Client HSN","Client Description"],axis=1,inplace=True)
                st.dataframe(df)
                df_single=df
                # dt_string = now.strftime("_%d_%m_%Y_%H%M%S")
                # downloadfolder=os.path.join(os.path.join(os.environ['USERPROFILE']), 'Downloads')
                # OutPutFileName = "\"+"Goods_"+text_input+"_"+dt_string+".xlsx"      
                # df.to_csv(downloadfolder+OutPutFileName,index=None)
                # with open(downloadfolder+OutPutFileName) as f:
                #     file=f.read()

                # if len(df)>0:
                #     st.download_button("Download Search Result", file, file_name="Goods_"+text_input+"_"+dt_string+".csv", mime=None, key=None)                
            else:

                df=service_executor.hsn_to_df(int(text_input),None,db_path)
                df.drop(["Client HSN","Client Description"],axis=1,inplace=True)
                st.dataframe(df)
                df_single=df                
    now = datetime.now()
    dt_string = now.strftime("_%d_%m_%Y_%H%M%S")
    downloadfolder=os.getcwd()
    OutPutFileName = "//"+text_input+"_"+dt_string+".xlsx"      
    df_single.to_csv(downloadfolder+OutPutFileName,index=None)
    with open(downloadfolder+OutPutFileName) as f:
        file=f.read()

    if len(df_single)>0:
        st.download_button("Download Search Result", file, file_name=text_input+"_"+dt_string+".csv", mime=None, key=None)            #st.table(datatable)




    uploaded_file = st.file_uploader("Choose your .xlsx file", type=".xlsx")
    if uploaded_file:
        now = datetime.now()
        if select_1=="Goods":
            rate_derived=executor.bulk_processing(uploaded_file.name,db_path)
            dt_string = now.strftime("_%d_%m_%Y_%H%M%S")
            downloadfolder=os.path.join(os.path.join(os.environ['USERPROFILE']), 'Downloads')
            OutPutFileName = "//"+"Goods_Bulk_upload_rate_derived"+"_"+dt_string+".xlsx"      
            rate_derived.to_csv(downloadfolder+OutPutFileName,index=None)
            with open(downloadfolder+OutPutFileName) as f:
                file=f.read()

            if len(rate_derived)>0:
                st.download_button("Download rate_derived file", file, file_name='Bulk_upload_rate_derived.csv', mime=None, key=None)
        else:

            rate_derived=service_executor.bulk_processing(uploaded_file.name,db_path)
            dt_string = now.strftime("_%d_%m_%Y_%H%M%S")
            downloadfolder=os.getcwd()
            OutPutFileName = "//"+"Service_Bulk_upload_rate_derived"+"_"+dt_string+".xlsx"      
            rate_derived.to_csv(downloadfolder+OutPutFileName,index=None)
            with open(downloadfolder+OutPutFileName) as f:
                file=f.read()

            if len(rate_derived)>0:
                st.download_button("Download rate_derived file", file, file_name='Bulk_upload_rate_derived.csv', mime=None, key=None)

if __name__ == "__main__":
    main()
