import base64
import tempfile

import streamlit as st
from pdf2image import convert_from_path

from pathlib import Path
import executor
import pandas as pd
from datetime import datetime
import os




def main():
    """Streamlit application
    """

    st.title("GST Rate Finder")
    st.sidebar.title("Parameter Selectors")
    st.sidebar.markdown("Select the Period")
    #database_path=os.path.join(os.getcwd()+"DB")




    select = st.sidebar.selectbox('Select the Database',["Real Time","Last Day of previous Month"])
    st.sidebar.markdown("Select the Type")
    select_1 = st.sidebar.selectbox('Select Type',["Goods","Services"])    
    with st.form(key='Individual HSN Search'):
        text_input = st.text_input(label='Search HSN')
        submit_button = st.form_submit_button(label='Submit') 
        if submit_button:
            df=executor.hsn_to_df(int(text_input),None)
            df.drop(["Client HSN","Client Description"],axis=1,inplace=True)
            st.dataframe(df)
            #st.table(datatable)




    uploaded_file = st.file_uploader("Choose your .xlsx file", type=".xlsx")
    if uploaded_file:
        now = datetime.now()
        rate_derived=executor.bulk_processing(uploaded_file.name)
        dt_string = now.strftime("_%d_%m_%Y_%H%M%S")
        downloadfolder=os.path.join(os.path.join(os.environ['USERPROFILE']), 'Downloads')
        OutPutFileName = "\\"+"Bulk_upload_rate_derived"+"_"+dt_string+".xlsx"      
        rate_derived.to_csv(downloadfolder+OutPutFileName,index=None)
        with open(downloadfolder+OutPutFileName) as f:
            file=f.read()

        if len(rate_derived)>0:
            st.download_button("Download rate_derived file", file, file_name='Bulk_upload_rate_derived.csv', mime=None, key=None)
        # if uploaded_file is not None:
        #     # Make temp file path from uploaded file
        #     with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        #         st.markdown("## Original PDF file")
        #         fp = Path(tmp_file.name)
        #         fp.write_bytes(uploaded_file.getvalue())
        #         st.write(show_pdf(tmp_file.name))

        #         imgs = convert_from_path(tmp_file.name)

        #         st.markdown(f"Converted images from PDF")
        #         st.image(imgs)


if __name__ == "__main__":
    main()