# -*- coding: utf-8 -*-
"""
Created on Sun Oct 13 15:37:08 2019

@author: hewi
"""

#### IF error : "ModuleNotFOundError: no module named PyPDF2"
   # then uncomment line below (i.e. remove the #):
       
#pip install PyPDF2

import pandas as pd
import os
import glob
import asyncio
import aiohttp
import aiofiles
from tqdm import tqdm
from modules.download_pdf import download_pdf
from modules.download_byte import download_byte


###!!NB!! column with URL's should be called: "Report Html Address" and the year should be in column named: "Pub_Year"

### File names will be the ID from the ID column (e.g. BR2005.pdf)

########## EDIT HERE:
    
### specify path to file containing the URLs
list_pth = 'C:\\Users\\A-SPAC05\\Desktop\\Projects\\Week4\\PDF_Downloader\\Data\\GRI_2017_2020.xlsx'

###specify Output folder (in this case it moves one folder up and saves in the script output folder)
pth = 'C:\\Users\\A-SPAC05\\Desktop\\Projects\\Week4\\PDF_Downloader\\Output\\'

###Specify path for existing downloads
dwn_pth = 'C:\\Users\\A-SPAC05\\Desktop\\Projects\\Week4\\PDF_Downloader\\Output\\dwn'
if not os.path.exists(dwn_pth):
    os.makedirs(dwn_pth)

### cheack for files already downloaded
dwn_files = glob.glob(os.path.join(dwn_pth, "*.pdf")) 
exist = [os.path.basename(f)[:-4] for f in dwn_files]

###specify the ID column name
ID = "BRnum"


##########

async def read_excel(file_path, index_col):
    async with aiofiles.open(file_path, mode='rb') as f:
        content = await f.read()
    return pd.read_excel(content, sheet_name=0, index_col=index_col)




async def download_task(session, pth, df, j):
    result = await download_byte(session, pth, df, j)
    return j, result

async def main():
    ### read in file
    df = pd.read_excel(list_pth, sheet_name=0, index_col=ID)

    ### filter out rows that have been downloaded
    df = df[~df.index.isin(exist)]


    connector = aiohttp.TCPConnector()
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [download_task(session, pth, df, j) for j in df.index]
        for future in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
            try:
                j, result = await future
                if result is not None:
                    with open(pth+'download_results.csv', 'a') as f:
                        f.write(f"{j},{result}\n")
                #print(f"{downloads} Downloaded {future}")
                
            except Exception as e:
                pass
                #print(f"Error downloading {future}: {e}")



if __name__ == "__main__":
    asyncio.run(main())

