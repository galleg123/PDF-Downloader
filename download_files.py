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
import ssl
import certifi
from aiohttp.cookiejar import CookieJar


###!!NB!! column with URL's should be called: "Report Html Address" and the year should be in column named: "Pub_Year"

### File names will be the ID from the ID column (e.g. BR2005.pdf)

########## EDIT HERE:
    
### specify path to file containing the URLs
# list_pth = 'C:\\Users\\A-SPAC05\\Desktop\\Projects\\Week4\\PDF_Downloader\\Data\\GRI_2017_2020.xlsx'
list_pth = 'C:\\Users\\A-SPAC05\\Desktop\\Projects\\Week4\\PDF_Downloader\\Data\\Copy of GRI_2017_2020_Short.xlsx'


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


# Function to read the excel file
async def read_excel(file_path, index_col):
    async with aiofiles.open(file_path, mode='rb') as f:
        content = await f.read()
    return pd.read_excel(content, sheet_name=0, index_col=index_col)



# The download task that will be queued up in the event loop
async def download_task(session, pth, df, j):
    result = await download_byte(session, pth, df, j)
    return j, result


async def main():
    # read in file
    df = pd.read_excel(list_pth, sheet_name=0, index_col=ID)

    # filter out rows that have been downloaded
    df = df[~df.index.isin(exist)]

    # Filter out rows that do not have a URL
    df = df[df["Pdf_URL"].apply(lambda x: isinstance(x, str)) | df["Report Html Address"].apply(lambda x: isinstance(x, str))]

    ssl_context = ssl.create_default_context(cafile=certifi.where(), purpose=ssl.Purpose.SERVER_AUTH)

    # Settings for the aiohttp Client session
    connector = aiohttp.TCPConnector(ssl=ssl_context, limit=100, force_close=True, enable_cleanup_closed=True, cookie_jar=CookieJar())
    async with aiohttp.ClientSession(connector=connector,  timeout=aiohttp.ClientTimeout(total=10)) as session:
        # Queue up the tasks in the event loop
        tasks = [download_task(session, pth, df, j) for j in df.index]

        # Wait for the tasks to complete and write the results to a file
        for future in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
            try:
                j, result = await future
                if result is not None:
                    with open(pth+'download_results.csv', 'a') as f:
                        f.write(f"{j},{result}\n")
            except:
                pass



if __name__ == "__main__":
    asyncio.run(main())

