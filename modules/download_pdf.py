import aiohttp
import os
import PyPDF2


async def download_pdf(session, pth, df, j, backup=False):
    savefile = str(pth + "dwn/" + str(j) + '.pdf')

    if backup:
        url = "Report Html Address"
    else:
        url = "Pdf_URL"

    try:
        async with session.get(df.at[j, url]) as response:
            if response.status == 200:
                with open(savefile, 'wb') as f:
                    f.write(await response.read())
            else:
                raise Exception(response.status)
        if os.path.isfile(savefile):
            try:
                # creating a pdf reader object
                with open(savefile, 'rb') as pdfFileObj:
                    pdfReader = PyPDF2.PdfReader(pdfFileObj)
                    if len(pdfReader.pages) > 0:
                        df.at[j, 'pdf_downloaded'] = "yes"
                        return "yes"
                    else:
                        pdfFileObj.close()
                        if os.path.isfile(savefile):
                            os.remove(savefile)
                        if not backup:
                            await download_pdf(session, pth, df, j, backup=True)
                        else:
                            df.at[j, 'pdf_downloaded'] = "file_error"
                            return "file_error"
            except Exception as e:
                os.remove(savefile)
                if not backup:
                    await download_pdf(session, pth, df, j, backup=True)
                else:
                    df.at[j, 'pdf_downloaded'] = str(e)
                    return str(e)
                    #print(str(str(j) + " " + str(e)))
        else:
            os.remove(savefile)
            if not backup:
                await download_pdf(session, pth, df, j, backup=True)
            else:
                df.at[j, 'pdf_downloaded'] = "404"
                return "404"
    except (ConnectionResetError, Exception) as e:
        os.remove(savefile)
        if not backup:
            await download_pdf(session, pth, df, j, backup=True)
        else:
            df.at[j, "error"] = str(e)
            return "Error"

