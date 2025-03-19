import aiohttp
import os
import PyPDF2


async def download_byte(session, pth, df, j, backup=False):
    savefile = str(pth + "dwn/" + str(j) + '.pdf')

    if backup:
        url = "Report Html Address"
    else:
        url = "Pdf_URL"

    try:
        async with session.get(df.at[j, url]) as response:
            response.raise_for_status()
            with open(savefile, 'wb') as f:
                chunk = await response.content.read(1)
                f.write(chunk)
                return "yes"

            
    except (ConnectionResetError, Exception) as e:
        if os.path.isfile(savefile):
            os.remove(savefile)
        if not backup:
            return await download_byte(session, pth, df, j, backup=True)
        else:
            return "no"

