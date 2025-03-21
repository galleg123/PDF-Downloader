import aiohttp
import os
from urllib.parse import urlparse
import csv
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type


async def download_byte(session, pth, df, j, backup=False):
    savefile = str(pth + "dwn/" + str(j) + '.pdf')
    status_log_file = str(pth + 'status_codes.csv')  # File to log status codes

    # Choose URL to use
    if backup:
        url = "Report Html Address"
    else:
        url = "Pdf_URL"

    try:
        # Check if there is an URL
        if not isinstance(df.at[j, url], str):
            raise Exception(f"No URL at {url}")

        # Parse the url to check if it has a scheme
        parsed_url = urlparse(df.at[j, url])
        if not parsed_url.scheme:
            for scheme in ["https://", "http://"]:
                try:
                    print(f'Trying with: {scheme + df.at[j, url]}')
                    status_code = await get_pdf(session, scheme + df.at[j, url], savefile,status_log_file)
                    return "yes"
                except Exception as e:
                    continue
        else:
            status_code = await get_pdf(session, df.at[j, url], savefile, status_log_file)

            return "yes"

        raise Exception(f"Could not download {df.at[j, url]}")

    # Try the other URL if the first one fails
    except (ConnectionResetError, Exception) as e:
        if os.path.isfile(savefile):
            os.remove(savefile)
        if not backup:
            return await download_byte(session, pth, df, j, backup=True)
        else:
            return "no"

# Retry wrapper to download from the URL with a maximum of 3 attempts
@retry(stop= stop_after_attempt(3), wait=wait_fixed(2), retry=retry_if_exception_type(aiohttp.ClientError))
async def get_pdf(session: aiohttp.ClientSession, url, savefile, status_file='status_codes.csv'):
    async with session.get(url, raise_for_status=True) as response:
        status_code = response.status  # Capture the status code
        #log_status(status_file, url, status_code)

        if response.headers.get('Content-Type') != 'application/pdf':
            raise Exception(f"Content-Type is not PDF: {response.headers.get('Content-Type')}")
        with open(savefile, 'wb') as f:
            chunk = await response.read()  # Read only 1B for verification
            f.write(chunk)
        return status_code


# Logging function.
def log_status(file_path, url, status):
    """Log the URL and status code to a CSV file."""
    with open(file_path, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([url, status])