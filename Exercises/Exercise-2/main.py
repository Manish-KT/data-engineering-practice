import requests
import pandas as pd
from bs4 import BeautifulSoup
import asyncio
import aiohttp
import os

BASE_DIR = "Exercise-2/CSV_Files"


async def download_file(url, session):
    """
    Asynchronously downloads a file from the given URL and saves it to disk.

    Args:
        url (str): The URL of the file to download.
        session (aiohttp.ClientSession): The aiohttp client session.

    """
    async with session.get(url) as response:
        if response.status == 200:
            filename = url.split('/')[-1]
            with open(BASE_DIR + "/" + filename, 'wb') as f:
                while True:
                    chunk = await response.content.read(1024)
                    if not chunk:
                        break
                    f.write(chunk)


async def download_multiple_files(urls):
    """
    Asynchronously downloads multiple files from the given list of URLs.

    Args:
        urls (list): List of URLs to download.

    """
    async with aiohttp.ClientSession() as session:
        tasks = [asyncio.create_task(download_file(url, session)) for url in urls]
        await asyncio.gather(*tasks)


def extract_urls():
    """
    Extracts URLs of files needed based on the specified criteria.

    Returns:
        list: List of URLs of files matching the criteria.

    """
    URL = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
    File_needed = "2024-01-19 10:38"

    data = requests.get(URL).text
    soup = BeautifulSoup(data, "html.parser")

    table = soup.find("table").findAll("tr")

    page_url = []
    # all table that contain this information lies in this range
    for row in table[3:-1]:
        try:
            row_data = row.findAll("td")[1].text.strip()
            if File_needed in row_data and "K" in row.findAll("td")[-2].text:
                page_url.append(URL + row.find("a")["href"])
        except Exception as e:
            print(e)
            print("No data found")

    print("Total files to present: ", len(page_url))
    return page_url


def main():
    """
    Main function to orchestrate the process of extracting URLs, downloading files, and processing data.

    """
    print("Extracting URLs....")
    page_urls = extract_urls()
    print("Extracted all URLs")

    if not os.path.exists(BASE_DIR):
        os.makedirs(BASE_DIR)

    print("Downloading files....")
    asyncio.run(download_multiple_files(page_urls))
    print("Downloaded all files")

    all_files = os.listdir(BASE_DIR)

    for file in all_files:
        try:
            df = pd.read_csv(BASE_DIR + "/" + file)
            print(file)
            print(df[df["HourlyDryBulbTemperature"] == df["HourlyDryBulbTemperature"].max()])
        except Exception as e:
            print(e)
            print("Error in file: ", file)


if __name__ == "__main__":
    main()

