import os
import zipfile
import asyncio
import aiohttp
import glob

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

download_folder = "./Exercises/Exercise-1/downloads/"

async def download_and_extract(session, url, filename):
    print(f"Downloading {filename}...")
    try:
        async with session.get(url) as response:
            with open(filename, 'wb') as f:
                while True:
                    chunk = await response.content.read(1024)
                    if not chunk:
                        break
                    f.write(chunk)
            
            with zipfile.ZipFile(filename, "r") as zip_ref:
                zip_ref.extractall(download_folder)
            
            os.remove(filename)
            print(f"Downloaded {filename}")

            # Remove non-CSV files
            csv_files = glob.glob(f"{download_folder}*.csv")
            for file in glob.glob(f"{download_folder}*"):
                if file not in csv_files:
                    os.remove(file)
                    print(f"Removed {file}")

            return filename
    except Exception as e:
        print(f"Error downloading {filename}: {e}")


async def main_async():
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)

    async with aiohttp.ClientSession() as session:
        tasks = [download_and_extract(session, 
                                      uri, 
                                      f"{download_folder}{uri.split('/')[-1]}") for uri in download_uris]
        downloaded_files = await asyncio.gather(*tasks)

    print("All files downloaded:")
    for filename in downloaded_files:
        print(filename)


if __name__ == "__main__":
    # Execute the main_async function
    asyncio.run(main_async())
