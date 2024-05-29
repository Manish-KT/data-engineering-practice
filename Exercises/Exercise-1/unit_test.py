import os
import unittest
import asyncio
from unittest.mock import patch
import aiohttp
from main import download_and_extract, download_uris

# The test case below is an example of how to test the download_and_extract function.

test_folder = "./Exercises/Exercise-1/test_downloads/" 

class TestDownloadAndExtract(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.download_uris = download_uris
        self.download_folder = test_folder

    async def test_download_and_extract(self):
        async with aiohttp.ClientSession() as session:
            for uri in self.download_uris:
                filename = f"{self.download_folder}{uri.split('/')[-1]}"
                with self.subTest(uri=uri):
                    await download_and_extract(session, uri, filename)
                    self.assertTrue(os.path.exists(filename))
    
    async def asyncSetUp(self):
        if not os.path.exists(self.download_folder):
            os.makedirs(self.download_folder)

    async def asyncTearDown(self):
        if os.path.exists(self.download_folder):
            for file in os.listdir(self.download_folder):
                file_path = os.path.join(self.download_folder, file)
                os.remove(file_path)
            os.rmdir(self.download_folder)

if __name__ == "__main__":
    unittest.main()
