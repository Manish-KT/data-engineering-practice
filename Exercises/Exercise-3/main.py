import boto3
import gzip
import os

def main():
    # Create a session using the UNSIGNED config
    s3 = boto3.resource('s3')

    # Define bucket and key
    bucket_name = 'commoncrawl'
    key = 'crawl-data/CC-MAIN-2022-05/wet.paths.gz'

    # Temporary download location
    temp_file_loc = "/tmp/"

    # Download the file to a temporary location
    s3.Object(bucket_name, key).download_file(temp_file_loc + 'wet.paths.gz')

    # Extract the .gz file
    with gzip.open(temp_file_loc + 'wet.paths.gz', 'rb') as f_in:
       file_content = f_in.read().decode('utf-8')

    # Write the extracted content to a text file
    with open(temp_file_loc + 'zipfileContent.txt', 'w') as f_out:
        f_out.write(file_content)

    # Get the first line from the extracted content
    first_line = file_content.splitlines()[0]
    print("First line:", first_line)

    # Download the first file based on the first line
    s3.Object(bucket_name, first_line).download_file(temp_file_loc + 'first_file.warc.wet.gz')

    # Extract and print the first few lines of the downloaded file
    with gzip.open(temp_file_loc + 'first_file.warc.wet.gz', 'rb') as f_in:
       for i, line in enumerate(f_in, start=1):
           print(line.decode('utf-8').strip())
           if i >= 5:
               break

if __name__ == "__main__":
    main()
