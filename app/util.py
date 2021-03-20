# util.py
# Python program to bulk download blob files from azure storage
# Uses latest python SDK() for Azure blob storage
# Requires python 3.6 or above
import os
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.storage.blob import ContentSettings, ContainerClient

class AzureBlobFileDownloader:
  def __init__(self, conn_string, blob_container):
    print("Intializing AzureBlobFileDownloader")
 
    # Initialize the connection to Azure storage account
    self.blob_service_client =  BlobServiceClient.from_connection_string(conn_string)
    self.my_container = self.blob_service_client.get_container_client(blob_container)
 
 
  def save_blob(self, file_name, file_content, local_path):
    # Get full path to the file
    download_file_path = os.path.join(local_path, file_name)
 
    # for nested blobs, create local path as well!
    os.makedirs(os.path.dirname(download_file_path), exist_ok=True)
 
    with open(download_file_path, "wb") as file:
      file.write(file_content)
 
  def download_all_blobs_in_container(self, local_path):
    my_blobs = self.my_container.list_blobs()
    for blob in my_blobs:
      print(blob.name)
      bytes = self.my_container.get_blob_client(blob).download_blob().readall()
      self.save_blob(blob.name, bytes, local_path)