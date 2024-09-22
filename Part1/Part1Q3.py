from azure.storage.blob import BlobServiceClient
import os
from dotenv import load_dotenv

load_dotenv()

# Retrieve values from environment variables
connection_string = os.getenv("AZURE_CONNECTION_STRING")
container_name = os.getenv("AZURE_CONTAINER_NAME")
file_path = os.getenv("AZURE_FILE_PATH")
blob_name = os.path.basename(file_path)

# Initialize the BlobServiceClient
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Check if the container exists, create if it doesn't
container_client = blob_service_client.get_container_client(container_name)
if not container_client.exists():
    container_client.create_container()
    print(f'New container "{container_name}" created.')

# Upload the file
blob_client = blob_service_client.get_blob_client(
    container=container_name, blob=blob_name
)

with open(file_path, "rb") as data:
    blob_client.upload_blob(data, overwrite=True)

print(f"File {file_path} uploaded to container {container_name} as blob {blob_name}.")
