# README

This repository contains the answer for Part1 and scripts to automate the following tasks using Python and shell scripts to finish the Part2 tasks:

1. **Step 1**: Deploy a Virtual Machine (VM) in Azure.
2. **Step 2**: Read data from an Azure Storage Account.
3. **Step 3**: Perform data analysis on the downloaded data.
4. **Step 4**: Export the results, save them back to Azure Storage, and download them to the VM.

## Prerequisites

1. Azure CLI:

    - Ensure the Azure CLI is installed on computer. Download it from [here](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli)
    - After installation, log in to Azure account by running:

        ```
        az login
        ```

2. Python Packages:

    - Before running the Python scripts, install the required Python packages. Run the following command in your terminal:

        ```
        pip install azure-identity azure-mgmt-network azure-mgmt-resource azure-mgmt-storage azure-mgmt-compute azure-storage-blob pandas python-dotenv
        ```

3. Environment Variables:

    - This project uses a `.env` file to manage sensitive information like Azure credentials, resource group names, and other variables.
    - Update the `.env` file with the appropriate values. Replace the following placeholders with actual values:

        ```
        SUBSCRIPTION_ID=my_subscription_id
        RESOURCE_GROUP_NAME=my_resource_group_name
        VM_USERNAME=my_vm_username
        VM_PASSWORD=my_vm_password
        ACCOUNT_URL=my_account_url
        STORAGE_ACCOUNT_NAME=my_storage_account_name
        VNET_NAME=VNet-Wenkui
        LOCATION=westeurope
        NSG_NAME=NSG-Wenkui
        SUBNET_NAME=Subnet-Wenkui
        PUBLIC_IP_NAME=PublicIP-Wenkui
        NIC_NAME=NIC-Wenkui
        VM_NAME=VM-Wenkui
        RAW_CONTAINER_NAME=raw
        BLOB_NAME=tourism_dataset.csv
        LOCAL_FILE_PATH=./tourism_dataset.csv
        USER_CONTAINER_NAME=wenkui-tian
        RESULT_FILE_NAME=Wenkui-Tian.csv
        DIRECTORY_NAME=Wenkui-Tian
        ```

## Steps to Execute

### Step 1: Deploy a Virtual Machine (VM)

- To deploy the virtual machine and set up networking, run the `Part2.py` script. This script automates the creation of the VM, network interfaces, and storage resources.

    ```
    python Part2.py
    ```

- Make sure all variables in the `.env` file are set correctly before running the script.

### Step 2: Read Data from Azure Storage Account

- The script `Part2.py` automatically reads data from the specified Azure Storage Account and stores it locally for analysis.

### Step 3: Perform Data Analysis

- Through the `Part2.py`, data is loaded into a pandas DataFrame where analysis is performed (such as calculating averages and identifying top categories).

### Step 4: Export Results and Save to VM

- Through the `Part2.py`, the results of the analysis are saved as CSV files, which are also uploaded back to Azure Storage.

- After the analysis results are exported back to Azure Storage, need to download the results to the virtual machine.

- First, generate and configure a new SSH key pair by going to **Virtual Machine** > **Help** > **Reset Password**.

- Download the private key file, and use the following command to connect to your VM via SSH:

    ```
    ssh -i path/to/private/key <username>@<vm-ip>
    ```

- After connecting to the VM, update the `my_account_key` in the `download_blob.sh` script to your actual storage account key.

- Execute the commands from the shell script in the VM:

    ```
    sh download_blob.sh
    ```

- This will complete the final step by downloading the analysis results from the Azure Storage account to the VM.
