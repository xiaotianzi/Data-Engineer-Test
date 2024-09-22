#!/bin/bash

# Install Azure CLI (for Ubuntu/Debian)
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

# Download blob from Azure storage
az storage blob download --account-name dataengineerv1 --account-key <my_account_key> \
  --container-name wenkui-tian --name Wenkui-Tian/Wenkui-Tian.csv \
  --file ~/result-Wenkui
