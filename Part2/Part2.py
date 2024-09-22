import os
from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network.models import NetworkInterfaceIPConfiguration
from azure.mgmt.storage.models import VirtualNetworkRule

from azure.storage.blob import BlobServiceClient
import pandas as pd


def initialize_clients(subscription_id, resource_group_name, location):
    """
    Initialize clients. Creates a resource group if it doesn't exist.
    """
    credential = DefaultAzureCredential()
    network_client = NetworkManagementClient(credential, subscription_id)
    resource_client = ResourceManagementClient(credential, subscription_id)
    compute_client = ComputeManagementClient(credential, subscription_id)
    storage_client = StorageManagementClient(credential, subscription_id)
    resource_client.resource_groups.create_or_update(
        resource_group_name, {"location": location}
    )
    print(f"Resource group {resource_group_name} created or already exists.")
    return credential, resource_client, network_client, compute_client, storage_client


def create_vnet(network_client, resource_group_name, vnet_name, location):
    """
    Creates a Virtual Network (VNet).
    """
    vnet_params = {
        "location": location,
        "address_space": {"address_prefixes": ["10.0.0.0/16"]},
    }
    vnet = network_client.virtual_networks.begin_create_or_update(
        resource_group_name, vnet_name, vnet_params
    ).result()
    print(f"Virtual Network {vnet_name} created.")
    return vnet


def create_nsg(network_client, resource_group_name, nsg_name, location):
    """
    Creates a Network Security Group (NSG).
    """
    nsg_params = {"location": location}
    nsg = network_client.network_security_groups.begin_create_or_update(
        resource_group_name, nsg_name, nsg_params
    ).result()
    print(f"Network Security Group {nsg_name} created.")
    return nsg


def create_subnet(network_client, resource_group_name, vnet_name, subnet_name, nsg):
    """
    Creates a Subnet within the VNet.
    """
    subnet_params = {
        "address_prefix": "10.0.0.0/24",
        "network_security_group": {"id": nsg.id},
    }
    subnet = network_client.subnets.begin_create_or_update(
        resource_group_name, vnet_name, subnet_name, subnet_params
    ).result()
    print(f"Subnet {subnet_name} created within VNet {vnet_name}.")
    return subnet


def create_public_ip(network_client, resource_group_name, public_ip_name, location):
    # Create Public IP Address
    public_ip_params = {
        "location": location,
        "public_ip_allocation_method": "Dynamic",  # Can be 'Static' or 'Dynamic'
        "sku": {"name": "Basic"},  # Basic or Standard
        "public_ip_address_version": "IPv4",
    }
    public_ip = network_client.public_ip_addresses.begin_create_or_update(
        resource_group_name, public_ip_name, public_ip_params
    ).result()

    print(f"Public IP {public_ip.name} created with address {public_ip.ip_address}.")
    return public_ip


def create_nic(
    network_client, resource_group_name, nic_name, public_ip, location, subnet
):
    """
    Creates a Network Interface Card (NIC) and associates it with the subnet, the public IP and NSG.
    """
    ip_config = NetworkInterfaceIPConfiguration(
        name="ipconfig1",
        subnet={"id": subnet.id},
        private_ip_allocation_method="Dynamic",
        public_ip_address={"id": public_ip.id},
    )
    nic_params = {"location": location, "ip_configurations": [ip_config]}
    nic = network_client.network_interfaces.begin_create_or_update(
        resource_group_name, nic_name, nic_params
    ).result()
    print(
        f"Network Interface Card {nic_name} created and associated with subnet {subnet.name}."
    )
    return nic


def create_vm(
    compute_client,
    resource_group_name,
    vm_name,
    location,
    nic,
    vm_username,
    vm_password,
):
    """
    Creates a Virtual Machine (VM) using the NIC.
    """
    vm_params = {
        "location": location,
        "hardware_profile": {"vm_size": "Standard_B1s"},  # Cheaper VM size
        "storage_profile": {
            "image_reference": {
                "publisher": "Canonical",
                "offer": "0001-com-ubuntu-server-jammy",
                "sku": "22_04-lts-gen2",
                "version": "latest",
            }
        },
        "os_profile": {
            "computer_name": vm_name,
            "admin_username": vm_username,
            "admin_password": vm_password,
        },
        "network_profile": {"network_interfaces": [{"id": nic.id}]},
    }
    vm = compute_client.virtual_machines.begin_create_or_update(
        resource_group_name, vm_name, vm_params
    ).result()
    print(f"Virtual Machine {vm_name} created.")
    return vm


def read_data_from_azure(
    account_url, credential, raw_container_name, blob_name, local_file_path
):
    # Initialize the BlobServiceClient with the storage account URL and credentials
    blob_service_client = BlobServiceClient(
        account_url=account_url, credential=credential
    )
    # Get the container client
    container_client = blob_service_client.get_container_client(raw_container_name)

    # Get the blob client (the CSV file)
    blob_client = container_client.get_blob_client(blob_name)

    # Download the blob to a local file
    with open(local_file_path, "wb") as file:
        blob_data = blob_client.download_blob()
        blob_data.readinto(file)

    print(f"Blob downloaded to {local_file_path}")

    # Load the downloaded CSV file into a Pandas DataFrame
    df = pd.read_csv(local_file_path)
    return df


def analyze_df(df):
    # Group the data by 'Country' and calculate the average 'Rating'
    country_avg_rate = df.groupby("Country")["Rating"].mean().reset_index()

    # Sort by 'Rating' to display the results in descending order
    country_avg_rate = country_avg_rate.sort_values(by="Rating", ascending=False)

    # Display the result
    print("Average Rating per Country:")
    print(country_avg_rate)

    # Equivalent SQL Query:
    # SELECT Country, AVG(Rating) as average_rating
    # FROM tourism_dataset
    # GROUP BY Country
    # ORDER BY average_rating DESC;

    # Group the data by 'Category' and calculate the average 'Rating'
    category_avg_rate = df.groupby("Category")["Rating"].mean().reset_index()

    # Sort the categories by average 'Rating' in descending order and select the top 3
    top_3_categories = category_avg_rate.sort_values(by="Rating", ascending=False).head(
        3
    )

    # Display the top 3 categories
    print("\nTop 3 Categories by Average Rating:")
    print(top_3_categories)

    # Equivalent SQL Query:
    # SELECT Category, AVG(Rating) as average_rating
    # FROM tourism_dataset
    # GROUP BY Category
    # ORDER BY average_rating DESC
    # LIMIT 3;

    return country_avg_rate, top_3_categories


def save_result_to_csv(country_avg_rate, top_3_categories, result_file_name):
    # Saving the results to CSV file
    # Step 1: Save the first DataFrame with its header
    with open(result_file_name, "w", newline="") as f:
        # Write country_avg_rate DataFrame with its own header
        country_avg_rate.to_csv(f, index=False)

        # Step 2: Write an empty line to separate the sections
        f.write("\n")

        # Step 3: Manually write the "Top 3 Categories" header
        f.write("Top 3 Categories,Rating\n")

        # Step 4: Write the top_3_categories DataFrame without a header (header written manually)
        top_3_categories.to_csv(f, header=False, index=False)

    print(f"Concatenated DataFrame with separate headers saved as '{result_file_name}'")


def save_file_to_azure_storage(
    account_url, credential, directory_name, user_container_name, result_file_name
):
    # Define the directory in the Azure storage account
    blob_service_client = BlobServiceClient(
        account_url=account_url, credential=credential
    )

    # Create a new container or directory (if needed, use a container client if required)
    container_client = blob_service_client.get_container_client(user_container_name)

    # Upload the CSV files to Azure Blob Storage
    def upload_to_azure(file_name, blob_name):
        blob_client = container_client.get_blob_client(f"{directory_name}/{blob_name}")

        with open(file_name, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        print(f"{blob_name} uploaded to Azure Storage under {directory_name}/")

    # Upload the CSV files
    upload_to_azure(result_file_name, result_file_name)


def add_service_endpoint_and_configure_storage_networking(
    storage_client,
    network_client,
    resource_group_name,
    storage_account_name,
    vnet,
    subnet,
):
    """
    Adds a Microsoft.Storage service endpoint to the specified subnet and configures the storage account
    to allow access from that subnet, preserving existing rules.
    """
    # Check if Microsoft.Storage service endpoint is already configured
    service_endpoints = (
        [se.service for se in subnet.service_endpoints]
        if subnet.service_endpoints
        else []
    )

    if "Microsoft.Storage" not in service_endpoints:
        # Add Microsoft.Storage service endpoint to the subnet
        print(f"Adding Microsoft.Storage service endpoint to subnet {subnet.name}")
        subnet.service_endpoints = subnet.service_endpoints or []
        subnet.service_endpoints.append(
            {
                "service": "Microsoft.Storage",
                "locations": [
                    vnet.location
                ],  # Add the storage endpoint for the region of the subnet
            }
        )

        # Update the subnet with the new service endpoint
        subnet = network_client.subnets.begin_create_or_update(
            resource_group_name, vnet.name, subnet.name, subnet
        ).result()

        print(f"Microsoft.Storage service endpoint added to subnet {subnet.name}")
    else:
        print(
            f"Microsoft.Storage service endpoint already configured on subnet {subnet.name}"
        )

    # Create the new Virtual Network Rule
    new_vnet_rule = VirtualNetworkRule(
        virtual_network_resource_id=subnet.id, action="Allow"  # Subnet ID
    )

    # Get the existing network rules for the storage account
    storage_account = storage_client.storage_accounts.get_properties(
        resource_group_name, storage_account_name
    )
    existing_vnet_rules = storage_account.network_rule_set.virtual_network_rules or []

    # Check if the rule for this VNet already exists to avoid duplicates
    if not any(
        rule.virtual_network_resource_id == new_vnet_rule.virtual_network_resource_id
        for rule in existing_vnet_rules
    ):
        # Add the new rule to the existing rules
        existing_vnet_rules.append(new_vnet_rule)
        print(f"Adding VNet rule for {vnet.name}")

        # Update storage account network rules, preserving the existing ones
        storage_client.storage_accounts.update(
            resource_group_name=resource_group_name,
            account_name=storage_account_name,
            parameters={
                "network_rule_set": {
                    "virtual_network_rules": existing_vnet_rules,
                    "bypass": "AzureServices",  # Preserve the bypass rule for Azure services
                    "default_action": "Deny",  # Deny access by default except from the allowed VNets
                }
            },
        )
        print(
            f"Storage account {storage_account_name} network rules updated to allow access from {vnet.name}"
        )
    else:
        print(
            f"VNet rule for {vnet.name} already exists in storage account {storage_account_name}"
        )


if __name__ == "__main__":
    load_dotenv()
    # Step 1: Deploy a Virtual Machine (VM)
    # Load environment variables
    subscription_id = os.getenv("SUBSCRIPTION_ID")
    resource_group_name = os.getenv("RESOURCE_GROUP_NAME")
    vnet_name = os.getenv("VNET_NAME")
    location = os.getenv("LOCATION")
    nsg_name = os.getenv("NSG_NAME")
    subnet_name = os.getenv("SUBNET_NAME")
    public_ip_name = os.getenv("PUBLIC_IP_NAME")
    nic_name = os.getenv("NIC_NAME")
    vm_name = os.getenv("VM_NAME")
    vm_username = os.getenv("VM_USERNAME")
    vm_password = os.getenv("VM_PASSWORD")

    ## 1: Initilize clients
    credential, resource_client, network_client, compute_client, storage_client = (
        initialize_clients(subscription_id, resource_group_name, location)
    )

    ## 2: Create a Virtual Network (VNet)
    vnet = create_vnet(network_client, resource_group_name, vnet_name, location)

    ## 3: Create a Network Security Group (NSG)
    nsg = create_nsg(network_client, resource_group_name, nsg_name, location)

    ## 4: Create a Subnet
    subnet = create_subnet(
        network_client, resource_group_name, vnet_name, subnet_name, nsg
    )

    ## 5: Create a Public IP
    public_ip = create_public_ip(
        network_client, resource_group_name, public_ip_name, location
    )

    ## 6: Create a Network Interface Card (NIC)
    nic = create_nic(
        network_client, resource_group_name, nic_name, public_ip, location, subnet
    )

    ## 7: Create a Virtual Machine (VM)
    vm = create_vm(
        compute_client,
        resource_group_name,
        vm_name,
        location,
        nic,
        vm_username,
        vm_password,
    )

    # Step 2: Read Data from Azure Storage Account
    account_url = os.getenv("ACCOUNT_URL")
    raw_container_name = os.getenv("RAW_CONTAINER_NAME")
    blob_name = os.getenv("BLOB_NAME")
    local_file_path = os.getenv("LOCAL_FILE_PATH")

    df = read_data_from_azure(
        account_url, credential, raw_container_name, blob_name, local_file_path
    )
    print(df.head())

    # Step 3: Perform Data Analysis
    country_avg_rate, top_3_categories = analyze_df(df)

    # Step 4: Export Results and Save to Azure Storage, Configure Networking
    user_container_name = os.getenv("USER_CONTAINER_NAME")
    result_file_name = os.getenv("RESULT_FILE_NAME")
    directory_name = os.getenv("DIRECTORY_NAME")
    storage_account_name = os.getenv("STORAGE_ACCOUNT_NAME")
    save_result_to_csv(country_avg_rate, top_3_categories, result_file_name)
    save_file_to_azure_storage(
        account_url, credential, directory_name, user_container_name, result_file_name
    )
    add_service_endpoint_and_configure_storage_networking(
        storage_client,
        network_client,
        resource_group_name,
        storage_account_name,
        vnet,
        subnet,
    )
