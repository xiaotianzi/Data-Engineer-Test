from azure.identity import DefaultAzureCredential
from azure.mgmt.monitor import MonitorManagementClient
from datetime import datetime, timedelta
import csv
from dotenv import load_dotenv
import os

load_dotenv()

# Initialize credentials and MonitorManagementClient
credential = DefaultAzureCredential()
subscription_id = os.getenv(
    "SUBSCRIPTION_ID"
)  # Replace with your Azure subscription ID
monitor_client = MonitorManagementClient(credential, subscription_id)

# Define the time range for the Activity Logs (e.g., last 1 day)
start_time = datetime.now() - timedelta(days=1)
end_time = datetime.now()

# Define the resource you want to filter (e.g., specific resource name or resource ID)
resource_id = os.getenv("RESOURCE_ID")

# Query Activity Logs for the specific resource
activity_logs = monitor_client.activity_logs.list(
    filter=f"eventTimestamp ge '{start_time}' and eventTimestamp le '{end_time}' and resourceId eq '{resource_id}')"
)

# Define a CSV file to store the logs
csv_file = "activity_logs.csv"

# Write the logs to a CSV file
with open(csv_file, mode="w", newline="") as file:
    writer = csv.writer(file)
    # Write header row to match the format you showed
    writer.writerow(
        [
            "Correlation id",
            "Operation name",
            "Status",
            "Event category",
            "Level",
            "Time",
            "Subscription",
            "Event initiated by",
            "Resource type",
            "Resource group",
            "Resource",
        ]
    )

    # Write log data
    for log in activity_logs:
        writer.writerow(
            [
                log.correlation_id,
                log.operation_name.localized_value if log.operation_name else None,
                log.status.localized_value if log.status else None,
                log.category.value if log.category else None,
                log.level if log.level else None,
                log.event_timestamp.isoformat() if log.event_timestamp else None,
                log.subscription_id,
                log.caller,
                log.resource_type.value if log.resource_type else None,
                log.resource_group_name,
                log.resource_id,
            ]
        )

print(f"Activity logs saved to {csv_file}")
