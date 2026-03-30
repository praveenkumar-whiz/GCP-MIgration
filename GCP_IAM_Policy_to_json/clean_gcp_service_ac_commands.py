import pandas as pd
import re

input_file = "cleaned_gcp_labs.xlsx"
output_file = "final_cleaned_gcp_labs_v2.xlsx"

print(f"\nLoading file: {input_file}\n")

df = pd.read_excel(input_file)

rows_processed = 0
service_removed = 0
policy_removed = 0
timestamp_removed = 0

cleaned_rows = []

for index, row in df.iterrows():

    lab_id = row["lab_id"]
    other_data = str(row["other_data"])

    # ------------------------------------------------
    # 1 Remove starting numbers (1 2 3 etc)
    # ------------------------------------------------
    other_data = re.sub(r'^\s*\d+\s+', '', other_data)

    # ------------------------------------------------
    # 2 Remove trailing timestamp metadata
    # pattern: number + datetime + datetime
    # ------------------------------------------------
    timestamp_pattern = r'\d+\s+\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\s+\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}'
    
    if re.search(timestamp_pattern, other_data):
        other_data = re.sub(timestamp_pattern, '', other_data).strip()
        timestamp_removed += 1

    # ------------------------------------------------
    # 3 Split commands
    # ------------------------------------------------
    commands = other_data.split(";")

    cleaned_commands = []

    for cmd in commands:

        cmd = cmd.strip()

        if cmd == "":
            continue

        # remove service account commands
        if cmd.startswith("~/google-cloud-sdk/bin/gcloud iam service-accounts"):
            service_removed += 1
            continue

        # remove IAM policy binding commands
        if cmd.startswith("~/google-cloud-sdk/bin/gcloud projects add-iam-policy-binding"):
            policy_removed += 1
            continue

        cleaned_commands.append(cmd)

    # rebuild command string
    cleaned_data = ";".join(cleaned_commands)

    cleaned_rows.append({
        "lab_id": lab_id,
        "other_data": cleaned_data
    })

    rows_processed += 1
    print(f"Row processed: {rows_processed} | Lab ID: {lab_id}")

# ---------------------------------
# Save cleaned file
# ---------------------------------

cleaned_df = pd.DataFrame(cleaned_rows)

cleaned_df.to_excel(output_file, index=False)

print("\n=================================")
print(f"Total rows processed: {rows_processed}")
print(f"Service account commands removed: {service_removed}")
print(f"IAM policy binding commands removed: {policy_removed}")
print(f"Timestamp metadata removed: {timestamp_removed}")
print(f"Output saved: {output_file}")
print("=================================")