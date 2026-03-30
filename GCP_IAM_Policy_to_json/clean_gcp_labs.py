import csv
import re
import pandas as pd

input_file = "gcp_infra_dump.csv"
output_file = "cleaned_gcp_labs.xlsx"

print(f"\nProcessing file: {input_file}\n")

rows_processed = 0
cleaned_data = []

# Regex pattern to detect GCP permissions like service.resource.action
permission_pattern = re.compile(r'^[a-zA-Z0-9]+\.[a-zA-Z0-9]+\.[a-zA-Z0-9]+')

with open(input_file, "r", encoding="utf-8") as file:
    reader = csv.reader(file)

    for row in reader:

        if len(row) < 2:
            continue

        # second column = lab_id
        lab_id = row[1].strip().replace('"', '')

        other_data_parts = []

        # scan remaining columns
        for col in row[2:]:
            value = col.strip()

            # skip permissions
            if permission_pattern.match(value):
                continue

            # keep other data
            if value != "":
                other_data_parts.append(value)

        other_data = " ".join(other_data_parts)

        cleaned_data.append({
            "lab_id": lab_id,
            "other_data": other_data
        })

        rows_processed += 1
        print(f"Row processed: {rows_processed} | Lab ID: {lab_id}")

print("\nRemoving permissions completed\n")

# Convert to DataFrame
df = pd.DataFrame(cleaned_data)

# Save Excel
df.to_excel(output_file, index=False)

print("=================================")
print(f"Total rows processed: {rows_processed}")
print(f"Output file created: {output_file}")
print("=================================")