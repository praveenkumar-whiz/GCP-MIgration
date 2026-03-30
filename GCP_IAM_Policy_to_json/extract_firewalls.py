import csv
import re
import pandas as pd

input_file = "gcp_infra_dump.csv"
output_file = "lab_firewall_commands.xlsx"

results = {}

total_rows = 0
rows_with_firewall = 0
rows_without_firewall = 0
error_rows = []
total_commands = 0

pattern = r'~/google-cloud-sdk/bin/gcloud[^;]*firewall-rules create[^;]*;'

print("Starting firewall extraction...\n")

with open(input_file, "r", encoding="utf-8", errors="ignore") as f:
    reader = csv.reader(f)

    for row in reader:
        total_rows += 1

        try:
            if len(row) < 2:
                error_rows.append(("UNKNOWN", total_rows))
                continue

            lab_id = row[1]

            row_text = " ".join(row)

            matches = re.findall(pattern, row_text)

            if matches:
                rows_with_firewall += 1
                total_commands += len(matches)

                clean_cmds = [cmd.strip() for cmd in matches]

                if lab_id not in results:
                    results[lab_id] = []

                results[lab_id].extend(clean_cmds)

                print(f"✔ Lab {lab_id} -> {len(matches)} firewall command(s) found")

            else:
                rows_without_firewall += 1

        except Exception as e:
            error_rows.append((lab_id, total_rows))
            print(f"⚠ Error processing row {total_rows} (Lab {lab_id})")

# Prepare dataframe
data = []

for lab_id, cmds in results.items():
    data.append({
        "lab_id": lab_id,
        "firewall_commands": " ".join(cmds)
    })

df = pd.DataFrame(data)
df.to_excel(output_file, index=False)

print("\n========== SUMMARY ==========")
print(f"Total rows processed     : {total_rows}")
print(f"Rows with firewall rules : {rows_with_firewall}")
print(f"Rows without firewall    : {rows_without_firewall}")
print(f"Total firewall commands  : {total_commands}")
print(f"Error rows               : {len(error_rows)}")

if error_rows:
    print("\nError Details:")
    for lab_id, row_no in error_rows:
        print(f"Row {row_no} | Lab ID: {lab_id}")

print(f"\nOutput file created: {output_file}")
print("Extraction completed successfully ✅")