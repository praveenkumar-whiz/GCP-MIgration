import csv
import re
from collections import defaultdict
import pandas as pd

input_file = "gcp_infra_dump.csv"
output_file = "lab_task_policies_v1.xlsx"

task_policies = defaultdict(set)

rows_processed = 0
task_count = 0
error_rows = 0
excel_limit_warnings = 0

def is_number(value):
    return re.fullmatch(r'\d+', value) is not None

print("\nStarting policy extraction...\n")

try:
    with open(input_file, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)

        for row_number, row in enumerate(reader, start=2):

            rows_processed += 1

            try:
                if len(row) < 3:
                    continue

                task_id = row[1]
                policies = []

                for val in row[2:]:
                    val = val.strip()

                    if is_number(val):
                        break

                    if val:
                        policies.append(val)

                for p in policies:
                    task_policies[task_id].add(p)

            except Exception as e:
                error_rows += 1
                print(f"[ERROR] Row {row_number} | Task ID {task_id if 'task_id' in locals() else 'UNKNOWN'}")
                print(f"Reason: {e}\n")

except FileNotFoundError:
    print(f"Input file not found: {input_file}")
    exit()

print(f"\nFinished reading CSV")
print(f"Rows processed: {rows_processed}")
print(f"Unique task_ids found: {len(task_policies)}\n")

data = []

for task_id, policies in task_policies.items():

    policy_string = ",".join(sorted(policies))

    if len(policy_string) > 32767:
        excel_limit_warnings += 1
        print(f"[WARNING] task_id {task_id} exceeds Excel cell limit ({len(policy_string)} chars)")

    data.append([task_id, policy_string])

df = pd.DataFrame(data, columns=["task_id", "policy"])

try:
    df.to_excel(output_file, index=False)
    print(f"\nExcel file created successfully: {output_file}")

except Exception as e:
    print(f"\nFailed to write Excel file")
    print(e)

print("\nExecution Summary")
print("----------------------------")
print(f"Rows processed        : {rows_processed}")
print(f"Task IDs processed    : {len(task_policies)}")
print(f"Row errors            : {error_rows}")
print(f"Excel limit warnings  : {excel_limit_warnings}")
print("----------------------------\n")