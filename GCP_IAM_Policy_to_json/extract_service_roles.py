import re
import pandas as pd

INPUT_FILE = "gcp_infra_dump.csv"
OUTPUT_FILE = "gcp_service_account_roles.xlsx"

print("====================================")
print("Extracting Service Account Roles")
print("====================================")

results = []
processed = 0
errors = 0
labs_with_roles = 0

with open(INPUT_FILE, "r", encoding="utf-8", errors="ignore") as f:

    for line_number, line in enumerate(f):

        try:
            processed += 1

            # Extract lab_id
            lab_match = re.search(r'^"\d+","(\d+)"', line)

            if not lab_match:
                continue

            lab_id = lab_match.group(1)

            roles = []

            # Extract roles INCLUDING roles/
            matches = re.findall(r'--role=""?(roles/[A-Za-z0-9\.\-]+)""?;', line)

            for role in matches:
                if role not in roles:
                    roles.append(role)

            if roles:
                labs_with_roles += 1

            results.append({
                "lab_id": lab_id,
                "service_account_roles": "; ".join(roles)
            })

            if processed % 500 == 0:
                print(f"Processed {processed} lines")

        except Exception as e:
            errors += 1
            print(f"Error at line {line_number}: {e}")

print("------------------------------------")
print("Creating Excel file...")

df = pd.DataFrame(results)

df.to_excel(OUTPUT_FILE, index=False)

print("------------------------------------")
print("PROCESS COMPLETE")
print("------------------------------------")
print(f"Total lines scanned : {processed}")
print(f"Labs with roles     : {labs_with_roles}")
print(f"Errors              : {errors}")
print(f"Output file         : {OUTPUT_FILE}")
print("------------------------------------")