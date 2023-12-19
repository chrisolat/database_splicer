import sys
sys.path.append("../modules")
import os

print("python3 ../generate_schema.py --generate_schema_tables --schemas contacts --outfile schema_tables.json")
os.system("python3 ../generate_schema.py --generate_schema_tables --schemas contacts --outfile schema_tables.json")

print("\npython3 ../generate_schema.py --generate_schema --infile schema_tables.json --outfile output.json")
os.system("python3 ../generate_schema.py --generate_schema --infile schema_tables.json --outfile output.json ")

print("\npython3 ../generate_schema.py --generate_detailed_schema --infile output.json --outfile detailed.json")
os.system("python3 ../generate_schema.py --generate_detailed_schema --infile output.json --outfile detailed.json")
print("Output file: detailed.json")