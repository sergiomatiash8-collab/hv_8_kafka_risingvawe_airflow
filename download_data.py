import kagglehub
import os
import shutil

# 1. Download dataset
path = kagglehub.dataset_download("thoughtvector/customer-support-on-twitter")

print("Dataset path:", path)

# 2. Show all files to understand structure
print("\nFiles in dataset:")
for root, dirs, files in os.walk(path):
    for file in files:
        print(os.path.join(root, file))

# 3. Find first CSV file
csv_file = None
for root, dirs, files in os.walk(path):
    for file in files:
        if file.endswith(".csv"):
            csv_file = os.path.join(root, file)
            break

if csv_file is None:
    raise Exception("CSV file not found")

print("\nFound CSV:", csv_file)

# 4. Create data directory
os.makedirs("data", exist_ok=True)

# 5. Copy file into project
destination = os.path.join("data", "twcs.csv")
shutil.copy(csv_file, destination)

print("Copied to:", destination)