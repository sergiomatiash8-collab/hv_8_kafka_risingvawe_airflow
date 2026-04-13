import kagglehub
import os
import shutil

# 1. Download dataset
path = kagglehub.dataset_download("thoughtvector/customer-support-on-twitter")

print("Path to dataset files:", path)

# 2. Показати всі файли (щоб ти бачив структуру)
print("\nFiles in dataset:")
for root, dirs, files in os.walk(path):
    for file in files:
        print(os.path.join(root, file))

# 3. Знайти перший CSV файл
csv_file = None
for root, dirs, files in os.walk(path):
    for file in files:
        if file.endswith(".csv"):
            csv_file = os.path.join(root, file)
            break

if csv_file is None:
    raise Exception("❌ CSV file not found!")

print("\nFound CSV:", csv_file)

# 4. Створити папку data
os.makedirs("data", exist_ok=True)

# 5. Скопіювати в проєкт
destination = os.path.join("data", "twcs.csv")
shutil.copy(csv_file, destination)

print("✅ Copied to:", destination)