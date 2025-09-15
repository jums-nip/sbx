# Databricks notebook source
# MAGIC %md
# MAGIC ### 😎 Task Notebook to Decrypt PGP encrypted files to CSV
# MAGIC

# COMMAND ----------

import subprocess

# COMMAND ----------

# List files in the key directory to confirm access
key_dir = '/mnt/pgp/'
files = dbutils.fs.ls(key_dir)
for file in files:
    print(f"Found file: {file.name}")

# COMMAND ----------

# Copy the key file from the mounted storage to the local file system (/tmp)
dbutils.fs.cp("dbfs:/mnt/pgp/Karl Saycon_0x42A38B46_SECRET.asc", "file:/tmp/Karl_Saycon_0x42A38B46_SECRET.asc")

# COMMAND ----------

import subprocess

# Define the local path to the key file in /tmp
key_file = "/tmp/Karl_Saycon_0x42A38B46_SECRET.asc"

# Run the GPG import command
import_cmd = f"gpg --import {key_file}"
result = subprocess.run(import_cmd, shell=True, capture_output=True, text=True)

print("Import output:")
print(result.stdout)
print("Import error (if any):")
print(result.stderr)


# COMMAND ----------

list_keys_cmd = "gpg --list-secret-keys"
result = subprocess.run(list_keys_cmd, shell=True, capture_output=True, text=True)
print("Secret keys in GPG:")
print(result.stdout)


# COMMAND ----------

import os
import subprocess
from concurrent.futures import ThreadPoolExecutor

# Set the input and output directories
input_dir = '/dbfs/mnt/sftp/'
output_dir = '/dbfs/mnt/sas/'

# Function to decrypt a single file
def decrypt_file(file_info):
    input_file = input_dir + file_info.name
    output_file = output_dir + file_info.name.replace('.pgp', '')  # Remove '.pgp' extension for the output file
    
    # GPG decryption command
    gpg_command = [
        'gpg',
        '--output', output_file,
        '--decrypt', '--recipient', '64E32C1EF159B3BE8C1FCB178AA9A62900B27F7B',
        input_file
    ]
    
    try:
        # Run the decryption command
        result = subprocess.run(gpg_command, check=True, capture_output=True, text=True)
        print(f"Decryption successful for {file_info.name}.")
        
        # If successful, remove the original .pgp file from /mnt/sftp
        dbutils.fs.rm(f"/mnt/sftp/{file_info.name}")
        print(f"Deleted {file_info.name} from /mnt/sftp after decryption.")
        
        return f"Success: {file_info.name}"
    
    except subprocess.CalledProcessError as e:
        print(f"Decryption failed for {file_info.name}. Error: {e.stderr}")
        return f"Failed: {file_info.name}"

# List all .pgp files in the input directory
files = [file for file in dbutils.fs.ls('/mnt/sftp') if file.name.endswith('.pgp')]

# Use ThreadPoolExecutor for concurrent decryption
with ThreadPoolExecutor() as executor:
    results = list(executor.map(decrypt_file, files))

# Print out summary of results
print("Decryption results:")
for result in results:
    print(result)
