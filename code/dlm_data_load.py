# Python Code to Load DLM data
import gzip
from io import StringIO
import os
import pandas as pd

# Define the current directory path
current_dir = os.getcwd()

# Iterate through all files in the current directory
for file_name in os.listdir(current_dir):
    # Check if the file has a .gz extension
    
    if file_name.endswith('.gz'):
        print(file_name)
        # Construct the full file path
        file_path = os.path.join(current_dir, file_name)
        
        # Define the output file path (remove the .gz extension)
        output_file_path = ''.join(file_path.split(".")[:-2])  # Remove the last 3 characters (.gz)
        
        try:
            # Open the gzip-compressed file for reading
            with gzip.open(file_path, 'rb') as f:
                # Read the contents of the compressed file
                compressed_data = f.read().decode('utf-8')
                data_stream = StringIO(compressed_data)
                rows = [line.strip().split("|") for line in data_stream]
                df = pd.DataFrame(rows)
                df.to_csv(f"{output_file_path}.csv")
        
        except Exception as e:
            print(f"Error unzipping {file_name}: {e}")

