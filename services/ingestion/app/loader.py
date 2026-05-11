import pandas as pd

def load_kaggle_data(file_path):
    """Reads the local CSV file in chunks to handle memory efficiently."""
    try:
        # Using chunks in case the file is large
        return pd.read_csv(file_path, chunksize=1000)
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return None