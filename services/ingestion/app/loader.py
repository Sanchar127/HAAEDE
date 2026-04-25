import pandas as pd

def load_dataset(path: str):
    df = pd.read_csv(path)
    df = df.fillna(0)
    return df