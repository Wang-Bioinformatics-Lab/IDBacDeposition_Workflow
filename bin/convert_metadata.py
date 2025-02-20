import sys
import os
import argparse
import pandas as pd
import uuid
import json
from massql import msql_fileloading

def main():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('input_metadata')
    parser.add_argument('output_metadata')

    args = parser.parse_args()

    metadata_df = pd.read_excel(args.input_metadata)
    # Replace newlines in column names with spaces
    metadata_df.columns = metadata_df.columns.str.replace("\n", " ")
    # Strip whitespace from column names
    metadata_df.columns = metadata_df.columns.str.strip()
    # Convert multiple spaces to single
    metadata_df.columns = metadata_df.columns.str.replace(r" +", " ", regex=True)
    
    metadata_df.to_csv(args.output_metadata, index=False, sep="\t")


if __name__ == "__main__":
    main()