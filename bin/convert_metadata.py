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
    
    metadata_df.to_csv(args.output_metadata, index=False, sep="\t")


if __name__ == "__main__":
    main()