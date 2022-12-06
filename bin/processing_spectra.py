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
    parser.add_argument('input_spectra_folder')
    parser.add_argument('output_folder')
    parser.add_argument('--output_identifier', default=str(uuid.uuid4()) + ".json")

    args = parser.parse_args()

    print(args)

    metadata_df = pd.read_excel(args.input_metadata)
    print(metadata_df)

    all_rows = metadata_df.to_dict('records')
    for record in all_rows:
        filename = os.path.join(args.input_spectra_folder, record["Filename"])

        if not os.path.exists(filename):
            continue

        ms1_df, ms2_df = msql_fileloading.load_data(filename)

        scan_or_coord = record["Scan/Coordinate"]

        if scan_or_coord == "*":
            # We'll just grab the first one
            print("Grabbing first scan")
            first_scan = ms1_df["scan"].iloc[0]
            peaks_df = ms1_df[ms1_df["scan"] == first_scan]
            peaks_list = peaks_df.to_dict('records')
            peaks_list = [[peak["mz"], peak["i"]] for peak in peaks_list]
            
            #print(peaks_list)

        record["peaks"] = peaks_list

    output_file = os.path.join(args.output_folder, args.output_identifier)
    open(output_file, "w").write(json.dumps(all_rows))


if __name__ == "__main__":
    main()