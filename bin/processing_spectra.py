import sys
import os
import argparse
import pandas as pd
import uuid
import json
from massql import msql_fileloading
import pymzml


def load_data(filename):
    # TODO: read the mzML directly
    return "MING"

def main():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('input_metadata')
    parser.add_argument('input_spectra_folder')
    parser.add_argument('output_folder')
    parser.add_argument('--output_identifier', default=str(uuid.uuid4()))

    args = parser.parse_args()

    print(args)

    metadata_df = pd.read_excel(args.input_metadata)

    all_rows = metadata_df.to_dict('records')
    for record in all_rows:
        filename = os.path.join(args.input_spectra_folder, record["Filename"])

        if not os.path.exists(filename):
            continue

        
        ms1_df, ms2_df = msql_fileloading.load_data(filename)

        scan_or_coord = record["Scan/Coordinate"]

        if scan_or_coord == "*":
            # We'll just grab the first one
            # TODO: Actually do the right thing here
            print("Grabbing first scan")
            first_scan = ms1_df["scan"].iloc[0]
            peaks_df = ms1_df[ms1_df["scan"] == first_scan]
            peaks_list = peaks_df.to_dict('records')
            peaks_list = [[peak["mz"], peak["i"]] for peak in peaks_list]
        else:
            raise Exception("Need to implement getting scan")

        record["peaks"] = peaks_list

    # Outputting the JSON
    output_json = os.path.join(args.output_folder, args.output_identifier + ".json")
    open(output_json, "w").write(json.dumps(all_rows))

    # Outputting the Summary 
    summary_df = pd.DataFrame(all_rows)
    summary_df = summary_df.drop(['peaks'], axis=1)

    output_extraction_tsv = os.path.join(args.output_folder, args.output_identifier + ".tsv")
    summary_df.to_csv(output_extraction_tsv, sep="\t", index=False)


if __name__ == "__main__":
    main()