import sys
import os
import argparse
import pandas as pd
import uuid
import json
from massql import msql_fileloading
from pyteomics import mzxml, mzml
from tqdm import tqdm

def load_data(input_filename):
    try:
        ms1_df, ms2_df = msql_fileloading.load_data(input_filename)

        return ms1_df, ms2_df
    except:
        print("Error loading data, falling back on default")

    MS_precisions = {
        1: 5e-6,
        2: 20e-6,
        3: 20e-6,
        4: 20e-6,
        5: 20e-6,
        6: 20e-6,
        7: 20e-6
    }
    
    ms1_df = pd.DataFrame()
    ms2_df = pd.DataFrame()

    all_mz = []
    all_i = []
    all_scan = []
    
    # TODO: read the mzML directly
    with mzml.read(input_filename) as reader:
        for spectrum in tqdm(reader):
            try:
                scan = spectrum["id"].replace("scanId=", "").split("scan=")[-1]
            except:
                scan = spectrum["id"]

            mz = spectrum["m/z array"]
            intensity = spectrum["intensity array"]

            all_mz += list(mz)
            all_i += list(intensity)
            all_scan += len(mz) * [scan]

            print(spectrum["id"])
            
    if len(all_mz) > 0:
        ms1_df['i'] = all_i
        ms1_df['mz'] = all_mz
        ms1_df['scan'] = all_scan

    return ms1_df, ms2_df

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

        ms1_df, ms2_df = load_data(filename)

        if len(ms1_df) == 0:
            print("Peaks Empty, skipping", filename)
            continue

        scan_or_coord = record["Scan/Coordinate"]

        spectra_list = []

        if scan_or_coord == "*":
            print("Grabbing all scans")
            
            # Splitting by scan
            print(ms1_df)
            scan_groups = ms1_df.groupby("scan")
            for scan, scan_df in scan_groups:
                peaks_list = scan_df.to_dict('records')
                peaks_list = [[peak["mz"], peak["i"]] for peak in peaks_list]

                print(scan, len(peaks_list))

                spectra_list.append(peaks_list)
        else:
            print("Grabbing {} scans".format(scan_or_coord))

            peaks_df = ms1_df[ms1_df["scan"] == scan_or_coord]
            peaks_list = peaks_df.to_dict('records')
            peaks_list = [[peak["mz"], peak["i"]] for peak in peaks_list]

            spectra_list.append(peaks_list)

        record["spectrum"] = spectra_list

    # Outputting the JSON
    output_json = os.path.join(args.output_folder, args.output_identifier + ".json")
    open(output_json, "w").write(json.dumps(all_rows, indent=4))

    # Outputting the Summary 
    summary_df = pd.DataFrame(all_rows)
    try:
        summary_df = summary_df.drop(['spectrum'], axis=1)
    except:
        pass

    output_extraction_tsv = os.path.join(args.output_folder, args.output_identifier + ".tsv")
    summary_df.to_csv(output_extraction_tsv, sep="\t", index=False)


if __name__ == "__main__":
    main()