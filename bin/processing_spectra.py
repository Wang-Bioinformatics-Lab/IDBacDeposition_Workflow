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

def load_metadata_file(metadata_path:str):
    """
    Reads a metadata file and converts it to a pandas dataframe. 
    
    Args:
    metadata_path: str, path to the metadata file

    Returns:
    metadata_df: pd.DataFrame, metadata

    Raises:
    ValueError: if the metadata file is not a CSV, XLSX, XLS, or TSV file
    """
    metadata_path = str(metadata_path)
    if metadata_path.endswith('.csv'):
        metadata_df = pd.read_csv(metadata_path)
    elif metadata_path.endswith('.xlsx'):
        metadata_df = pd.read_excel(metadata_path, sheet_name=None)
        # If it contains multiple tables, get the one named "Metadata sheet"
        if isinstance(metadata_df, dict):
            metadata_df = pd.read_excel(metadata_path, sheet_name=None)
        if 'Metadata sheet' in metadata_df:
            metadata_df = metadata_df['Metadata sheet']
        elif 'Metadata template' in metadata_df:
            metadata_df = metadata_df['Metadata template']
        else:
            # Pop "instructions" or Instructions or anything like that
            all_keys = list(metadata_df.keys())
            for key in all_keys:
                if key.lower().strip() in ['instructions', 'instruction', 'metadata instructions']:
                    metadata_df.pop(key)
            # If there is only one sheet, use that
            if len(metadata_df) == 1:
                metadata_df = list(metadata_df.values())[0]
            else:
                raise ValueError(f"Excel file should contain only one sheet, or one named 'Metadata sheet' or 'Metadata template'. Instead found {metadata_df.keys()}")
    elif metadata_path.endswith('.xls'):
        metadata_df = pd.read_excel(metadata_path, sheet_name=None)
        # If it contains multiple tables, get the one named "Metadata sheet"
        if isinstance(metadata_df, dict):
            metadata_df = pd.read_excel(metadata_path, sheet_name=None)
        if 'Metadata sheet' in metadata_df:
            metadata_df = metadata_df['Metadata sheet']
        elif 'Metadata template' in metadata_df:
            metadata_df = metadata_df['Metadata template']
        else:
            all_keys = list(metadata_df.keys())
            for key in all_keys:
                if key.lower().strip() in ['instructions', 'instruction', 'metadata instructions']:
                    metadata_df.pop(key)
            # If there is only one sheet, use that
            if len(metadata_df) == 1:
                metadata_df = list(metadata_df.values())[0]
            else:
                raise ValueError(f"Excel file should contain only one sheet, or one named 'Metadata sheet' or 'Metadata template'. Instead found {metadata_df.keys()}")
    elif metadata_path.endswith('.tsv'):
        metadata_df = pd.read_csv(metadata_path, sep='\t')
    else:
        raise ValueError(f'Metadata file must be a CSV, XLSX, XLS, or TSV file, but got {metadata_path} instead.')
    
    return metadata_df

def main():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('input_metadata')
    parser.add_argument('input_spectra_folder')
    parser.add_argument('output_folder')
    parser.add_argument('--output_identifier', default=str(uuid.uuid4()))

    args = parser.parse_args()

    print(args)

    metadata_df = load_metadata_file(args.input_metadata)
    metadata_df.columns = metadata_df.columns.str.strip()

    # Make sure scan/coordinate is present
    if not "Scan/Coordinate" in metadata_df.columns:
        raise ValueError("Scan/Coordinate column not found in metadata")

    columns_needed = ["Filename", "Scan/Coordinate"]
    for column in columns_needed:
        if not column in metadata_df.columns:
            print("Error, missing column", column)
            sys.exit(1)

    # Ensure none are empty
    if metadata_df["Scan/Coordinate"].isnull().values.any():
        print("Scan/Coordinate column has empty values")
        sys.exit(1)

    all_rows = metadata_df.to_dict('records')

    # TODO: We should limit the column names
    columns_possible = ["Filename", "Scan/Coordinate", "Strain name"]

    for record in all_rows:
        # checking if file is NaN
        if pd.isnull(record["Filename"]):
            continue

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
            scan_groups = ms1_df.groupby("scan")
            for scan, scan_df in scan_groups:
                peaks_list = scan_df.to_dict('records')
                peaks_list = [[peak["mz"], peak["i"]] for peak in peaks_list]

                print("SCAN and length of peaks", scan, len(peaks_list))

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