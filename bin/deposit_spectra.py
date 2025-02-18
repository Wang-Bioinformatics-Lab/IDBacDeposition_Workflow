import os
import argparse
from pathlib import Path
import glob
import json
import requests
import yaml
from dotenv import dotenv_values

#SERVER_URL = "http://169.235.26.140:5392/" # This is Debug Server
SERVER_URL = "https://idbac.org/"

def _validate_entry(spectrum_obj, existing_names):
    valid_fields = ["spectrum", "Strain name", "Strain ID", "Filename",
                    "Scan/Coordinate", "Genbank accession", "NCBI taxid", "16S Taxonomy",
                    "16S Sequence", "Culture Collection", "MALDI matrix name", "MALDI prep", "Number of replicates",
                    "Cultivation media", "Cultivation temp", "Cultivation time", "Isolation media", "PI",
                    "MS Collected by", "Isolate Collected by", "Sample Collected by",
                    "Sample name", "Isolate Source", "Source Location Name", "Longitude",
                    "Latitude", "Altitude", "Collection Temperature", "MALDI instrument", "Comment"]
    
    required_fields = ["spectrum", "Strain name", "Filename", "MALDI matrix name", "MALDI prep",
                    "Cultivation media", "Cultivation temp", "Cultivation time", "PI"]


    new_spectrum_obj = {}

    for key in spectrum_obj:
        if key in valid_fields:
            new_spectrum_obj[key] = spectrum_obj[key]
        else:
            print("Invalid Field", key)
            continue

    for key in required_fields:
        if not key in new_spectrum_obj:
            print("Missing Required Field", key)
            raise Exception(f"Missing Required Field, {key}") from None
        
    # if "Strain name" in new_spectrum_obj and new_spectrum_obj["Strain name"] in existing_names:
    #     print("Strain Name already exists in the database")
    #     raise Exception("Strain Name already exists in the database") from None

    return new_spectrum_obj


def main():
    parser = argparse.ArgumentParser(description='Depositing the spectra one at a time.')
    parser.add_argument('input_json_folder')
    parser.add_argument('--params')
    parser.add_argument('--dryrun', default="Yes")
    parser.add_argument('--existing_names', required=True)

    args = parser.parse_args()

    existing_names_file = Path(str(args.existing_names))

    existing_names = json.load(open(existing_names_file, 'r'))

    config = dotenv_values()

    # Prepping the requests from the json
    all_json_files = glob.glob(os.path.join(args.input_json_folder, "*.json"))

    for json_filename in all_json_files:
        print(json_filename)

        spectra_list = json.load(open(json_filename))
        # Strip whitespace from the keys
        spectra_list = [{k.strip(): v for k, v in d.items()} for d in spectra_list]

        all_strain_names = []
        for spectrum_obj in spectra_list:
            if not "spectrum" in spectrum_obj:
                continue
    
            # Validate them ahead of time
            _validate_entry(spectrum_obj, existing_names)
            all_strain_names.append(spectrum_obj["Strain name"])

        # Check that strain names are unique
        if len(all_strain_names) != len(set(all_strain_names)):
            raise Exception("Strain names are not unique") from None

        for spectrum_obj in spectra_list:
            parameters = {}

            workflow_params = yaml.safe_load(open(args.params))

            if not "spectrum" in spectrum_obj:
                continue

            parameters["task"] = workflow_params["task"]
            parameters["user"] = workflow_params["OMETAUSER"]
            parameters["CREDENTIALSKEY"] = config["CREDENTIALSKEY"]
            parameters["spectrum_json"] = json.dumps(spectrum_obj)

            if args.dryrun == "No":
                print("Submitting Spectrum")
                r = requests.post("{}/api/spectrum".format(SERVER_URL), data=parameters)
                r.raise_for_status()

    # Once we've updated everything, we should tell the KB to update
    if args.dryrun == "No":
        r = requests.get("{}/api/database/refresh".format(SERVER_URL))
        r.raise_for_status()


if __name__ == "__main__":
    main()