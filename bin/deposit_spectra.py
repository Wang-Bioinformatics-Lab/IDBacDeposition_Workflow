import sys
import os
import argparse
import glob
import pandas as pd
import json
import requests
import yaml
from dotenv import dotenv_values, load_dotenv

#SERVER_URL = "http://169.235.26.140:5392/" # This is Debug Server
SERVER_URL = "https://idbac.org/"

def _validate_entry(spectrum_obj):
    valid_fields = ["spectrum", "Strain name", "Strain ID", "Filename",
                    "Scan/Coordinate", "Genbank accession", "NCBI taxid", "16S Taxonomy",
                    "16S Sequence", "Culture Collection", "MALDI matrix name", "MALDI prep",
                    "Cultivation media", "Cultivation temp", "Cultivation time", "PI",
                    "MS Collected by", "Isolate Collected by", "Sample Collected by",
                    "Sample name", "Isolate Source", "Source Location Name", "Longitude",
                    "Latitude", "Altitude", "Collection Temperature", "Comment"]
    
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
            raise Exception("Missing Required Field")

    return new_spectrum_obj


def main():
    parser = argparse.ArgumentParser(description='Depositing the spectra one at a time.')
    parser.add_argument('input_json_folder')
    parser.add_argument('--params')
    parser.add_argument('--dryrun', default="Yes")

    args = parser.parse_args()

    config = dotenv_values()

    # Prepping the requests from the json
    all_json_files = glob.glob(os.path.join(args.input_json_folder, "*.json"))

    for json_filename in all_json_files:
        print(json_filename)

        spectra_list = json.load(open(json_filename))
        # Strip whitespace from the keys
        spectra_list = [{k.strip(): v for k, v in d.items()} for d in spectra_list]

        for spectrum_obj in spectra_list:
            parameters = {}

            workflow_params = yaml.safe_load(open(args.params))

            if not "spectrum" in spectrum_obj:
                continue

            try:
                spectrum_obj = _validate_entry(spectrum_obj)
            except:
                continue

            parameters["task"] = workflow_params["task"]
            parameters["user"] = workflow_params["OMETAUSER"]
            parameters["CREDENTIALSKEY"] = config["CREDENTIALSKEY"]
            parameters["spectrum_json"] = json.dumps(spectrum_obj)

            if args.dryrun == "No":
                print("Submitting Spectrum")
                r = requests.post("{}/api/spectrum".format(SERVER_URL), data=parameters)
                r.raise_for_status()

            #TODO: We should define a collection

    # Once we've updated everything, we should tell the KB to update
    if args.dryrun == "No":
        r = requests.get("{}/api/database/refresh".format(SERVER_URL))
        r.raise_for_status()


if __name__ == "__main__":
    main()