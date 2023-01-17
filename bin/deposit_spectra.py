import sys
import os
import argparse
import glob
import pandas as pd
import json
import requests
import yaml
from dotenv import dotenv_values, load_dotenv

SERVER_URL = "http://169.235.26.140:5392/"
#SERVER_URL = "https://idbac-kb.gnps2.org/"

def main():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('input_json_folder')
    parser.add_argument('--params')

    args = parser.parse_args()

    config = dotenv_values()

    # Prepping the requests from the json
    all_json_files = glob.glob(os.path.join(args.input_json_folder, "*.json"))

    for json_filename in all_json_files:
        spectra_list = json.load(open(json_filename))

        for spectrum_obj in spectra_list:
            parameters = spectrum_obj

            workflow_params = yaml.safe_load(open(args.params))

            parameters["task"] = workflow_params["task"]
            parameters["user"] = workflow_params["OMETAUSER"]
            parameters["CREDENTIALSKEY"] = config["CREDENTIALSKEY"]

            r = requests.post("{}/api/deposit".format(SERVER_URL), json=parameters)
            r.raise_for_status()


if __name__ == "__main__":
    main()