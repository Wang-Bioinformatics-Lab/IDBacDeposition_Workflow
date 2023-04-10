import sys
import os
import argparse
import glob
import pandas as pd
import json
import requests
import yaml
from dotenv import dotenv_values, load_dotenv

SERVER_URL = "http://169.235.26.140:5392/" # This is Debug Server
#SERVER_URL = "https://idbac-kb.gnps2.org/"

def main():
    parser = argparse.ArgumentParser(description='Process some integers.')
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

        for spectrum_obj in spectra_list:
            parameters = {}

            workflow_params = yaml.safe_load(open(args.params))

            if not "peaks" in spectrum_obj:
                continue

            parameters["task"] = workflow_params["task"]
            parameters["user"] = workflow_params["OMETAUSER"]
            parameters["CREDENTIALSKEY"] = config["CREDENTIALSKEY"]
            parameters["spectrum_json"] = json.dumps(spectrum_obj)

            if args.dryrun == "No":
                r = requests.post("{}/api/spectrum".format(SERVER_URL), data=parameters)
                #print(r.text)
                r.raise_for_status()

            #TODO: We should define a collection


if __name__ == "__main__":
    main()