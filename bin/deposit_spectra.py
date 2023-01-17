import sys
import os
import argparse
import pandas as pd
import json
import requests
from dotenv import dotenv_values


SERVER_URL = "http://169.235.26.140:5392/"
SERVER_URL = "https://idbac-kb.gnps2.org/"

def main():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('input_json')

    args = parser.parse_args()

    print(args)

    config = dotenv_values(".env")

    print(config)

    raise Exception("NONE")



if __name__ == "__main__":
    main()