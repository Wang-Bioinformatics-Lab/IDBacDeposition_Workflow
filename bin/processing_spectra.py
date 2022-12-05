import sys
import argparse

def main():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('input_metadata')
    parser.add_argument('input_spectra_folder')
    parser.add_argument('output_filename')

    args = parser.parse_args()


if __name__ == "__main__":
    main()