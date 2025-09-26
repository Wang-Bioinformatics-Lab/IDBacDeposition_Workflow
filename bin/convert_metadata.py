import argparse
import pandas as pd

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
    parser.add_argument('output_metadata')

    args = parser.parse_args()

    metadata_df = load_metadata_file(args.input_metadata)
    # Replace newlines in column names with spaces
    metadata_df.columns = metadata_df.columns.str.replace("\n", " ")
    # Strip whitespace from column names
    metadata_df.columns = metadata_df.columns.str.strip()
    # Convert multiple spaces to single
    metadata_df.columns = metadata_df.columns.str.replace(r" +", " ", regex=True)
    
    metadata_df.to_csv(args.output_metadata, index=False, sep="\t")


if __name__ == "__main__":
    main()