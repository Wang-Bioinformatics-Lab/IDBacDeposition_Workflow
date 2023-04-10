#!/usr/bin/env nextflow

params.input_metadata = ""
params.input_spectra_folder = ""
params.dryrun = "Yes"

// Workflow Boiler Plate
params.OMETALINKING_YAML = "flow_filelinking.yaml"
params.OMETAPARAM_YAML = "job_parameters.yaml"

TOOL_FOLDER = "$baseDir/bin"

process processData {
    publishDir "./nf_output", mode: 'copy'

    conda "$TOOL_FOLDER/conda_env_idbac.yml"

    input:
    file input from Channel.fromPath(params.input_metadata)
    file spectra from Channel.fromPath(params.input_spectra_folder)

    output:
    file 'output_spectra' into _spectra_json_ch

    """
    mkdir output_spectra
    python $TOOL_FOLDER/processing_spectra.py $input $spectra output_spectra
    """
}

process depositSpectrum {
    publishDir "./nf_output", mode: 'copy'

    conda "$TOOL_FOLDER/conda_env_idbac.yml"

    input:
    file input from _spectra_json_ch
    file params_file from Channel.fromPath(params.OMETAPARAM_YAML)

    """
    python $TOOL_FOLDER/deposit_spectra.py $input \
    --params $params_file \
    --dryrun $params.dryrun
    """
}

process showMetadata {
    publishDir "./metadata_converted", mode: 'copy'

    input:
    file input from Channel.fromPath(params.input_metadata)

    output:
    file 'converted_metadata.tsv'

    """
    python $TOOL_FOLDER/convert_metadata.py $input converted_metadata.tsv
    """
}

