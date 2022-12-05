#!/usr/bin/env nextflow

params.input_metadata = ""
params.input_spectra_folder = ""

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
    file 'output.tsv'

    """
    python $TOOL_FOLDER/processing_spectra.py $input $spectra output.tsv
    """
}