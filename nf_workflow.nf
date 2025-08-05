#!/usr/bin/env nextflow
nextflow.enable.dsl=2

params.input_metadata = ""
params.input_spectra_folder = ""
params.dryrun = "Yes"

// Processing Parameters
params.merge_replicates = "Yes"


// Workflow Boiler Plate
params.OMETALINKING_YAML = "flow_filelinking.yaml"
params.OMETAPARAM_YAML = "job_parameters.yaml"

params.idbac_url = "idbac.org"

// Min/Max m/z values for processing
params.min_mz = ""
params.max_mz = ""

TOOL_FOLDER = "$baseDir/bin"

process processInputDataAndMetadata {
    publishDir "./nf_output", mode: 'copy'

    conda "$TOOL_FOLDER/conda_env_idbac.yml"

    input:
    file input_metadata
    file spectra

    output:
    file 'output_spectra'

    """
    if [ ! -d "output_spectra" ]; then
        mkdir output_spectra
    fi
    min_mz_flag=""
    max_mz_flag=""
    if [ ! -z "${params.min_mz}" ]; then
        min_mz_flag="--min_mz ${params.min_mz}"
        echo "processInputDataAndMetadata() Using min_mz: ${params.min_mz}"
    fi
    if [ ! -z "${params.max_mz}" ]; then
        max_mz_flag="--max_mz ${params.max_mz}"
        echo "processInputDataAndMetadata() Using max_mz: ${params.max_mz}"
    fi

    python $TOOL_FOLDER/processing_spectra.py $input_metadata $spectra output_spectra \$min_mz_flag \$max_mz_flag
    """
}

process getExistingNames {
    output:
    path 'existing_names.txt', emit: existing_names

    """
    MAX_RETRIES=3
    TIMEOUT=30
    URL="${params.idbac_url}/api/get_all_strain_names"
    OUTPUT_FILE="existing_names.txt"

    # Retry logic
    for ((i=1; i<=MAX_RETRIES; i++))
    do
        echo "Attempt \$i of \$MAX_RETRIES..."
        
        # Run wget with timeout
        wget --timeout=\$TIMEOUT \$URL -O \$OUTPUT_FILE
        
        # Check if the wget command was successful
        if [ \$(echo \$?) -eq 0 ]; then
            echo "Download successful."
            break
        else
            echo "Download failed. Retrying in \$TIMEOUT seconds..."
            sleep \$TIMEOUT
        fi
        
        # If the last attempt fails, print an error message
        if [ \$i -eq \$MAX_RETRIES ]; then
            echo "Failed to download after \$MAX_RETRIES attempts."
        fi
    done
    """
}

process depositSpectrum {
    publishDir "./nf_output", mode: 'copy'

    conda "$TOOL_FOLDER/conda_env_idbac.yml"

    input:
    file input_spectra_json
    file params_file
    file existing_names
    val dummy

    """
    python $TOOL_FOLDER/deposit_spectra.py $input_spectra_json \
    --params $params_file \
    --dryrun $params.dryrun \
    --existing_names existing_names.txt
    """
}

process showMetadata {
    publishDir "./metadata_converted", mode: 'copy'

    conda "$TOOL_FOLDER/conda_env.yml"

    input:
    file input

    output:
    file 'converted_metadata.tsv'

    """
    python $TOOL_FOLDER/convert_metadata.py $input converted_metadata.tsv
    """
}

// Here we are going to visualize the data
process baselineCorrection {
    publishDir "./nf_output", mode: 'copy'

    conda "$TOOL_FOLDER/conda_maldiquant.yml"

    input:
    file input_file 

    output:
    file 'baselinecorrected/*.mzML'

    """
    mkdir baselinecorrected
    Rscript $TOOL_FOLDER/baselineCorrection.R $input_file baselinecorrected/${input_file}
    """
}

process mergeInputSpectra {
    publishDir "./nf_output", mode: 'copy'

    conda "$TOOL_FOLDER/conda_env.yml"

    input:
    file "input_spectra/*"

    output:
    file 'merged/*.mzML'
    val 1

    """
    mkdir merged

    # Get min, max m/z values from params
    min_mz_flag=""
    max_mz_flag=""
    if [ ! -z "${params.min_mz}" ]; then
        min_mz_flag="--min_mz ${params.min_mz}"
        echo "mergeInputSpectra() Using min_mz: ${params.min_mz}"
    fi
    if [ ! -z "${params.max_mz}" ]; then
        max_mz_flag="--max_mz ${params.max_mz}"
        echo "mergeInputSpectra() Using max_mz: ${params.max_mz}"
    fi

    python $TOOL_FOLDER/merge_spectra.py \
    input_spectra \
    merged \
    --merge_replicates ${params.merge_replicates} \
    \$min_mz_flag \$max_mz_flag
    """
}


workflow {
    input_metadata_ch = Channel.fromPath(params.input_metadata)
    input_spectra_ch = Channel.fromPath(params.input_spectra_folder)

    showMetadata(input_metadata_ch)

    // Processing data
    _spectra_json_ch = processInputDataAndMetadata(input_metadata_ch, input_spectra_ch)

    getExistingNames()

    input_params_ch = Channel.fromPath(params.OMETAPARAM_YAML)

    // Now we will process the data like we did in analysis workflow by doing baseline normalization

    // Doing baseline correction
    input_mzml_files_ch = Channel.fromPath(params.input_spectra_folder + "/*.mzML")
    baseline_query_spectra_ch = baselineCorrection(input_mzml_files_ch)

    // Doing merging of spectra
    (merged_spectra_ch, dummy) = mergeInputSpectra(baseline_query_spectra_ch.collect())
    
    // Doing Deposition
    depositSpectrum(_spectra_json_ch, input_params_ch, getExistingNames.out.existing_names, dummy)
}