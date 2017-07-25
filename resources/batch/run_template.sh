#!/usr/bin/env bash

# Read the Batch env vars that were saved by the coordination script
. az_batch_env.txt

# Set up MPI because the PATH etc isn't passed through by sudo
. /opt/intel/impi/2017.2.174/intel64/bin/mpivars.sh

# Set up Intel MPI to use RDMA network
export MPI_ROOT=$I_MPI_ROOT
export I_MPI_FABRICS=shm:dapl
export I_MPI_DAPL_PROVIDER=ofa-v2-ib0
export I_MPI_DYNAMIC_CONNECTION=0

run_dir=$AZ_BATCH_TASK_SHARED_DIR/share

cd $run_dir

{input_prep_commands}

mpirun -np {num_cores} -ppn {cores_per_node} -hosts $AZ_BATCH_HOST_LIST hemelb -in {config_xml} > stdout.txt 2> stderr.txt

function upld {{
    timestamp=$(TZ=GMT date '+%a, %d %h %Y %H:%M:%S %Z')
    curl -H "x-ms-blob-type: BlockBlob" -H "Date: $timestamp" -H "x-ms-version: 2015-07-08" -T $1 "{output_container_url}/$1?{output_sas}"
}}

upld stdout.txt
upld stderr.txt
upld results/report.txt
upld results/report.xml
for xtr in results/Extracted/*.xtr; do
    upld $xtr
done
