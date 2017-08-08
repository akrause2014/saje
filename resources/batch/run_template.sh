#!/usr/bin/env bash
# Script for job {job_id}

num_nodes={num_nodes}
num_cores={num_cores}
cores_per_node={cores_per_node}

# Set up MPI because the PATH etc isn't passed through by sudo
. /opt/intel/impi/2017.2.174/intel64/bin/mpivars.sh

# Set up Intel MPI to use RDMA network
export MPI_ROOT=$I_MPI_ROOT
export I_MPI_FABRICS=shm:dapl
export I_MPI_DAPL_PROVIDER=ofa-v2-ib0
export I_MPI_DYNAMIC_CONNECTION=0

if [[ $num_nodes == 1 ]]; then
    export AZ_BATCH_HOST_LIST=localhost
else
    # Read the Batch env vars that were saved by the coordination script
    . az_batch_env.txt
    cd $AZ_BATCH_TASK_SHARED_DIR/share
fi

function upld {{
    timestamp=$(TZ=GMT date '+%a, %d %h %Y %H:%M:%S %Z')
    curl -H "x-ms-blob-type: BlockBlob" -H "Date: $timestamp" -H "x-ms-version: 2015-07-08" -T $1 "{output_container_url}/$1?{output_sas}"
}}

# Get inputs
{input}

# Execute commands
{commands}

# Store outputs
shopt -s nullglob
{output}

