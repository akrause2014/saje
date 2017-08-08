#!/usr/bin/env bash

# Die on unhandled error
set -e

# Save the values of all the batch environment variables as they are
# not passed through the sudo -u _azbatch
env | perl -ne 'if(s/^(AZ_BATCH\w*)=(.*)$/export \1="\2"/) {print;}' > az_batch_env.txt

master_addr_port=(${AZ_BATCH_MASTER_NODE//:/ })
master_addr=${master_addr_port[0]}
mnt=$AZ_BATCH_TASK_SHARED_DIR
job_dir=`realpath ../..`

# This file will be run by the job release task to destroy the NFS share
unco=$job_dir/uncoordinate.sh
touch $unco
chmod a+x $unco
    
if $AZ_BATCH_IS_CURRENT_NODE_MASTER; then
    # Have this one be NFS server
    nfs_share="10.0.0.0/24:$AZ_BATCH_TASK_SHARED_DIR/share"
    
    # Store the command to remove the export
    echo "exportfs -u $nfs_share" > $unco

    # Setup share and export it
    mkdir $AZ_BATCH_TASK_SHARED_DIR/share
    chmod 777 -R $AZ_BATCH_TASK_SHARED_DIR/share
    exportfs -o rw,sync,no_root_squash,no_all_squash $nfs_share
    
else
    # All other nodes are NFS clients

    # Store command to unmount shared dir
    echo "umount -f $AZ_BATCH_TASK_SHARED_DIR/share" > $unco

    mkdir -p $AZ_BATCH_TASK_SHARED_DIR/share
    # Loop until we pick up the NFS server's export
    until showmount -e $master_addr | fgrep "$AZ_BATCH_TASK_SHARED_DIR/share"; do
	echo "Can't find mount $master_addr:$AZ_BATCH_TASK_SHARED_DIR/share"
	sleep 10
    done

    # Connect to it
    mount -t nfs $master_addr:$AZ_BATCH_TASK_SHARED_DIR/share    $AZ_BATCH_TASK_SHARED_DIR/share
fi

exit
