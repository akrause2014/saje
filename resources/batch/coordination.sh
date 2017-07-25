#!/usr/bin/env bash

# Save the values of all the batch environment variables as they are
# not passed through the sudo -u _azbatch
env | perl -ne 'if(s/^(AZ_BATCH\w*)=(.*)$/export \1="\2"/) {print;}' > az_batch_env.txt

master_addr_port=(${AZ_BATCH_MASTER_NODE//:/ })
master_addr=${master_addr_port[0]}
mnt=$AZ_BATCH_TASK_SHARED_DIR

if $AZ_BATCH_IS_CURRENT_NODE_MASTER; then
    # Have this one be NFS server
    mkdir $AZ_BATCH_TASK_SHARED_DIR/share
    chmod 777 -R $AZ_BATCH_TASK_SHARED_DIR/share
    echo "$AZ_BATCH_TASK_SHARED_DIR/share      10.0.0.0/24(rw,sync,no_root_squash,no_all_squash)" > /etc/exports
    systemctl restart nfs-server
    cd $AZ_BATCH_TASK_SHARED_DIR/share

    # Download / unpack?
    
else
    # All other nodes are NFS clients
    # Connect with NFS server
    mkdir -p $AZ_BATCH_TASK_SHARED_DIR/share
    echo "$master_addr:$AZ_BATCH_TASK_SHARED_DIR/share    $AZ_BATCH_TASK_SHARED_DIR/share   nfs defaults 0 0::" >> /etc/fstab
    while :
    do
        echo "Looping"
        mount -a
        mountpoint -q $AZ_BATCH_TASK_SHARED_DIR/share
        if [ $? -eq 0 ]; then
            break
        else
            sleep 10
        fi
    done
fi

exit
