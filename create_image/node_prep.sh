#!/bin/bash

yum -y install nfs-utils
systemctl enable rpcbind
systemctl enable nfs-server
systemctl enable nfs-lock
systemctl enable nfs-idmap
systemctl start rpcbind
systemctl start nfs-server
systemctl start nfs-lock
systemctl start nfs-idmap

ln -s /opt/intel/impi/2017.2.174/intel64/bin/mpivars.sh /etc/profile.d/mpivars.sh

tar -xzf hemelb.tar.gz
cp -vr hemelb/bin hemelb/share /usr/
rm -rf hemelb hemelb.tar.gz

# Disable requiretty to allow run sudo within scripts
#sed -i -e 's/Defaults    requiretty.*/ #Defaults    requiretty/g' /etc/sudoers
