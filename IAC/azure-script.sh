#!/bin/sh
#az login

echo -n "Please provide string value for 'subscriptionID' : "; read subID
echo -n "Please provide string value for 'resourceGroupName' : " ; read resName
echo -n "Please provide string value for 'dataLakeName' : " ; read dlName
#echo -n "Please provide string value for 'dataLakePath' : " ; read dlPath # uncomment if path can change
dlPath='/messages'
echo -n "Please provide string value for 'evenHubNamespaceName' : " ; read ehNsName
echo -n "Please provide string value for 'evenHubName' : " ; read ehName
ehID=75d8f599-b285-42f0-974a-fc2c9bef692e

sed "s/NAME/$dlName/" ./dlsTemplate.json > dlsTemplate-temp.json
sed -e "s/NSNAME/$ehNsName/; s/EHNAME/$ehName/; s/SUBID/$subID/; s/DLSNAME/$dlName/; s!DLSPATH!$dlPath!" ./eventHub_template.json > eventHub_template-temp.json
az group create --location westeurope --name $resName
az group deployment create --resource-group  $resName --name deployDataLake --template-file dlsTemplate-temp.json
az dls fs create --account $dlName --path $dlPath --folder
az dls fs access set-entry --path $dlPath --account $dlName --acl-spec "user:$ehID:rwx"
az dls fs access set-entry --path $dlPath --account $dlName --acl-spec "default:user:$ehID:rwx"
az dls fs access set-entry --path / --account $dlName --acl-spec "user:$ehID:rwx"
az dls fs access set-entry --path / --account $dlName --acl-spec "default:user:$ehID:rwx"
az group deployment create --resource-group $resName --name deploytest --template-file eventHub_template-temp.json
