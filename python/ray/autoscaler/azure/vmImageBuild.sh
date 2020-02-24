#!/bin/bash

# Resource group name - we are using myImageBuilderRG in this example
imageResourceGroup=marcozo-ray-vm-image-10
# Datacenter location - we are using West US in this example
location=WestUS2
# Name for the image - we are using myBuilderImage in this example
imageName=ray-dev-image
# Run output name
runOutputName=ray-dev-image-run
# VM Image Name
imageName=ray-dev-image-01

subscriptionID=6187b663-b744-4d24-8226-7e66525baf8f

echo Create the resource group
az group create -n $imageResourceGroup -l $location

echo Set permissions on the resource group for the image builder
az role assignment create \
    --assignee cf32a0cc-373c-47c9-9156-0db11f6a6dfc \
    --role Contributor \
    --scope /subscriptions/$subscriptionID/resourceGroups/$imageResourceGroup

az role assignment create \
    --assignee ef511139-6170-438e-a6e1-763dc31bdf74 \
    --role Contributor \
    --scope /subscriptions/$subscriptionID/resourceGroups/$imageResourceGroup

# Apply the values
sed -i -e "s/<subscriptionID>/$subscriptionID/g" vmImageDevTemplate.json
sed -i -e "s/<rgName>/$imageResourceGroup/g" vmImageDevTemplate.json
sed -i -e "s/<region>/$location/g" vmImageDevTemplate.json
sed -i -e "s/<imageName>/$imageName/g" vmImageDevTemplate.json
sed -i -e "s/<runOutputName>/$runOutputName/g" vmImageDevTemplate.json

echo Submit the image configuration
az resource create \
    --resource-group $imageResourceGroup \
    --properties @vmImageDevTemplate.json \
    --is-full-object \
    --resource-type Microsoft.VirtualMachineImages/imageTemplates \
    -n $imageName

# Cleanup if above failed
# az resource delete \
#    --resource-group $imageResourceGroup \
#    --resource-type Microsoft.VirtualMachineImages/imageTemplates \
#    -n $imageName

echo Start the image build
az resource invoke-action \
     --resource-group $imageResourceGroup \
     --resource-type  Microsoft.VirtualMachineImages/imageTemplates \
     -n $imageName \
     --action Run