# Azure Pipelines

This folder contains the code required to create the Azure Pipelines for the CI/CD of the Ray project.
Keep in mind that this could be outdated.
Please check the following links if you want to update the procedure.
- [Azure virtual machine scale set agents](https://docs.microsoft.com/en-us/azure/devops/pipelines/agents/scale-set-agents?view=azure-devops)
- [Repo for the Azure Pipelines images](https://github.com/actions/virtual-environments) 

## Self-hosted Linux Agents

### Create VM Image

The following are the instructions to build the VM image of a self-hosted linux agent using a Virtual Hard Drive (VHD).
The image will be the same one that is used by the Microsoft-hosted linux agents. This approach
simplifies the maintenance and also allows to keep the pipelines code compatible with both
types of agents.

Requirements:
- Install packer        : https://www.packer.io/downloads.html
- Install azure-cli     : https://docs.microsoft.com/en-us/cli/azure/install-azure-cli?view=azure-cli-latest

Steps for Mac and Ubuntu:
- Clone the GitHub Actions virtual environments repo: `git clone https://github.com/actions/virtual-environments.git`
- Move into the folder of the repo cloned aboved: `pushd virtual-environments/images/linux`
- Log in your azure account: `az login`
- Set your Azure subscription id and tenant id: 
    - Check your subscriptions: `az account list --output table`
    - Set your default (replace your Subscription id in the command): `az account set -s {Subscription Id}`
    - Get the subscription id: `SUBSCRIPTION_ID=$(az account show --query 'id' --output tsv)`
    - Get the tenant id: `TENANT_ID=$(az account show --query 'tenantId' --output tsv)`
- Select the azure location: `AZURE_LOCATION="eastus"`
- Create and select the name of the resource group where the Azure resources will be created:
    - Set the group: `RESOURCE_GROUP_NAME="RayADOAgents"`
    - Try to create the group. If the resource group exists, the details for it will be returned: `az group create -n $RESOURCE_GROUP_NAME -l $AZURE_LOCATION`
- Create a Storage Account:
    - Set Storage Account name: `STORAGE_ACCOUNT_NAME="rayadoagentsimage"`
    - Create the Storage Account: `az storage account create -n $STORAGE_ACCOUNT_NAME -g $RESOURCE_GROUP_NAME -l $AZURE_LOCATION --sku "Standard_LRS"`
- Create a Service Principal. If you have an existing Service Principal, it can also be used instead of creating a new one:
    - Set the object id: `OBJECT_ID="http://rayadoagents"`
    - Create client and get secret: `CLIENT_SECRET=$(az ad sp create-for-rbac -n $OBJECT_ID --scopes="/subscriptions/${SUBSCRIPTION_ID}" --query 'password' -o tsv)`. If the Principal already exist, this command returns the id of the role assignment. Please use your old password. Or delete the existing Principal with `az ad sp delete --id $OBJECT_ID`.
    - Get client id: `CLIENT_ID=$(az ad sp show --id $OBJECT_ID --query 'appId' -o tsv)`
- Set Install password: `INSTALL_PASSWORD="$CLIENT_SECRET"`
- Create a Key Vault. If you have an existing Service Principal, it can also be used instead of creating a new one:
    - Set Key Vault name: `KEY_VAULT_NAME="ray-agent-secrets"`
    - Create the Key Vault: `az keyvault create --name $KEY_VAULT_NAME --resource-group $RESOURCE_GROUP_NAME --location $AZURE_LOCATION`. If the Key Vault exist, this command returns the info.
- Set a GitHub Personal Access Token with rights to download:
    - Set Key Pair name: `GITHUB_FEED_TOKEN_NAME="raygithubfeedtoken"`
    - Upload your PAT to the vault (replace your token in the command):`az keyvault secret set --name $GITHUB_FEED_TOKEN_NAME --vault-name $KEY_VAULT_NAME --value "{GitHub Token}"`
    - Get PAT from the Vault: `GITHUB_FEED_TOKEN=$(az keyvault secret show --name $GITHUB_FEED_TOKEN_NAME --vault-name $KEY_VAULT_NAME --query 'value' --output tsv)`
- Create the Managed Disk image:
    - Create a packer variables file: 
    ```
cat << EOF > azure-variables.json
{
    "client_id": "${CLIENT_ID}",
    "client_secret": "${CLIENT_SECRET}",
    "subscription_id": "${SUBSCRIPTION_ID}",
    "tenant_id": "${TENANT_ID}",
    "object_id": "${OBJECT_ID}",
    "location": "${AZURE_LOCATION}",
    "resource_group": "${RESOURCE_GROUP_NAME}",
    "storage_account": "${STORAGE_ACCOUNT_NAME}",
    "install_password": "${INSTALL_PASSWORD}",
    "github_feed_token": "${GITHUB_FEED_TOKEN}"
}
EOF
    ```
    - Execute packer build: `packer build -var-file=azure-variables.json ubuntu1604.json`

For more details (Check the following doc in the virtual environment repo)[https://github.com/actions/virtual-environments/blob/master/help/CreateImageAndAzureResources.md].


### Create Agent Pool

#### 1. Create the Virtual Machine Scale Set (VMSS)

Creation of the VMSS is done using the Azure Resource Manager (ARM) template, `image/agentpool.json`. The following are important fixed parameters that could be changed:

| Parameter         | Description                                                                |
| -------------     | -------------                                                              |
| vmssName          | name of the VMSS to be created                                             |
| instanceCount     | number of VMs to create in initial deployemnt (can be changed later)       |

Steps for Mac and Ubuntu:
- Log in your azure account: `az login`
- Set your Azure subscription id and tenant id: 
    - Check your subscriptions: `az account list --output table`
    - Set your default: `az account set -s {Subscription Id}`
    - Get the subscription id: `SUBSCRIPTION_ID=$(az account show --query 'id' --output tsv)`
    - Get the tenant id: `TENANT_ID=$(az account show --query 'tenantId' --output tsv)`
    - Set Storage Account name (same that is above): `STORAGE_ACCOUNT_NAME="rayadoagentsimage"`
- Select the azure location: `AZURE_LOCATION="eastus"`
- Create and select the name of the resource group where the Azure resources will be created:
    - Set the group: `RESOURCE_GROUP_NAME="RayADOAgents"`
    - Try to create the group. If the resource group exists, the details for it will be returned: `az group create -n $RESOURCE_GROUP_NAME -l $AZURE_LOCATION`
- Create a Key Vault. If you have an existing Service Principal, it can also be used instead of creating a new one:
    - Set Key Vault name: `KEY_VAULT_NAME="ray-agent-secrets"`
    - Create the Key Vault: `az keyvault create --name $KEY_VAULT_NAME --resource-group $RESOURCE_GROUP_NAME --location $AZURE_LOCATION`. If the Key Vault exist, this command returns the info.
- Create a Key Pair in the Vault: 
    - Set Key Pair name: `SSH_KEY_PAIR_NAME="rayagentadminrsa"`
    - Set Key Pair name: `SSH_KEY_PAIR_NAME_PUB="${SSH_KEY_PAIR_NAME}pub"`
    - Set SSH key pair file path: `SSH_KEY_PAIR_PATH="$HOME/.ssh/$SSH_KEY_PAIR_NAME"`
    - Create the SSH key pair: `ssh-keygen -m PEM -t rsa -b 4096 -f $SSH_KEY_PAIR_PATH`
    - Upload your key pair to the vault:
        - Public part to be used by the VMs: `az keyvault secret set --name $SSH_KEY_PAIR_NAME_PUB --vault-name $KEY_VAULT_NAME --file ${SSH_KEY_PAIR_PATH}.pub`
        - (Optional) Private part to be used by the VMs: `az keyvault secret set --name $SSH_KEY_PAIR_NAME --vault-name $KEY_VAULT_NAME --file $SSH_KEY_PAIR_PATH`
    - Get public part from the Vault: `SSH_KEY_PUB=$(az keyvault secret show --name $SSH_KEY_PAIR_NAME_PUB --vault-name $KEY_VAULT_NAME --query 'value' --output tsv)`
- Create the VMSS: 
    - Set the Subnet Id of the subnet where the VMs must be: `SUBNET_ID="{Subnet Id}"`
    - Set the VMSS name: `VMSS_NAME="RayPipelineAgentPoolStandardF16sv2"`
    - Set the instance count: `INSTANCE_COUNT="2"`
    - Get Reader role definition: `ROLE_DEFINITION_ID=$(az role definition list --subscription $SUBSCRIPTION_ID --query "([?roleName=='Reader'].id)[0]" --output tsv)`
    - Set the source image VHD NAME (assuming the latest): `SOURCE_IMAGE_VHD_NAME="$(az storage blob list --subscription $SUBSCRIPTION_ID --account-name $STORAGE_ACCOUNT_NAME -c images --prefix pkr --query 'sort_by([], &properties.creationTime)[-1].name' --output tsv)"`
    - Set the source image VHD URI: `SOURCE_IMAGE_VHD_URI="https://${STORAGE_ACCOUNT_NAME}.blob.core.windows.net/images/${SOURCE_IMAGE_VHD_NAME}"`
    - Create the VM scale set: `az group deployment create --resource-group $RESOURCE_GROUP_NAME --template-file image/agentpool.json --parameters "vmssName=$VMSS_NAME" --parameters "instanceCount=$INSTANCE_COUNT" --parameters "sourceImageVhdUri=$SOURCE_IMAGE_VHD_URI" --parameters "sshPublicKey=$SSH_KEY_PUB" --parameters "location=$AZURE_LOCATION" --parameters "subnetId=$SUBNET_ID" --parameters "keyVaultName=$KEY_VAULT_NAME" --parameters "tenantId=$TENANT_ID" --parameters "roleDefinitionId=$ROLE_DEFINITION_ID" --name $VMSS_NAME`

#### 2. Create the Agent Pool in Azure DevOps

Open Azure DevOps > "Project Settings" (bottom right) > "Agent Pools" > "New Agent Pool" > "Add pool" to create a new agent pool. Enter the agent pool's name, which must match the value you provided VMSS_NAME (see steps above).

Make sure your admin is added as the administrator in ADO in 2 places:
- Azure DevOps > "Project Settings" (bottom right) > "Agent Pools" > [newly created agent poool] >"Security Tab" and
- Azure DevOps > bizair > Organization Settings > Agent Pools > Security

#### 3. Connect VMs to pool

Steps for Mac and Ubuntu:
- Copy some files to fix some errors in the generation of the agent image:
    - The error is due to a issue with the packer script. It's not downloading a postgresql installation script.
    In order to check if the image was not fully build, connect to the vm using ssh (see steps below), and run this: `INSTALLER_SCRIPT_FOLDER="/imagegeneration/installers" source /imagegeneration/installers/test-toolcache.sh`.
    If you don't get any error message, skip the following 3 steps.
    - Tar the image folder: `tar -zcvf image.tar.gz image`
    - Set Key Pair name: `export SSH_KEY_PAIR_NAME="rayagentadminrsa"`
    - Set SSH key pair file path: `export SSH_KEY_PAIR_PATH="$HOME/.ssh/$SSH_KEY_PAIR_NAME"`
    - Set the IP of your VM: `export IP={my.ip}`
    - Copy to each of your machines in the Scale set: `scp -o "IdentitiesOnly=yes" -i $SSH_KEY_PAIR_PATH ./image.tar.gz agentadmin@"${IP}":/home/agentadmin`
    - Delete the tar: `rm image.tar.gz`
- Connect using ssh: 
    - Open a ssh tunnel: `ssh -o "IdentitiesOnly=yes" -i $SSH_KEY_PAIR_PATH agentadmin@"${IP}"`
- Fix the image: 
    - Untar the image file: `tar zxvf ./image.tar.gz`
    - Switch to root: `sudo -s`
    - In your machine get PAT from the Vault: 
        - Set Key Pair name: `export GITHUB_FEED_TOKEN_NAME="raygithubfeedtoken"`
        - Set Key Vault name: `export KEY_VAULT_NAME="ray-agent-secrets"`
        - Get the token: `az keyvault secret show --name $GITHUB_FEED_TOKEN_NAME --vault-name $KEY_VAULT_NAME --query 'value' --output tsv`
    - Set the PAT in your ssh session: `export GITHUB_FEED_TOKEN={ GitHub Token }`
    - Add agentadmin to the root group: `sudo gpasswd -a agentadmin root`
    - Install missing part: `source ./image/fix-image.sh`
    - Set the system up:
    ```
    export GITHUB_FEED_TOKEN={ GitHub Token }
    export DEBIAN_FRONTEND=noninteractive
    export METADATA_FILE="/imagegeneration/metadatafile"
    export HELPER_SCRIPTS="/imagegeneration/helpers"
    export INSTALLER_SCRIPT_FOLDER="/imagegeneration/installers"
    export BOOST_VERSIONS="1.69.0"
    export BOOST_DEFAULT="1.69.0"
    export AGENT_TOOLSDIRECTORY=/opt/hostedtoolcache
    mkdir -p $INSTALLER_SCRIPT_FOLDER/node_modules
    sudo chmod --recursive a+rwx $INSTALLER_SCRIPT_FOLDER/node_modules
    sudo chown -R agentadmin:root $INSTALLER_SCRIPT_FOLDER/node_modules
    source $INSTALLER_SCRIPT_FOLDER/hosted-tool-cache.sh
    source $INSTALLER_SCRIPT_FOLDER/test-toolcache.sh
    chown -R agentadmin:root $AGENT_TOOLSDIRECTORY
    echo 'export NVM_DIR="$HOME/.nvm"
    [ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"  # This loads nvm
    [ -s "$NVM_DIR/bash_completion" ] && \. "$NVM_DIR/bash_completion"  # This loads nvm bash_completion
    AGENT_TOOLSDIRECTORY="/opt/hostedtoolcache/"' >> ~/.bashrc
    ```
- Go to the [New Agent] option in the pool and follow the instructions for linux agents:
    - Download the agent: `wget https://vstsagentpackage.azureedge.net/agent/2.170.1/vsts-agent-linux-x64-2.170.1.tar.gz`
    - Create and move to a directory for the agent: `mkdir myagent && cd myagent`
    - Untar the agent: `tar zxvf ../vsts-agent-linux-x64-2.170.1.tar.gz`
    - Configure the agent: `./config.sh`
        - Accept the license.
        - Enter your organization URL.
        - Enter your ADO PAT.
        - Set a Personal Access Token:
            - Set Key Pair name: `ADO_TOKEN_NAME="rayagentadotoken"`
            - Upload your PAT to the vault (replace your token in the command):`az keyvault secret set --name $ADO_TOKEN_NAME --vault-name $KEY_VAULT_NAME --value "{ADO Token}"`
        - Enter the agent pool's name, which must match the value you provided VMSS_NAME (see steps above) 
        - Enter or accept agent name.
    - Install the ADO Agent as a service and start it:
        - `sudo ./svc.sh install`
        - `sudo ./svc.sh start`
        - `sudo ./svc.sh status`
    - Allow agent user to access Docker:
        - `export VM_ADMIN_USER="agentadmin"`
        - `sudo gpasswd -a "${VM_ADMIN_USER}" docker`
        - `sudo chmod ga+rw /var/run/docker.sock`
        - Update group permissions so docker is available without logging out and back in: `newgrp - docker`
        - Test docker: `docker run hello-world`
        - `export VM_ADMIN_USER="agentadmin"`
        - If `/home/"$VM_ADMIN_USER"/.docker` exist:
            - `sudo chown "$VM_ADMIN_USER":docker /home/"$VM_ADMIN_USER"/.docker -R`
            - `sudo chmod ga+rwx "$HOME/.docker" -R`
        - Create a symlink:
            - `mkdir -p /home/agentadmin/myagent/_work`
            - `ln -s /opt/hostedtoolcache /home/agentadmin/myagent/_work/_tool`

### Deleting an Agent Pool

1. Open Azure DevOps > Settings > Agent Pools > find pool to be removed and click "..." > Delete
2. Open Azure Portal > Key Vaults > ray-agent-secrets > Access Policies > delete the access policy assigned to the VMSS to be deleted
3. Open Azure Portal > All Resources > type the VMSS name into the search bar > select and delete the following resources tied to that VMSS:
  - public IP address
  - load balancer
  - the VMSS itself

### Useful Commands

```
# Get connection info for all VMSS instances
az vmss list-instance-connection-info -g $RESOURCE_GROUP_NAME --name $VMSS_NAME

# SSH to a VMSS instance
ssh -o "IdentitiesOnly=yes" -i $SSH_KEY_PAIR_PATH agentadmin@{ PUBLIC IP}

# Download agentadmin private SSH key (formatting is lost if key is pulled from the UI)
az keyvault secret download --file $SSH_KEY_PAIR_PATH --vault-name $KEY_VAULT_NAME --name $SSH_KEY_PAIR_NAME


az keyvault secret download --file ~/downloads/PAT --vault-name $KEY_VAULT_NAME --name $ADO_TOKEN_NAME
```
