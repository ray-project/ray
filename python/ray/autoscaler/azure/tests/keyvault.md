# Azure Autoscaler Test Credentials

## Create a new SSH key pair

```bash
ssh-keygen -t rsa -b 4096 -f ~/.ssh/ray-autoscaler-tests-ssh-key
```

## Create a keyvault and configure access

```bash
# (optional) create a new resource group
az group create --resource-group ray-dev -l westus2

# Create the keyvault
az keyvault create -name ray-dev-kv --resource-group ray-dev -l westus2

# Grant current users "Key Vault Secrets Officer" permissions
az role assignment create --assignee $(az ad signed-in-user show --query id -o tsv) --role "Key Vault Secrets Officer" --scope $(az keyvault show --name ray-dev-kv --query id -o tsv)
```

## Upload SSH keys

```bash
az keyvault secret set --vault-name ray-dev-kv --name ray-autoscaler-tests-ssh-key --value "$(base64 -w 0 ~/.ssh/ray-autoscaler-tests-ssh-key)"
az keyvault secret set --vault-name ray-dev-kv --name ray-autoscaler-tests-ssh-key-pub --value "$(base64 -w 0 ~/.ssh/ray-autoscaler-tests-ssh-key.pub)"
```
