targetScope = 'resourceGroup'

@description('Azure region for all resources.')
param location string = resourceGroup().location

@description('Client code used in resource names. Use lowercase letters/numbers only for storage compatibility.')
param clientCode string = 'techron'

@description('Project code used in resource names. Use lowercase letters/numbers only for storage compatibility.')
param projectCode string = 'growwbot'

@description('Deployment environment code, for example dev, test, uat, or prod.')
param environment string = 'prod'

@description('Optional Container App name override. Defaults to ca-{clientCode}-{projectCode}-{environment}.')
param appName string = ''

@description('Optional Container Apps managed environment name override. Defaults to cae-{clientCode}-{projectCode}-{environment}.')
param environmentName string = ''

@description('Container image to run, for example ghcr.io/owner/repo:sha.')
param image string

@description('Optional globally unique storage account name override. Lowercase letters and numbers only.')
param storageAccountName string = ''

@description('Optional Azure Files share name override. Defaults to fs-{clientCode}-{projectCode}-{environment}.')
param fileShareName string = ''

@description('Optional storage binding name inside the Container Apps environment.')
param storageMountName string = ''

@description('Container registry server. Use ghcr.io for GitHub Container Registry.')
param registryServer string = 'ghcr.io'

@description('Container registry username. Leave blank for public images.')
param registryUsername string = ''

@secure()
@description('Container registry password/token. Leave blank for public images.')
param registryPassword string = ''

@secure()
@description('Container App secrets as name/value pairs. Example: { "openai-api-key": "..." }.')
param appSecrets object = {}

@description('Container environment variables. Use value or secretRef fields.')
param appEnv array = [
  {
    name: 'DB_PATH'
    value: '/data/trading_bot.db'
  }
  {
    name: 'SESSION_PATH'
    value: '/data/session_name'
  }
]

@description('Additional tags to apply to provisioned Azure resources.')
param tags object = {}

var effectiveAppName = empty(appName) ? 'ca-${clientCode}-${projectCode}-${environment}' : appName
var effectiveEnvironmentName = empty(environmentName) ? 'cae-${clientCode}-${projectCode}-${environment}' : environmentName
var effectiveStorageAccountName = empty(storageAccountName)
  ? toLower('st${take(clientCode, 6)}${take(projectCode, 8)}${environment}${take(uniqueString(resourceGroup().id, clientCode, projectCode, environment), 4)}')
  : storageAccountName
var effectiveFileShareName = empty(fileShareName) ? 'fs-${clientCode}-${projectCode}-${environment}' : fileShareName
var effectiveStorageMountName = empty(storageMountName) ? 'afs-${clientCode}-${projectCode}-${environment}' : storageMountName
var dataVolumeName = 'vol-botdata'
var resourceTags = union({
  client: clientCode
  project: projectCode
  environment: environment
  workload: 'telegram-groww-ai-bot'
  managedBy: 'bicep'
}, tags)
var registrySecretName = 'registry-password'
var appSecretArray = [
  for secret in items(appSecrets): {
    name: secret.key
    value: string(secret.value)
  }
]
var registrySecrets = empty(registryPassword) ? [] : [
  {
    name: registrySecretName
    value: registryPassword
  }
]
var registryConfig = empty(registryPassword) ? [] : [
  {
    server: registryServer
    username: registryUsername
    passwordSecretRef: registrySecretName
  }
]

resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: effectiveStorageAccountName
  location: location
  tags: resourceTags
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'
  properties: {
    allowBlobPublicAccess: false
    minimumTlsVersion: 'TLS1_2'
  }
}

resource fileService 'Microsoft.Storage/storageAccounts/fileServices@2023-05-01' = {
  parent: storage
  name: 'default'
}

resource fileShare 'Microsoft.Storage/storageAccounts/fileServices/shares@2023-05-01' = {
  parent: fileService
  name: effectiveFileShareName
  properties: {
    shareQuota: 1
    enabledProtocols: 'SMB'
  }
}

resource managedEnv 'Microsoft.App/managedEnvironments@2023-05-01' = {
  name: effectiveEnvironmentName
  location: location
  tags: resourceTags
  properties: {}
}

resource envStorage 'Microsoft.App/managedEnvironments/storages@2023-05-01' = {
  parent: managedEnv
  name: effectiveStorageMountName
  properties: {
    azureFile: {
      accountName: storage.name
      accountKey: storage.listKeys().keys[0].value
      shareName: fileShare.name
      accessMode: 'ReadWrite'
    }
  }
}

resource containerApp 'Microsoft.App/containerApps@2023-05-01' = {
  name: effectiveAppName
  location: location
  tags: resourceTags
  properties: {
    managedEnvironmentId: managedEnv.id
    configuration: {
      activeRevisionsMode: 'Single'
      registries: registryConfig
      secrets: concat(registrySecrets, appSecretArray)
    }
    template: {
      containers: [
        {
          name: 'bot'
          image: image
          env: appEnv
          resources: {
            cpu: json('0.25')
            memory: '0.5Gi'
          }
          volumeMounts: [
            {
              volumeName: dataVolumeName
              mountPath: '/data'
            }
          ]
        }
      ]
      scale: {
        minReplicas: 1
        maxReplicas: 1
      }
      volumes: [
        {
          name: dataVolumeName
          storageType: 'AzureFile'
          storageName: envStorage.name
        }
      ]
    }
  }
}

output containerAppName string = containerApp.name
output storageShare string = '${storage.name}/${fileShare.name}'
output persistentMountPath string = '/data'
output resourceNames object = {
  containerApp: containerApp.name
  containerAppsEnvironment: managedEnv.name
  storageAccount: storage.name
  fileShare: fileShare.name
  storageBinding: envStorage.name
}
