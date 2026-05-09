targetScope = 'resourceGroup'

@description('Azure region for all resources.')
param location string = resourceGroup().location

@description('Container App name.')
param appName string = 'telegram-groww-ai-bot'

@description('Container Apps managed environment name.')
param environmentName string = '${appName}-env'

@description('Container image to run, for example ghcr.io/owner/repo:sha.')
param image string

@description('Globally unique storage account name. Lowercase letters and numbers only.')
param storageAccountName string = toLower('st${uniqueString(resourceGroup().id, appName)}')

@description('Azure Files share used for SQLite and the Telethon session.')
param fileShareName string = 'botdata'

@description('Storage binding name inside the Container Apps environment.')
param storageMountName string = 'botdata'

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
  name: storageAccountName
  location: location
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
  name: fileShareName
  properties: {
    shareQuota: 1
    enabledProtocols: 'SMB'
  }
}

resource managedEnv 'Microsoft.App/managedEnvironments@2023-05-01' = {
  name: environmentName
  location: location
  properties: {}
}

resource envStorage 'Microsoft.App/managedEnvironments/storages@2023-05-01' = {
  parent: managedEnv
  name: storageMountName
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
  name: appName
  location: location
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
              volumeName: 'botdata'
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
          name: 'botdata'
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
