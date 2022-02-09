# run the kubernetes agent

prefect agent kubernetes install -k INSERT_API_KEY_HERE --rbac --service-account-name gcp-workload-identity | k apply -f -
