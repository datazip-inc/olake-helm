# OLake Helm Chart

<h1 align="center" style="border-bottom: none">
    <a href="https://datazip.io/olake" target="_blank">
        <img alt="olake" src="https://github.com/user-attachments/assets/d204f25f-5289-423c-b3f2-44b2194bdeaf" width="100" height="100"/>
    </a>
    <br>OLake
</h1>

<p align="center">Fastest open-source tool for replicating Databases to Apache Iceberg or Data Lakehouse. ⚡ Efficient, quick and scalable data ingestion for real-time analytics. Starting with MongoDB. Visit <a href="https://olake.io/" target="_blank">olake.io/docs</a> for the full documentation, and benchmarks</p>

## Prerequisites

-   Kubernetes 1.19+
-   Helm 3.2.0+

## Installation

### Step 1: Add the OLake Helm Repository
```bash
helm repo add olake https://datazip-inc.github.io/olake-helm
helm repo update
```

### Step 2: Install the Chart
```bash
# Using default values.yaml file
helm install olake olake/olake
```

Just like any typical Helm chart, a values file could be crafted that would define/override any of the values exposed into the default [values.yaml](https://github.com/datazip-inc/olake-helm/helm/olake/values.yaml).
```bash
# Using the crafted my-values.yaml file
helm install --values my-values.yaml olake olake/olake
```

### Step 3: Access the OLake UI
```bash
# The UI service port (8080) needs to be forwarded to the local machine
kubectl port-forward svc/olake-ui 8000:8000

# Now, open a browser and head over to http://localhost:8000
```
Perform the login with the default credentials: `admin` / `password`.

**Note:** If OLake is installed with Ingress enabled, port-forwarding is not necessary. Access the application using the configured Ingress hostname.

## Upgrading

```bash
# Upgrade to latest version
helm upgrade olake ./helm/olake

# Upgrade with new values
helm upgrade olake ./helm/olake -f new-values.yaml
```

## Contributing

We ❤️ contributions! Check our [Bounty Program](https://olake.io/docs/community/issues-and-prs#goodies).

- UI contributions: [CONTRIBUTING.md](../CONTRIBUTING.md)
- Core contributions: [OLake Main Repository](https://github.com/datazip-inc/olake)
- Documentation: [OLake Docs Repository](https://github.com/datazip-inc/olake-docs)

## Support

- [GitHub Issues](https://github.com/datazip-inc/olake-ui/issues)
- [Slack Community](https://join.slack.com/t/getolake/shared_invite/zt-2utw44do6-g4XuKKeqBghBMy2~LcJ4ag)
- [Documentation](https://olake.io/docs)