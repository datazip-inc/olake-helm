# OLake Kubernetes Worker

A cloud-native Temporal worker that executes OLake Jobs as Kubernetes Pods. This worker is part of the modular `olake-workers` architecture, specifically designed for Kubernetes environments.

## 🚀 Quickstart

### Prerequisites
- Go 1.24+
- [DevSpace CLI](https://www.devspace.sh/docs/getting-started/installation) installed
- Access to a Kubernetes cluster (minikube or remote cluster)
- kubectl configured to access the cluster

## 🏗️ How It Works

The OLake Kubernetes Worker executes Temporal activities as isolated Kubernetes Pods:

### Activity Types
- **Discover**: Analyzes source systems to catalog available tables/schemas
- **Test**: Validates connectivity to destination systems  
- **Sync**: Performs data replication between source and destination

### Execution Model
1. **Receives activity** from Temporal server
2. **Creates Kubernetes Pod** with appropriate container image for the task
3. **Mounts shared storage** for data exchange between activity pods
4. **Monitors pod execution** and collects results
5. **Reports results** back to Temporal workflow

## 🛠️ Development

### DevSpace Development Environment (Recommended)

DevSpace provides a streamlined development workflow with live code sync, port forwarding, and automatic deployments.

#### Setup DevSpace

1. **Navigate to worker directory:**
   ```bash
   cd olake-workers/k8s
   ```

2. **Initialize the namespace (first time only):**
   ```bash
   # Create namespace
   kubectl create namespace olake
   ```

3. **Start development environment:**
   ```bash
   # Set namespace to olake
   devspace use namespace olake

   # Start the development environment
   devspace dev
   ```

This will:
- Deploy the OLake Helm chart to the cluster
- Start a development container with Go 1.24.2
- Sync local code changes to the container in real-time
- Forward port 8000 from olakeUI to `http://localhost:8000`
- Open a terminal in the dev container

#### Development Workflow

**Inside the DevSpace container:**
```bash
# Code is synced to /app - navigate there
cd /app

# Install dependencies
go mod tidy

# Run the worker
go run main.go
```

**Access OLake UI:**
- Open `http://localhost:8000` in the browser
- Use the UI to create and monitor jobs
- Worker changes are reflected immediately

#### DevSpace Commands

```bash
# Start development (full deployment + dev container)
devspace dev

# Deploy only (without dev container)
devspace deploy

# Clean up deployment
devspace purge

# View logs
devspace logs

# Open shell in dev container
devspace enter
```

#### Configuration

DevSpace is configured via `devspace.yaml` in this directory:

- **Images**: Builds `olakego/k8s-worker` from local Dockerfile
- **Deployments**: Uses the OLake Helm chart from `../../helm/olake`
- **Dev Environment**: 
  - Worker dev container with Go 1.24.2
  - Live code sync from current directory
  - SSH access for IDE integration
- **Port Forwarding**: 
  - Port 8000 → olakeUI service (for workflow control)

#### Troubleshooting DevSpace

**Container fails to start:**
```bash
# Check pod status
kubectl get pods -n olake

# View worker logs
devspace logs worker
```

**Code sync not working:**
```bash
# Restart DevSpace
devspace dev --force-rebuild
```

**Port forwarding issues:**
```bash
# Check if olakeUI is running
kubectl get svc -n olake
kubectl get pods -l app.kubernetes.io/component=olake-ui -n olake
```

### Traditional Docker Development

If Docker-based development prefered:

```bash
# Build locally
docker build -t olakego/olake-workers:local .

# For minikube development
minikube image load olakego/olake-workers:local
```

### Project Structure

```
olake-workers/k8s/
├── main.go              # Entry point
├── activities/          # Temporal activity implementations
├── workflows/           # Temporal workflow definitions
├── worker/              # Worker setup and health endpoints
├── pods/                # Kubernetes Pod management
├── config/              # Configuration loading and validation
├── database/            # PostgreSQL integration
├── shared/              # Shared types and constants
└── utils/               # Utility packages
```

### Local Development Workflow

1. **Make code changes**
2. **Build and test:**
   ```bash
   go build -o olake-workers main.go
   go test ./...
   ```
3. **Build Docker image:**
   ```bash
   docker build -t olakego/olake-workers:local .
   ```
4. **Deploy to cluster** (see Helm chart documentation)

---

## ⚙️ Configuration

The worker is configured via environment variables:

### Required Configuration

| Variable              | Description               | Example                                 |
|-----------------------|---------------------------|-----------------------------------------|
| `TEMPORAL_HOST_PORT`  | Temporal server address   | `temporal.olake.svc.cluster.local:7233` |
| `TEMPORAL_NAMESPACE`  | Temporal namespace        | `default`                               |
| `TEMPORAL_TASK_QUEUE` | Task queue to listen on   | `olake-workers`                      |
| `DB_HOST`             | PostgreSQL host           | `postgresql.olake.svc.cluster.local`    |
| `DB_PORT`             | PostgreSQL port           | `5432`                                  |
| `DB_NAME`             | Database name             | `temporal`                              |
| `DB_USER`             | Database user             | `temporal`                              |
| `DB_PASSWORD`         | Database password         | `temporal`                              |

### Optional Configuration

| Variable                    | Description                              | Default |
|-----------------------------|------------------------------------------|---------|
| `LOG_LEVEL`                 | Logging level (debug, info, warn, error) | `info`  |
| `HEALTH_PORT`               | Health check server port                 | `8090`  |
| `MAX_CONCURRENT_ACTIVITIES` | Max parallel activities                  | `15`    |
| `MAX_CONCURRENT_WORKFLOWS`  | Max parallel workflows                   | `10`    |

### Worker-Specific Timeouts

Configure activity and workflow timeouts:

| Variable                    | Description               | Default |
|-----------------------------|---------------------------|---------|
| `TIMEOUT_ACTIVITY_DISCOVER` | Discover activity timeout | `2h`    |
| `TIMEOUT_ACTIVITY_TEST`     | Test activity timeout     | `2h`    |
| `TIMEOUT_ACTIVITY_SYNC`     | Sync activity timeout     | `700h`  |

---

## 🔍 Monitoring

### Health Endpoints

The worker exposes health endpoints on port 8090:

```bash
# Liveness probe
curl http://localhost:8090/health

# Readiness probe  
curl http://localhost:8090/ready
```

### Logging

Structured JSON logging with configurable levels:

```bash
# Set debug logging
export LOG_LEVEL=debug

# View worker logs (in Kubernetes)
kubectl logs -l app.kubernetes.io/name=olake-workers -n olake
```

---

## 🐛 Troubleshooting

### Worker Won't Start

**Check Temporal connectivity:**
```bash
# Test connection to Temporal
telnet <temporal-host> 7233

# Check worker logs
kubectl logs -l app.kubernetes.io/name=olake-workers -n olake
```

**Verify database access:**
```bash
# Test PostgreSQL connection
psql -h <db-host> -p <db-port> -U <db-user> -d <db-name>
```

### Pods Failing to Execute

**Check RBAC permissions:**
```bash
# Verify service account has pod creation permissions
kubectl auth can-i create pods --as=system:serviceaccount:olake:olake-workers-sa -n olake
```

**Storage issues:**
```bash
# Check PVC status
kubectl get pvc -n olake

# Test NFS mounting
kubectl exec deployment/olake-workers -n olake -- mount | grep nfs
```

### Activity Timeouts

**Check resource constraints:**
```bash
# View pod resource usage
kubectl top pods -n olake

# Check node resources
kubectl describe nodes
```

**Adjust timeouts if needed:**
```bash
# Increase activity timeout
export TIMEOUT_ACTIVITY_SYNC=800h
```

### Common Issues

1. **Image pull errors**: Ensure `olakego/olake-workers` image is accessible
2. **Permission denied**: Check Kubernetes RBAC configuration
3. **Storage mounting failures**: Verify NFS server is running and accessible
4. **Database connection timeouts**: Check network policies and firewall rules

## 📚 Related Documentation
- **Helm Chart**: See [README.md](../../helm/olake/README.md) for deployment instructions
- **OLake UI**: [GitHub Repository](https://github.com/datazip-inc/olake-ui)
- **Temporal**: [Official Temporal Go SDK docs](https://docs.temporal.io/dev-guide/go)