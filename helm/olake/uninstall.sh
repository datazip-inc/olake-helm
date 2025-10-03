#!/usr/bin/env bash

###############################################################################
# OLake Helm Chart Uninstallation Script
# 
# This script completely removes the OLake Helm chart and all associated
# resources, including those protected by the "helm.sh/resource-policy: keep"
# annotation.
#
# Usage:
#   ./uninstall.sh [OPTIONS]
#
# Options:
#   -n, --namespace NAMESPACE    Namespace where OLake is installed (default: olake)
#   -r, --release RELEASE        Helm release name (default: olake)
#   --force                      Force delete stuck resources without waiting
#   --keep-pvcs                  Keep PersistentVolumeClaims (useful for data preservation)
#   --keep-namespace             Keep the namespace after cleanup
#   -h, --help                   Show this help message
#
###############################################################################

set -e

# Default values
NAMESPACE="olake"
RELEASE="olake"
FORCE_DELETE=false
KEEP_PVCS=false
KEEP_NAMESPACE=false

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Print functions
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_header() {
    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}"
}

# Help function
show_help() {
    cat << EOF
OLake Helm Chart Uninstallation Script

Usage: ./uninstall.sh [OPTIONS]

Options:
  -n, --namespace NAMESPACE    Namespace where OLake is installed (default: olake)
  -r, --release RELEASE        Helm release name (default: olake)
  --force                      Force delete stuck resources without waiting
  --keep-pvcs                  Keep PersistentVolumeClaims (useful for data preservation)
  --keep-namespace             Keep the namespace after cleanup
  -h, --help                   Show this help message

Examples:
  # Uninstall with default settings (removes everything)
  ./uninstall.sh

  # Uninstall but keep PVCs for data preservation
  ./uninstall.sh --keep-pvcs

  # Uninstall with custom namespace and release name
  ./uninstall.sh -n my-namespace -r my-release

  # Force delete stuck resources
  ./uninstall.sh --force

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -n|--namespace)
            NAMESPACE="$2"
            shift 2
            ;;
        -r|--release)
            RELEASE="$2"
            shift 2
            ;;
        --force)
            FORCE_DELETE=true
            shift
            ;;
        --keep-pvcs)
            KEEP_PVCS=true
            shift
            ;;
        --keep-namespace)
            KEEP_NAMESPACE=true
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

print_header "OLake Uninstallation Script"
print_info "Namespace: $NAMESPACE"
print_info "Release: $RELEASE"
print_info "Force Delete: $FORCE_DELETE"
print_info "Keep PVCs: $KEEP_PVCS"
print_info "Keep Namespace: $KEEP_NAMESPACE"
echo ""

# Check if namespace exists
if ! kubectl get namespace "$NAMESPACE" &> /dev/null; then
    print_warning "Namespace '$NAMESPACE' does not exist. Nothing to uninstall."
    exit 0
fi

# Check if helm release exists
if helm list -n "$NAMESPACE" | grep -q "^$RELEASE"; then
    print_header "Step 1: Uninstalling Helm Release"
    print_info "Uninstalling Helm release '$RELEASE' from namespace '$NAMESPACE'..."
    helm uninstall "$RELEASE" -n "$NAMESPACE"
    print_success "Helm release uninstalled"
else
    print_warning "Helm release '$RELEASE' not found in namespace '$NAMESPACE'"
fi

# Stop any DevSpace deployments
print_header "Step 2: Cleaning Up DevSpace Resources"
if kubectl get deployment -n "$NAMESPACE" -l devspace.sh/replaced=true &> /dev/null; then
    print_info "Removing DevSpace deployments..."
    kubectl delete deployment -n "$NAMESPACE" -l devspace.sh/replaced=true --ignore-not-found=true
    print_success "DevSpace deployments removed"
else
    print_info "No DevSpace deployments found"
fi

# Delete remaining pods
print_header "Step 3: Cleaning Up Pods"
PODS=$(kubectl get pods -n "$NAMESPACE" --no-headers 2>/dev/null | wc -l)
if [ "$PODS" -gt 0 ]; then
    print_info "Deleting remaining pods..."
    if [ "$FORCE_DELETE" = true ]; then
        kubectl delete pods --all -n "$NAMESPACE" --force --grace-period=0 --ignore-not-found=true
    else
        kubectl delete pods --all -n "$NAMESPACE" --ignore-not-found=true
    fi
    print_success "Pods deleted"
else
    print_info "No pods found"
fi

# Delete NFS Server StatefulSet (protected by resource policy)
print_header "Step 4: Cleaning Up NFS Server"
if kubectl get statefulset -n "$NAMESPACE" | grep -q "nfs-server"; then
    print_info "Deleting NFS Server StatefulSet..."
    kubectl delete statefulset -n "$NAMESPACE" -l app.kubernetes.io/component=nfs-server --ignore-not-found=true
    print_success "NFS Server StatefulSet deleted"
else
    print_info "No NFS Server StatefulSet found"
fi

# Delete NFS Server Service (protected by resource policy)
if kubectl get service -n "$NAMESPACE" | grep -q "nfs-server"; then
    print_info "Deleting NFS Server Service..."
    kubectl delete service -n "$NAMESPACE" -l app.kubernetes.io/component=nfs-server --ignore-not-found=true
    print_success "NFS Server Service deleted"
else
    print_info "No NFS Server Service found"
fi

# Delete NFS Server ServiceAccount (protected by resource policy)
if kubectl get serviceaccount -n "$NAMESPACE" | grep -q "nfs-server"; then
    print_info "Deleting NFS Server ServiceAccount..."
    kubectl delete serviceaccount -n "$NAMESPACE" -l app.kubernetes.io/component=nfs-server --ignore-not-found=true
    print_success "NFS Server ServiceAccount deleted"
else
    print_info "No NFS Server ServiceAccount found"
fi

# Delete PersistentVolumeClaims (protected by resource policy)
if [ "$KEEP_PVCS" = false ]; then
    print_header "Step 5: Cleaning Up PersistentVolumeClaims"
    PVCS=$(kubectl get pvc -n "$NAMESPACE" --no-headers 2>/dev/null | wc -l)
    if [ "$PVCS" -gt 0 ]; then
        print_info "Deleting PersistentVolumeClaims..."
        kubectl delete pvc --all -n "$NAMESPACE" --ignore-not-found=true
        
        # Wait for PVCs to be deleted or force if needed
        if [ "$FORCE_DELETE" = true ]; then
            print_info "Force deleting stuck PVCs..."
            for pvc in $(kubectl get pvc -n "$NAMESPACE" --no-headers 2>/dev/null | awk '{print $1}'); do
                kubectl patch pvc "$pvc" -n "$NAMESPACE" -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
            done
        fi
        print_success "PersistentVolumeClaims deleted"
    else
        print_info "No PersistentVolumeClaims found"
    fi
else
    print_warning "Skipping PVC deletion (--keep-pvcs flag set)"
fi

# Delete orphaned PersistentVolumes
print_header "Step 6: Cleaning Up Orphaned PersistentVolumes"
RELEASED_PVS=$(kubectl get pv --no-headers 2>/dev/null | grep -E "Released.*$NAMESPACE/" | awk '{print $1}')
if [ -n "$RELEASED_PVS" ]; then
    print_info "Deleting orphaned PersistentVolumes..."
    for pv in $RELEASED_PVS; do
        print_info "Deleting PV: $pv"
        kubectl delete pv "$pv" --ignore-not-found=true
    done
    print_success "Orphaned PersistentVolumes deleted"
else
    print_info "No orphaned PersistentVolumes found"
fi

# Delete NFS StorageClass (protected by resource policy)
print_header "Step 7: Cleaning Up NFS StorageClass"
if kubectl get storageclass 2>/dev/null | grep -q "nfs-server"; then
    print_info "Deleting NFS StorageClass..."
    kubectl delete storageclass nfs-server --ignore-not-found=true
    print_success "NFS StorageClass deleted"
else
    print_info "No NFS StorageClass found"
fi

# Delete ClusterRole (protected by resource policy)
print_header "Step 8: Cleaning Up ClusterRole"
if kubectl get clusterrole 2>/dev/null | grep -q "${RELEASE}-nfs-server\|olake-nfs-server"; then
    print_info "Deleting NFS Server ClusterRole..."
    kubectl delete clusterrole "${RELEASE}-nfs-server" --ignore-not-found=true 2>/dev/null || true
    kubectl delete clusterrole "olake-nfs-server" --ignore-not-found=true 2>/dev/null || true
    print_success "ClusterRole deleted"
else
    print_info "No ClusterRole found"
fi

# Delete ClusterRoleBinding (protected by resource policy)
print_header "Step 9: Cleaning Up ClusterRoleBinding"
if kubectl get clusterrolebinding 2>/dev/null | grep -q "${RELEASE}-nfs-server\|olake-nfs-server"; then
    print_info "Deleting NFS Server ClusterRoleBinding..."
    kubectl delete clusterrolebinding "${RELEASE}-nfs-server" --ignore-not-found=true 2>/dev/null || true
    kubectl delete clusterrolebinding "olake-nfs-server" --ignore-not-found=true 2>/dev/null || true
    print_success "ClusterRoleBinding deleted"
else
    print_info "No ClusterRoleBinding found"
fi

# Delete any remaining resources in the namespace
print_header "Step 10: Cleaning Up Remaining Resources"
print_info "Checking for any remaining resources..."

# Delete ConfigMaps
CONFIGMAPS=$(kubectl get configmap -n "$NAMESPACE" --no-headers 2>/dev/null | wc -l)
if [ "$CONFIGMAPS" -gt 0 ]; then
    print_info "Deleting ConfigMaps..."
    kubectl delete configmap --all -n "$NAMESPACE" --ignore-not-found=true
fi

# Delete Secrets
SECRETS=$(kubectl get secret -n "$NAMESPACE" --no-headers 2>/dev/null | grep -v "default-token" | wc -l)
if [ "$SECRETS" -gt 0 ]; then
    print_info "Deleting Secrets..."
    kubectl delete secret --all -n "$NAMESPACE" --ignore-not-found=true
fi

# Delete Services
SERVICES=$(kubectl get service -n "$NAMESPACE" --no-headers 2>/dev/null | wc -l)
if [ "$SERVICES" -gt 0 ]; then
    print_info "Deleting Services..."
    kubectl delete service --all -n "$NAMESPACE" --ignore-not-found=true
fi

# Delete Deployments
DEPLOYMENTS=$(kubectl get deployment -n "$NAMESPACE" --no-headers 2>/dev/null | wc -l)
if [ "$DEPLOYMENTS" -gt 0 ]; then
    print_info "Deleting Deployments..."
    kubectl delete deployment --all -n "$NAMESPACE" --ignore-not-found=true
fi

# Delete StatefulSets
STATEFULSETS=$(kubectl get statefulset -n "$NAMESPACE" --no-headers 2>/dev/null | wc -l)
if [ "$STATEFULSETS" -gt 0 ]; then
    print_info "Deleting StatefulSets..."
    kubectl delete statefulset --all -n "$NAMESPACE" --ignore-not-found=true
fi

# Delete Jobs
JOBS=$(kubectl get job -n "$NAMESPACE" --no-headers 2>/dev/null | wc -l)
if [ "$JOBS" -gt 0 ]; then
    print_info "Deleting Jobs..."
    kubectl delete job --all -n "$NAMESPACE" --ignore-not-found=true
fi

print_success "Remaining resources cleaned up"

# Delete namespace
if [ "$KEEP_NAMESPACE" = false ]; then
    print_header "Step 11: Deleting Namespace"
    print_info "Deleting namespace '$NAMESPACE'..."
    kubectl delete namespace "$NAMESPACE" --ignore-not-found=true
    
    if [ "$FORCE_DELETE" = true ]; then
        print_info "Checking for stuck namespace..."
        sleep 5
        if kubectl get namespace "$NAMESPACE" &> /dev/null; then
            print_warning "Namespace is stuck in 'Terminating' state. Attempting to force delete..."
            kubectl get namespace "$NAMESPACE" -o json | \
                jq '.spec.finalizers = []' | \
                kubectl replace --raw "/api/v1/namespaces/$NAMESPACE/finalize" -f - 2>/dev/null || true
        fi
    fi
    print_success "Namespace deleted"
else
    print_warning "Skipping namespace deletion (--keep-namespace flag set)"
fi

# Final summary
print_header "Uninstallation Complete"
echo ""
print_success "OLake has been successfully uninstalled!"
echo ""
print_info "Summary:"
echo "  - Helm release: Uninstalled"
echo "  - Pods: Deleted"
echo "  - NFS Server: Deleted"
echo "  - PVCs: $([ "$KEEP_PVCS" = true ] && echo "Kept" || echo "Deleted")"
echo "  - PVs: Cleaned up"
echo "  - StorageClass: Deleted"
echo "  - ClusterRole/Binding: Deleted"
echo "  - Namespace: $([ "$KEEP_NAMESPACE" = true ] && echo "Kept" || echo "Deleted")"
echo ""

if [ "$KEEP_PVCS" = true ]; then
    print_warning "PVCs were preserved. To delete them manually, run:"
    echo "  kubectl delete pvc --all -n $NAMESPACE"
fi

if [ "$KEEP_NAMESPACE" = true ]; then
    print_warning "Namespace was preserved. To delete it manually, run:"
    echo "  kubectl delete namespace $NAMESPACE"
fi

echo ""
print_info "To reinstall OLake, run:"
echo "  helm install olake ./helm/olake -n $NAMESPACE --create-namespace"
echo ""

