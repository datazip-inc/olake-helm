#!/usr/bin/env bash

# Function for colored output
function chalk() {
    local color=$1
    local text=$2
    local color_code=0
    if [[ $color == "red" ]]; then
        color_code=1
    elif [[ $color == "green" ]]; then
        color_code=2
    fi
    if [[ -n "$TERM" ]]; then
        echo -e "$(tput setaf $color_code)${text}$(tput sgr0)"
    else
        if [[ $color == "red" ]]; then
            echo -e "\033[31m${text}\033[0m"
        elif [[ $color == "green" ]]; then
            echo -e "\033[32m${text}\033[0m"
        else
            echo -e "${text}"
        fi
    fi
}

function fail() {
    local error="${1:-Unknown error}"
    echo "$(chalk red "${error}")"
    exit 1
}

function setup_buildx() {
    echo "Setting up Docker buildx and QEMU..."
    docker buildx version >/dev/null 2>&1 || fail "Docker buildx is not installed. Please install it."
    docker run --rm --privileged multiarch/qemu-user-static --reset -p yes || fail "Failed to set up QEMU"
    docker buildx create --use --name multiarch-builder || echo "Buildx builder already exists, using it."
    docker buildx inspect --bootstrap || fail "Failed to bootstrap buildx builder"
    echo "✅ Buildx and QEMU setup complete"
}

function release_k8s_worker() {
    local version=$1
    local platform=$2
    local environment=$3
    local image_name="$K8S_REPO_WORKER"
    
    # Set tag based on environment
    local tag_version=""
    local latest_tag=""
    
    case "$environment" in
        "master")
            tag_version="${version}"
            latest_tag="latest"
            ;;
        "staging")
            tag_version="stag-${version}"
            latest_tag="stag-latest"
            ;;
        "dev"|*) # Default to dev prefix if not master or staging
            tag_version="dev-${version}"
            latest_tag="dev-latest"
            ;;
    esac

    echo "Logging into Docker (if not already logged in by a previous function call)..."
    docker login -u="$DOCKER_LOGIN" -p="$DOCKER_PASSWORD" || fail "Docker login failed for $DOCKER_LOGIN"
    
    echo "**** Releasing K8s worker image $image_name for platforms [$platform] with version [$tag_version] ****"

    echo "Building and pushing K8s worker Docker image..."
    
    # Build K8s worker from script directory (olake-workers/k8s)
    local script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    pushd "$script_dir" >/dev/null || fail "Failed to change directory to $script_dir"
    docker buildx build --platform "$platform" --push \
        -t "${image_name}:${tag_version}" \
        -t "${image_name}:${latest_tag}" \
        --build-arg ENVIRONMENT="$environment" \
        --build-arg APP_VERSION="$version" \
        -f ./Dockerfile . || { popd >/dev/null; fail "K8s Worker build failed. Exiting..."; }
    popd >/dev/null
    
    echo "$(chalk green "K8s Worker release successful for $image_name version $tag_version")"
}

SEMVER_EXPRESSION='v([0-9]+\.[0-9]+\.[0-9]+)$'
STAGING_VERSION_EXPRESSION='v([0-9]+\.[0-9]+\.[0-9]+)-[a-zA-Z0-9_.-]+'

echo "Release tool running..."
CURRENT_BRANCH=$(git branch --show-current)
echo "Building on branch: $CURRENT_BRANCH"
echo "Environment: $ENVIRONMENT"
echo "Fetching remote changes from git with git fetch"
git fetch origin "$CURRENT_BRANCH" >/dev/null 2>&1
GIT_COMMITSHA=$(git rev-parse HEAD | cut -c 1-8)
echo "Latest commit SHA: $GIT_COMMITSHA"

echo "Running checks..."

# Verify Docker login
docker login -u="$DOCKER_LOGIN" -p="$DOCKER_PASSWORD" >/dev/null 2>&1 || fail "❌ Docker login failed. Ensure DOCKER_LOGIN and DOCKER_PASSWORD are set."
echo "✅ Docker login successful"

# Version validation based on environment (default is dev with no restrictions)
if [[ -z "$VERSION" ]]; then
    fail "❌ Version not set. Empty version passed."
fi

# Validate version format based on environment
if [[ "$ENVIRONMENT" == "master" ]]; then
    [[ $VERSION =~ $SEMVER_EXPRESSION ]] || fail "❌ Version $VERSION does not match semantic versioning required for master (e.g., v1.0.0)"
    echo "✅ Version $VERSION matches semantic versioning for master"
elif [[ "$ENVIRONMENT" == "staging" ]]; then
    [[ $VERSION =~ $STAGING_VERSION_EXPRESSION ]] || fail "❌ Version $VERSION does not match staging version format (e.g., v1.0.0-rc1)"
    echo "✅ Version $VERSION matches format for staging"
else
    echo "✅ Flexible versioning allowed for development: $VERSION"
fi

# Setup buildx and QEMU
setup_buildx

platform="linux/amd64,linux/arm64"
echo "✅ Releasing K8s worker application for environment $ENVIRONMENT with version $VERSION on platforms: $platform"

chalk green "=== Releasing Olake K8s Worker application ==="
chalk green "=== Environment: $ENVIRONMENT ==="
chalk green "=== Release version: $VERSION ==="

# Call the release function
release_k8s_worker "$VERSION" "$platform" "$ENVIRONMENT"

echo "$(chalk green "✅ K8s Worker release process completed successfully")"