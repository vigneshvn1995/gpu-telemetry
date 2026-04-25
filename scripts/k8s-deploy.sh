#!/usr/bin/env bash
# =============================================================================
# k8s-deploy.sh — builds the Docker image and deploys gpu-telemetry to a
#                 Kubernetes cluster.
#
# Usage:
#   ./scripts/k8s-deploy.sh
#
# Environment variables:
#   IMAGE_TAG     Image tag to build and deploy          (default: latest)
#   REGISTRY      Registry prefix to push to             (optional)
#                 Example: REGISTRY=myregistry.io/myteam
#   CLUSTER_TYPE  Hint for image loading: minikube|kind  (auto-detected if unset)
#
# Examples:
#   # Docker Desktop / kind with auto-detect
#   ./scripts/k8s-deploy.sh
#
#   # minikube
#   CLUSTER_TYPE=minikube ./scripts/k8s-deploy.sh
#
#   # Push to a registry and deploy to a remote cluster
#   REGISTRY=myregistry.io/myteam IMAGE_TAG=v1.0.0 ./scripts/k8s-deploy.sh
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

IMAGE_NAME="gpu-telemetry"
IMAGE_TAG="${IMAGE_TAG:-latest}"
REGISTRY="${REGISTRY:-}"
CLUSTER_TYPE="${CLUSTER_TYPE:-}"
NAMESPACE="gpu-telemetry"

FULL_IMAGE="${IMAGE_NAME}:${IMAGE_TAG}"
if [[ -n "$REGISTRY" ]]; then
  FULL_IMAGE="${REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}"
fi

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " gpu-telemetry  Kubernetes Deployment"
echo " Image: $FULL_IMAGE"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# ---- prerequisites ----------------------------------------------------------
for cmd in docker kubectl; do
  command -v "$cmd" >/dev/null 2>&1 || { echo "ERROR: $cmd not found in PATH."; exit 1; }
done

# ---- Step 1: Build ----------------------------------------------------------
echo "[1/4] Building Docker image: $FULL_IMAGE"
docker build -t "$FULL_IMAGE" "$ROOT"
echo "  Build complete."

# ---- Step 2: Load / push ----------------------------------------------------
echo "[2/4] Making image available to the cluster..."

if [[ -n "$REGISTRY" ]]; then
  # Push to a container registry and update image references in manifests.
  docker push "$FULL_IMAGE"
  echo "  Pushed to registry."
  for f in "$ROOT"/k8s/*.yaml; do
    sed -i.bak "s|image: gpu-telemetry:latest|image: $FULL_IMAGE|g" "$f"
    rm -f "${f}.bak"
  done
  echo "  Updated image references in k8s/*.yaml to $FULL_IMAGE"
elif [[ "$CLUSTER_TYPE" == "minikube" ]] || \
     (command -v minikube >/dev/null 2>&1 && minikube status --format='{{.Host}}' 2>/dev/null | grep -q Running); then
  minikube image load "$FULL_IMAGE"
  echo "  Loaded into minikube."
elif [[ "$CLUSTER_TYPE" == "kind" ]]; then
  kind load docker-image "$FULL_IMAGE"
  echo "  Loaded into kind."
else
  echo "  Assuming Docker Desktop (shared Docker daemon). No loading required."
fi

# ---- Step 3: Deploy ---------------------------------------------------------
echo "[3/4] Applying manifests to namespace: $NAMESPACE"

# If the namespace already exists, restart existing deployments/statefulsets
# first so that old pods are terminated before new ones are scheduled.
# This avoids the "1 old replicas are pending termination" stall during rollout.
if kubectl get namespace "$NAMESPACE" >/dev/null 2>&1; then
  echo "  Namespace $NAMESPACE already exists — scaling down all workloads to force-clear stuck pods..."
  # Scale to 0 first — this works even when pods are in CreateContainerConfigError,
  # ImagePullBackOff, or Pending states where rollout restart has no effect.
  kubectl scale deployment/broker   --replicas=0 -n "$NAMESPACE" 2>/dev/null || true
  kubectl scale deployment/streamer --replicas=0 -n "$NAMESPACE" 2>/dev/null || true
  kubectl scale deployment/api      --replicas=0 -n "$NAMESPACE" 2>/dev/null || true
  kubectl scale statefulset/collector --replicas=0 -n "$NAMESPACE" 2>/dev/null || true

  # Wait for all pods to be fully gone before re-applying.
  echo "  Waiting for all pods to terminate (up to 90s)..."
  kubectl wait pods --for=delete \
    -l "app.kubernetes.io/part-of=gpu-telemetry" \
    -n "$NAMESPACE" --timeout=90s 2>/dev/null || true
fi

kubectl apply -f "$ROOT/k8s/namespace.yaml"
kubectl apply -f "$ROOT/k8s/broker.yaml"
kubectl apply -f "$ROOT/k8s/collector.yaml"
kubectl apply -f "$ROOT/k8s/streamer.yaml"
kubectl apply -f "$ROOT/k8s/api.yaml"
echo "  Manifests applied."

# ---- Step 4: Wait -----------------------------------------------------------
echo "[4/4] Waiting for rollout to complete (timeout 180s)..."
kubectl rollout status deployment/broker     -n "$NAMESPACE" --timeout=180s
kubectl rollout status statefulset/collector -n "$NAMESPACE" --timeout=180s
kubectl rollout status deployment/streamer   -n "$NAMESPACE" --timeout=180s
kubectl rollout status deployment/api        -n "$NAMESPACE" --timeout=180s

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " Deployment successful"
echo ""
echo " Pod status:"
kubectl get pods -n "$NAMESPACE"
echo ""
echo " Access the API (run in a separate terminal):"
echo "   kubectl port-forward -n $NAMESPACE svc/api 8080:8080"
echo ""
echo " Then test:"
echo "   curl http://localhost:8080/health"
echo "   curl http://localhost:8080/api/v1/gpus | jq ."
echo "   open http://localhost:8080/swagger/index.html"
echo ""
echo " View logs:"
echo "   kubectl logs -n $NAMESPACE deploy/broker -f"
echo "   kubectl logs -n $NAMESPACE statefulset/collector -f"
echo "   kubectl logs -n $NAMESPACE deploy/streamer -f"
echo "   kubectl logs -n $NAMESPACE deploy/api -f"
echo ""
echo " Tear down:"
echo "   kubectl delete namespace $NAMESPACE"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
