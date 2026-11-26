# Google Cloud Run Deployment Guide

This repository is prepared for scale-to-zero deployment on **Google Cloud Run** using **Workload Identity Federation (WIF)** from GitHub Actions.

> **Cost Protection Policy**: No billable Google Cloud infrastructure is provisioned automatically. The deployment workflow (`.github/workflows/deploy.yml`) remains skipped until you review the expected costs below and set the repository variable `ENABLE_CLOUD_RUN_DEPLOY=true`.

---

## 1. Architecture & Scale-to-Zero Configuration

- **Container Image ([`Dockerfile`](Dockerfile))**:
  - Multi-stage build compiling static Linux binaries (`CGO_ENABLED=0`) for `/app/queue-api` and `/app/queue-worker` into `gcr.io/distroless/static-debian12:nonroot`.
  - Runs as unprivileged user `nonroot:nonroot`.
  - Listens on `0.0.0.0:$PORT` (default `8080`) when `PORT` is provided by Cloud Run, while keeping `127.0.0.1:8080` as the local development default when neither `LISTEN_ADDR` nor `PORT` is set.
- **Health Checks**:
  - `GET /healthz`: Verifies PostgreSQL connectivity with a 2-second timeout. Returns HTTP `200 OK` (`{"status":"ok"}`) when healthy or HTTP `503 Service Unavailable` (`{"error":"database unavailable"}`) without leaking database internals.
  - Cloud Run startup and liveness probes target `GET /healthz` on port `8080`.
- **Resource & Scaling Settings**:
  - `--min-instances=0` (scales completely to zero when idle)
  - `--max-instances=2` (caps burst concurrency and prevents runaway charges)
  - `--cpu=1 --memory=256Mi --concurrency=80 --cpu-throttling` (CPU is allocated only during active request processing)

---

## 2. Expected Monthly Cost Breakdown (For Review Before Provisioning)

| Component | Recommended Configuration | Idle Monthly Cost | Active Demo Workload (~50k requests/mo) | Notes |
| --- | --- | ---: | ---: | --- |
| **Cloud Run (`queue-api`)** | `min-instances=0`, `max-instances=2`, 1 vCPU, 256 MiB RAM, request-scoped CPU | **$0.00 / mo** | **$0.00 / mo** | Covered by Cloud Run permanent free tier (2M requests, 180,000 vCPU-sec, 360,000 GiB-sec/month) |
| **Artifact Registry** | Single repository storing latest image (~12 MB distroless image) | **$0.00 / mo** | **$0.00 / mo** | Covered by Artifact Registry free tier (0.5 GB storage/month) |
| **PostgreSQL Option A (Recommended for $0 Demo)** | External scale-to-zero PostgreSQL (e.g., Neon Free Tier or Supabase Free Tier) via TLS `DATABASE_URL` in Secret Manager | **$0.00 / mo** | **$0.00 / mo** | Scales to zero; requires no always-on VM or VPC connector |
| **PostgreSQL Option B (Managed GCP)** | Cloud SQL for PostgreSQL (`db-f1-micro`, shared vCPU, 10 GB zonal SSD) | **~$9.37–$15.00 / mo** | **~$9.37–$15.00 / mo** | Always-on instance; **do not provision** if you want a strictly $0.00/mo idle footprint |
| **Secret Manager** | 1 active secret version (`QUEUE_DATABASE_URL`) | **$0.00 / mo** | **$0.00 / mo** | Covered by Secret Manager free tier (6 active secret versions/month) |

---

## 3. Workload Identity Federation Setup (No Service Account Keys)

Never create or upload long-lived service account JSON keys. When you are ready to enable deployment, configure keyless OIDC federation between GitHub Actions and Google Cloud:

```sh
export PROJECT_ID="your-gcp-project-id"
export PROJECT_NUMBER="$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')"
export REGION="us-central1"
export GITHUB_REPO="colinsohn/durable-job-queue"

# 1. Create a least-privilege deployer service account
gcloud iam service-accounts create queue-cloudrun-deployer \
  --project="$PROJECT_ID" \
  --display-name="Durable Job Queue Cloud Run Deployer"

export SA_EMAIL="queue-cloudrun-deployer@${PROJECT_ID}.iam.gserviceaccount.com"

for role in roles/run.admin roles/artifactregistry.writer roles/iam.serviceAccountUser; do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="$role"
done

# 2. Create the Workload Identity Pool and GitHub OIDC Provider
gcloud iam workload-identity-pools create github-pool \
  --project="$PROJECT_ID" \
  --location="global" \
  --display-name="GitHub Actions Pool"

gcloud iam workload-identity-pools providers create-oidc github-provider \
  --project="$PROJECT_ID" \
  --location="global" \
  --workload-identity-pool="github-pool" \
  --display-name="GitHub Actions OIDC Provider" \
  --issuer-uri="https://token.actions.githubusercontent.com" \
  --attribute-mapping="google.subject=assertion.sub,attribute.actor=assertion.actor,attribute.repository=assertion.repository,attribute.ref=assertion.ref" \
  --attribute-condition="assertion.repository == '${GITHUB_REPO}' && assertion.ref == 'refs/heads/main'"

# 3. Allow only this GitHub repository's main branch to impersonate the service account
gcloud iam service-accounts add-iam-policy-binding "$SA_EMAIL" \
  --project="$PROJECT_ID" \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/github-pool/attribute.repository/${GITHUB_REPO}"
```

---

## 4. Enabling the GitHub Actions Deployment Workflow

Once WIF and your target PostgreSQL database (`QUEUE_DATABASE_URL` in Secret Manager) are configured, set the following GitHub repository variables (Settings → Secrets and variables → Actions → Variables):

| Repository Variable | Example Value | Purpose |
| --- | --- | --- |
| `ENABLE_CLOUD_RUN_DEPLOY` | `true` (leave unset or `false` to disable) | Explicit cost-approval gate required before `.github/workflows/deploy.yml` runs |
| `GCP_PROJECT_ID` | `your-gcp-project-id` | Target Google Cloud project ID |
| `GCP_REGION` | `us-central1` | Cloud Run and Artifact Registry region |
| `GCP_ARTIFACT_REPO` | `durable-job-queue` | Artifact Registry Docker repository name |
| `GCP_CLOUD_RUN_SERVICE` | `durable-job-queue-api` | Cloud Run service name |
| `GCP_WORKLOAD_IDENTITY_PROVIDER` | `projects/123456789/locations/global/workloadIdentityPools/github-pool/providers/github-provider` | Full WIF provider resource name |
| `GCP_SERVICE_ACCOUNT` | `queue-cloudrun-deployer@your-gcp-project-id.iam.gserviceaccount.com` | Deployer service account email |
| `GCP_RUNTIME_SERVICE_ACCOUNT` | `queue-cloudrun-runtime@your-gcp-project-id.iam.gserviceaccount.com` | Runtime service account with `roles/secretmanager.secretAccessor` on `QUEUE_DATABASE_URL` |
