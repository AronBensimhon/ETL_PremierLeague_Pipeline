# Deployment Configuration

This folder contains Kubernetes deployment configuration for running the Premier League ETL pipeline as a scheduled CronJob.

## Files

- `Dockerfile`: Container image definition
- `.dockerignore`: Files to exclude from Docker build
- `kubernetes-configmap.yaml`: Non-sensitive configuration
- `kubernetes-secret.yaml`: Sensitive credentials (template)
- `kubernetes-cronjob.yaml`: CronJob scheduling configuration

## Deployment Instructions

### 1. Build Docker Image
```bash
# Build the image
docker build -t premier-league-etl:latest -f deployment/Dockerfile .

# Tag for GCR (Google Container Registry)
docker tag premier-league-etl:latest gcr.io/your-project-id/premier-league-etl:latest

# Push to registry
docker push gcr.io/your-project-id/premier-league-etl:latest
```

### 2. Create Kubernetes Resources
```bash
# Apply ConfigMap
kubectl apply -f deployment/kubernetes-configmap.yaml

# Create Secret (IMPORTANT: Update values first!)
kubectl apply -f deployment/kubernetes-secret.yaml

# Deploy CronJob
kubectl apply -f deployment/kubernetes-cronjob.yaml
```

### 3. Verify Deployment
```bash
# Check CronJob status
kubectl get cronjobs

# View scheduled jobs
kubectl get jobs

# Check pod logs
kubectl logs -l app=premier-league-etl

# Manually trigger a job (for testing)
kubectl create job --from=cronjob/premier-league-etl manual-run-1
```

## Configuration

### Schedule
The CronJob runs daily at **2:00 AM UTC**. Modify the `schedule` field in `kubernetes-cronjob.yaml` to change timing:
- `0 2 * * *` - Daily at 2 AM
- `0 */6 * * *` - Every 6 hours
- `0 0 * * 0` - Weekly on Sunday at midnight

### Resources
Default resource allocation:
- **Requests**: 256Mi memory, 0.25 CPU cores
- **Limits**: 512Mi memory, 0.5 CPU cores

Adjust based on your workload in `kubernetes-cronjob.yaml`.

### Concurrency
Set to `Forbid` to prevent overlapping runs. Change to `Allow` if concurrent execution is desired.

## Security Considerations

1. **Never commit secrets to Git**
   - Use `kubectl create secret` in production
   - Store credentials in a secure vault (HashiCorp Vault, Google Secret Manager)

2. **Use service accounts with minimal permissions**
   - BigQuery Data Editor (for writing)
   - BigQuery Job User (for queries)

3. **Enable network policies**
   - Restrict egress to API endpoints only
   - Block unnecessary pod-to-pod communication

## Monitoring

### View Logs
```bash
# Real-time logs
kubectl logs -f -l app=premier-league-etl

# Previous job logs
kubectl logs job/premier-league-etl-<job-id>
```

### Check Job History
```bash
# List recent jobs
kubectl get jobs --sort-by=.metadata.creationTimestamp

# Delete old jobs manually
kubectl delete job premier-league-etl-<old-job-id>
```

## Troubleshooting

### Job Fails Immediately
```bash
# Check pod events
kubectl describe pod -l app=premier-league-etl

# Common issues:
# - Missing secrets
# - Invalid credentials
# - Image pull errors
```

### API Timeout Issues
```bash
# Increase activeDeadlineSeconds in cronjob spec
# Default is unlimited, but can set to 1800 (30 minutes)
```

### Email Alerts Not Sending
```bash
# Check Gmail App Password is correct
# Verify ALERT_EMAIL and EMAIL_PASSWORD in secret
kubectl get secret premier-league-etl-secrets -o yaml
```

## Cleanup
```bash
# Delete all resources
kubectl delete cronjob premier-league-etl
kubectl delete configmap premier-league-etl-config
kubectl delete secret premier-league-etl-secrets

# Delete completed jobs
kubectl delete jobs -l app=premier-league-etl
```

## Production Recommendations

1. **Use a CI/CD pipeline** (GitHub Actions, GitLab CI) to build and deploy
2. **Implement monitoring** (Prometheus, Grafana, or GCP Cloud Monitoring)
3. **Set up alerts** for job failures
4. **Use namespace isolation** for multi-environment deployments
5. **Enable pod autoscaling** if workload varies
6. **Implement log aggregation** (ELK stack, Loki, or Cloud Logging)