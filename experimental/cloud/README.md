The cloud resource facet extends marquez to track cloud assets to
help better understand usage and reduce costs. 

This document includes features that may not exist at time of writing.

# Storage and usage facet
To minimize costs for data storage, infrequently used datasets should
be deleted or moved to cold storage. To use this feature, all dataset
writes and reads should be monitored. 

## Usage
To get a list of datasets that are used, issue the graphql query:

```graphql
query {
  # Returns datasets that last had an event on Dec 1st, 2020 in 
  #  the EU region
  infrequent: latestDatasetEvent(filter: {
    to: "2020-12-01T00:00:00Z", region: "EU"
  }) {
    dataset {
      name
      source {
        name
        type
      }
    }
  }
  # Returns datasets that have been read at least 5 times in 2021 and 
  #  are currently in aws glacier storage
  revived: latestDatasetEvent(filter: {
    from: "2021-01-01T00:00:00Z"
    dataset: {
      facets: {
        storage: {
          type: "glacier"
        }
      }
    }
    count: 5
  })
}
```

## Reads
To emit a dataset read, emit an OpenLineage facet with:
- No eventType 
- No output datasets
- A random UUID for the runId
- The 'job' is the service that generated the read
- The job namespace is the team or group that generated the event

```json
{
  "eventTime" : "2021-01-01T00:00:00Z",
  "run" : {
    "runId" : "ea445b5c-22eb-457a-8007-01c7c52b6e54"
  },
  "job" : {
    "namespace" : "datascience_team",
    "name" : "service.recommendation_service"
  },
  "inputs" : [ {
    "namespace" : "datakin",
    "name" : "s3://ds-team/latest_recommendations.csv"
  } ],
  "producer" : "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client"
}
```

## Region Facet
The region facet helps better GDPR compliance. It is used as an input facet:
```json
...
  "inputs" : [ {
    "namespace" : "datakin",
    "name" : "s3://ds-team/eu_recent_recommendations.csv",
    "facets": {
      "region": {
        "country": "EU"
      }
    }
  } ],
...
```

## Moving datasets to cold storage
To move to cold storage, emit a storage facet on the dataset. 
```json
{
  "eventTime" : "2021-01-01T00:00:00Z",
  "run" : {
    "runId" : "ca64b7b4-fd53-46f1-9146-df8a0e90447f"
  },
  "job" : {
    "namespace" : "platform_team",
    "name" : "service.cold_storage"
  },
  "inputs" : [ {
    "namespace" : "datakin",
    "name" : "s3://ds-team/recent_recommendations.csv",
    "facets": {
      "storage": {
        "type": "glacier"
      }
    }
  } ],
  "producer" : "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client"
}
```

# Cost sharing facet
The cost sharing facet allows tagging cloud assets to jobs. This allows for better
understanding for the costs incurred for each team. For example, platform teams
can understand individual team usage to create cost reports that can be shared
between teams.

## Usage
To get a list of jobs that where run by team:

```graphql
query {
  # Get instance breakdown by namespace
  instances(filter: {from: "2020-01-01T00:00:00Z", to: "2021-01-01T00:00:00Z"}) {
    instance { 
      type
    }
    namespace {
      actualUsage(timeUnit: CPU_SECONDS)
      requestedUsage(timeUnit: CPU_SECONDS)
    }
  }
}
```

## Data collection
### Istio sidecar
Services can be tracked using a kubernetes sidecar to emit services that are running.
```
kubectl apply -f https://raw.githubusercontent.com/istio/istio/release-1.8/samples/addons/marquez-agent.yaml
```

Add to the helm chart:
```yaml
spec:
  template:
    metadata:
      annotations:
        marquez.io/scrape: true
        marquez.io/namespace: datascience_team
        marquez.io/job: recommendation_service
```

### Dataproc
When creating a dataproc operator, use the marquez dag and include the run id.
```python
from marquez_airflow import DAG

create_dataproc_cluster = DataprocClusterCreateOperator(
    task_id='create_dataproc_cluster',
    cluster_name='hadoop-cluster',
    num_workers=2,
    zone='europe-west1-b',
    master_machine_type='n1-standard-1',
    worker_machine_type='n1-standard-1',
    dag=dag)
```
### Manual via OpenLineage
The run event uses a instance facet to track resource usage:

```json
{
  "eventType" : "START",
  "eventTime": "2021-01-01T00:00:00Z",
  "run": {
    "runId": "f13fd15e-d1ff-46bc-b142-844fe1d4b9ae",
    "facets": {
      "instances": [{
        "type": "n1-standard-1",
        "zone": "europe-west1-b"
      },{
        "type": "n1-standard-1",
        "zone": "europe-west1-b"
      }]
    }
  },
  "job": {
    "namespace": "platform_team",
    "name": "spark.recommendation_job"
  },
  "outputs": [
    {
      "namespace": "datakin",
      "name": "s3://ds-team/recent_recommendations.csv"
    }
  ],
  "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client"
}
```

# Ensemble Usage
If all datasets and jobs are tracked with marquez, it becomes easy to find
the cost of datasets.

```graphql
query {
  datasets {
    facets {
      instanceUsage(filter: {from: "2020-01-01T00:00:00Z"}) {
        instance {
          type
        }
        actualUsage(timeUnit: CPU_SECONDS)
      }
    }
  }
}
```