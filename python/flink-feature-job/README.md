# Flink Feature Job (Scaffold)

This folder contains a Flink SQL scaffold for migrating feature extraction from
`python/feature-stream` to native Flink event-time processing.

## Purpose

- Stateful keyed windows over `tx.raw`
- Event-time velocity aggregates
- Kafka sink for `tx.feature`

## How to Run (Full Profile)

1. Start stack:

```bash
docker compose --profile full up --build
```

2. Open Flink SQL client in jobmanager container:

```bash
docker compose exec flink-jobmanager /opt/flink/bin/sql-client.sh
```

3. Execute SQL:

```sql
SOURCE python/flink-feature-job/feature_job.sql;
```

## Notes

- This is intentionally a scaffold and can be extended with richer anomaly models.
- Current `core` profile uses the Python `feature-stream` service for faster local iteration.
