SET 'execution.checkpointing.interval' = '30 s';
SET 'pipeline.name' = 'guardianpay-feature-job';

CREATE TABLE tx_raw (
  tx_id STRING,
  event_time TIMESTAMP(3),
  account_id STRING,
  card_id STRING,
  merchant_id STRING,
  amount DOUBLE,
  currency STRING,
  country STRING,
  device_id STRING,
  ip STRING,
  mcc STRING,
  channel STRING,
  is_card_present BOOLEAN,
  user_agent STRING,
  label STRING,
  WATERMARK FOR event_time AS event_time - INTERVAL '3' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'tx.raw',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-feature-job',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

CREATE TABLE tx_feature (
  tx_id STRING,
  velocity_1m_card DOUBLE,
  velocity_5m_device DOUBLE,
  velocity_15m_ip DOUBLE,
  amount_zscore_1h_account DOUBLE,
  anomaly_score DOUBLE,
  feature_version STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'tx.feature',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

INSERT INTO tx_feature
SELECT
  tx_id,
  CAST(cnt_card_1m AS DOUBLE) AS velocity_1m_card,
  CAST(cnt_device_5m AS DOUBLE) AS velocity_5m_device,
  CAST(cnt_ip_15m AS DOUBLE) AS velocity_15m_ip,
  CASE
    WHEN std_amt_1h > 0 THEN (amount - avg_amt_1h) / std_amt_1h
    ELSE 0.0
  END AS amount_zscore_1h_account,
  LEAST(
    1.0,
    0.65 * LEAST(1.0, (0.50 * cnt_card_1m + 0.30 * cnt_device_5m + 0.20 * cnt_ip_15m) / 20.0) +
    0.35 * LEAST(1.0, ABS(CASE WHEN std_amt_1h > 0 THEN (amount - avg_amt_1h) / std_amt_1h ELSE 0.0 END) / 6.0)
  ) AS anomaly_score,
  'flink-feature-v1' AS feature_version
FROM (
  SELECT
    tx_id,
    amount,
    COUNT(*) OVER (
      PARTITION BY card_id
      ORDER BY event_time
      RANGE BETWEEN INTERVAL '1' MINUTE PRECEDING AND CURRENT ROW
    ) AS cnt_card_1m,
    COUNT(*) OVER (
      PARTITION BY device_id
      ORDER BY event_time
      RANGE BETWEEN INTERVAL '5' MINUTE PRECEDING AND CURRENT ROW
    ) AS cnt_device_5m,
    COUNT(*) OVER (
      PARTITION BY ip
      ORDER BY event_time
      RANGE BETWEEN INTERVAL '15' MINUTE PRECEDING AND CURRENT ROW
    ) AS cnt_ip_15m,
    AVG(amount) OVER (
      PARTITION BY account_id
      ORDER BY event_time
      RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
    ) AS avg_amt_1h,
    STDDEV_POP(amount) OVER (
      PARTITION BY account_id
      ORDER BY event_time
      RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
    ) AS std_amt_1h
  FROM tx_raw
);
