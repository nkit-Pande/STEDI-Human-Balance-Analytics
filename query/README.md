# STEDI SQL Table Definitions and Notes

This README documents the SQL queries used to create the STEDI project tables and explains their purpose and structure.

---

## Customer Tables

### `customer_landing`

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`customer_landing` (
  `customerName` string,
  `email` string,
  `phone` string,
  `birthDate` string,
  `serialNumber` string,
  `registerationDate` bigint,
  `lastUpdateDate` bigint,
  `shareWithResearchAsOfDate` bigint,
  `shareWithPublicAsOfDate` bigint,
  `timestamp` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://simple2903-data-lake/customer/landing/'
TBLPROPERTIES ('classification' = 'json');
```

### `customer_trusted`

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`customer_trusted` (
  `customerName` string,
  `email` string,
  `phone` string,
  `birthDate` string,
  `serialNumber` string,
  `registerationDate` bigint,
  `lastUpdateDate` bigint,
  `shareWithResearchAsOfDate` bigint,
  `shareWithPublicAsOfDate` bigint,
  `timestamp` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://simple2903-data-lake/customer/trusted/'
TBLPROPERTIES ('classification' = 'json');
```

### `customer_curated`

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`customer_curated` (
  `user` string,
  `customerName` string,
  `x` double,
  `y` double,
  `z` double
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://simple2903-data-lake/customer/curated/'
TBLPROPERTIES ('classification' = 'json');
```

**Note:**

> This curated table only includes a subset of fields (`user`, `customerName`, `x`, `y`, `z`) instead of the full schema. This is done intentionally to streamline the dataset for specific analytics and machine learning tasks, focusing only on features relevant for further analysis.
>
> The schema for all tables is flexible and may be modified in the future to include additional fields as required.

---

## Accelerometer Tables

### `accelerometer_landing`

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`accelerometer_landing` (
  `user` string,
  `x` double,
  `y` double,
  `z` double
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://simple2903-data-lake/accelerometer/landing/'
TBLPROPERTIES ('classification' = 'json');
```

### `accelerometer_trusted`

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`accelerometer_trusted` (
  `user` string,
  `timestamp` bigint,
  `x` double,
  `y` double,
  `z` double
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://simple2903-data-lake/accelerometer/trusted/'
TBLPROPERTIES ('classification' = 'json');
```

---

## Step Trainer Tables

### `step_trainer_landing`

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`step_trainer_landing` (
  `sensorReadingTime` bigint,
  `serialNumber` string,
  `distanceFromObject` int
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://simple2903-data-lake/step_trainer/landing/'
TBLPROPERTIES ('classification' = 'json');
```

### `step_trainer_trusted`

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`step_trainer_trusted` (
  `sensorReadingTime` bigint,
  `serialNumber` string,
  `distanceFromObject` int
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://simple2903-data-lake/step_trainer/trusted/'
TBLPROPERTIES ('classification' = 'json');
```

---

## Machine Learning Table

### `machine_learning_curated`

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`machine_learning_curated` (
  `x` double,
  `y` double,
  `z` double,
  `timestamp` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://simple2903-data-lake/machine_learning/curated/'
TBLPROPERTIES ('classification' = 'json');
```

---
