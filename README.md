# 🚲 Citi Bike NYC — Big Data Analytics & ML Pipeline

> A comprehensive end-to-end big data project built on Apache PySpark, covering data engineering, analytical querying, machine learning, and interactive dashboard visualization using the NYC Citi Bike dataset.


<table>
  <tr>
    <td><img src="1.jpeg" alt="Dashboard" width="250"/></td>
    <td><img src="2.jpeg" alt="Dashboard" width="250"/></td>
    <td><img src="3.jpeg" alt="Dashboard" width="250"/></td>
    <td><img src="4.jpeg" alt="Dashboard" width="250"/></td>
  </tr>
</table>


---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Tech Stack](#2-tech-stack)
3. [Part 0 — Setup & Data Loading](#3-part-0--setup--data-loading)
4. [Part 1 — Data Preprocessing](#4-part-1--data-preprocessing)
5. [Part 2 — Final Dataset Overview](#5-part-2--final-dataset-overview)
6. [Part 3 — Analytical Queries](#6-part-3--analytical-queries)
7. [Part 4 — SparkML: Gender Prediction](#7-part-4--sparkml-gender-prediction)
8. [Part 5 — Interactive Dashboard](#8-part-5--interactive-dashboard)
9. [Key Metrics Summary](#9-key-metrics-summary)

---

## 1. Project Overview

This project performs a full-scale big data analysis of the **NYC Citi Bike** trip dataset using **Apache PySpark** on Google Colab. The pipeline spans the entire data lifecycle:

- Ingesting and cleaning 1.3 million raw trip records
- Engineering derived features using custom UDFs
- Executing 10 analytical queries in both the **DataFrame API** and **Spark SQL**
- Training and comparing three **SparkML classification models** for gender prediction
- Delivering an **interactive Plotly + ipywidgets dashboard** exported as a self-contained HTML file

---

## 2. Tech Stack

| Layer | Tools |
|---|---|
| Distributed Computing | Apache PySpark (SparkContext, SparkSession, Spark SQL) |
| Feature Engineering | PySpark UDFs, Window Functions |
| Machine Learning | SparkML (Logistic Regression, Decision Tree, Random Forest) |
| Visualization | Plotly, ipywidgets |
| Environment | Google Colab, Google Drive |
| Output | Pandas CSV export, interactive HTML dashboard |

---

## 3. Part 0 — Setup & Data Loading

- Mounted Google Drive and installed `pyspark` and `findspark`
- Initialized `SparkContext` and `SparkSession`
- Loaded `citi_data.csv` into a Spark DataFrame (`df_raw`)
- Renamed columns to a clean underscore format
- Typecast all columns to correct data types (`Timestamp`, `Double`, `Integer`)
- Performed final schema validation and null checks on timestamp columns

---

## 4. Part 1 — Data Preprocessing

### A. Handling Duplicates & Missing Values

- Counted NULL values per column and dropped rows with NULLs in essential station columns
- Scanned all string columns for empty/whitespace-only values and ghost/sentinel values
- Detected implicit zeros in numeric columns (coordinates, station IDs, birth year)
- Identified and removed exact duplicate rows

### B. Identifying Inconsistencies

- Applied a **coordinate bounding box check** to ensure all stations fall within NYC geographic bounds
- Detected station ID → name mismatches and applied a **canonical name mapping** to standardize station names
- Enforced business logic sanity checks:
  - `stoptime` must be after `starttime`
  - Birth year must correspond to a plausible rider age

### C. Derived Feature Engineering

| Feature | Description |
|---|---|
| `rider_age` | Year of ride minus birth year |
| `trip_duration_sec` | Difference between `stoptime` and `starttime` |
| `trip_distance_km` | Haversine distance UDF using start/end coordinates |
| `trip_speed_kmh` | Derived from distance and duration via UDF |
| `period_of_day` | Morning / Afternoon / Evening / Night from `start_hour` |
| `start_month` | Extracted from `starttime` |

### D. Noise Flagging & Data Splitting

Individual noise flags were generated for:
- Trips shorter than a minimum plausible duration
- Excessive speed values
- Invalid rider age (outside 16–100 range)
- Missing essential fields
- Out-of-bounds coordinates
- Excessively long trips

A composite `is_noisy` flag combined all individual flags. The dataset was split into:
- **`df_clean`** — production-ready records (cached and registered as Spark SQL view `citibike`)
- **`df_noisy`** — quarantined records for review

---

## 5. Part 2 — Final Dataset Overview

| Metric | Value |
|---|---|
| Total Raw Records | 1,300,000 |
| Total Clean Records | **1,298,982** (99.92%) |
| Total Noisy Records | 1,018 (0.08%) |
| Final Columns | 22 |

**Descriptive Statistics (Clean Data):**

| Feature | Min | Max | Avg |
|---|---|---|---|
| Rider Age | 16 yrs | 100 yrs | 38.95 yrs |
| Trip Duration | 61 sec | 86,332 sec (23.98 hrs) | 861.84 sec (14.36 min) |
| Trip Distance | 0.0 km | 16.73 km | 1.77 km |
| Trip Speed | 0.0 km/h | 29.71 km/h | 8.77 km/h |

---

## 6. Part 3 — Analytical Queries

All 10 queries were implemented in both the **Spark DataFrame API** and **Spark SQL**.

### Query Results Summary

**Q1 — Round-Trip Percentage per User Type**

| User Type | Round-Trip % |
|---|---|
| Customer | 5.30% |
| Subscriber | 1.61% |

**Q2 — Most Popular Start Stations**

Top station: **Pershing Square North** with **9,759 trips**

**Q3 — Rush Hours**

| Hour | Share of Daily Trips |
|---|---|
| 8 AM | 8.91% |
| 5 PM | 9.74% |

**Q4 — Age Group Trip Duration Analysis**

| Age Group | Avg Trip Duration |
|---|---|
| Young (12–24) | 15.20 min |
| Adult (25–54) | 14.47 min |
| Senior (55+) | 13.04 min |

**Q5 — Seasonal Trip Behavior**

| Season | Total Trips | Avg Duration | Avg Distance |
|---|---|---|---|
| Summer | 449,670 | 15.23 min | 1.851 km |
| Winter | 149,855 (lowest) | — | — |

> Summer had the highest ridership; Winter had the highest subscriber percentage.

**Q6 — Bike Utilization (Maintenance Flagging)**

Top **1%** of bikes (192 bikes) were flagged as **HIGH RISK** based on total accumulated ride-hours.

**Q7 — Most Popular End Stations per User Type**

Identified per-user-type destination preferences using window functions and aggregation.

**Q8 — Station Pairs with Highest Trip Demand**

Ranked `(start_station, end_station)` pairs by total trip count.

**Q9 — Gender Differences in Trip Behavior**

| Gender | Avg Speed | Avg Duration |
|---|---|---|
| Male | 9.22 km/h | 12.81 min |
| Female | 8.26 km/h | 15.21 min |

**Q10 — Weekday vs. Weekend Trip Behavior**

| Day Type | Avg Duration | Avg Speed |
|---|---|---|
| Weekday | 13.43 min | 9.05 km/h |
| Weekend | 17.16 min | 7.92 km/h |

> Weekend riders take longer, more leisurely trips; weekday riders are faster and more goal-oriented.

---

## 7. Part 4 — SparkML: Gender Prediction

### Goal

Predict rider gender (**Male = 1 / Female = 2**) from behavioral and trip features, excluding rows where gender is `Unknown (0)`.

### Dataset Preparation

- **Balanced dataset size:** 624,394 rows (~50% Male, ~50% Female via downsampling)
- Train/test split applied after balancing

### ML Pipeline

```
StringIndexer → OneHotEncoder → VectorAssembler → StandardScaler → Classifier
```

### Model Performance

| Model | Accuracy |
|---|---|
| Logistic Regression | 58.61% |
| Decision Tree | 59.45% |
| Random Forest | **59.81%** ✅ Best |

### Top Feature Importances (Random Forest)

| Rank | Feature | Importance |
|---|---|---|
| 1 | `trip_speed_kmh` | 47.57% |
| 2 | `trip_duration_sec` | 16.54% |
| 3 | `rider_age` | 10.84% |
| 4 | `trip_distance_km` | 6.34% |
| 5 | `start_hour` | 4.01% |

### Model Discussion

**Why Random Forest outperforms both alternatives:**

1. **Ensemble power** — averages 100 trees, each trained on a random data and feature subset, eliminating the variance/overfitting problem of a single Decision Tree
2. **Non-linearity** — gender-riding relationships are non-linear (e.g., a combination of high speed + short trip + subscriber is a stronger signal than any single feature), which tree ensembles capture but Logistic Regression cannot
3. **Stable feature importance** — averaged across all trees, making it far more reliable than a single tree's split decisions

**Decision Tree vs. Logistic Regression:** The Decision Tree handles non-linear splits and outperforms Logistic Regression, but a single deep tree overfits the training data — which Random Forest corrects through bagging.

### Key Insight on Feature Importance

- `trip_speed_kmh` and `trip_duration_sec` dominate — riding intensity and duration are the strongest behavioral signals distinguishing genders
- `rider_age` ranks strongly because gender demographics vary by age within the Citi Bike user base
- `user_type` contributes because subscriber demographics differ from casual customer demographics
- `start_hour` matters because commuting patterns (subscriber/male skew) and leisure patterns (customer/mixed) peak at different times

### Honest Caveat

Gender prediction from mobility data is inherently noisy — many riding behaviors overlap across genders. **An accuracy of 70–75% is considered strong for this task.** These models are more valuable for understanding *which features correlate with gender* than for high-confidence individual predictions.

---

## 8. Part 5 — Interactive Dashboard

- Built using **Plotly** for rich interactive charts and **ipywidgets** for user-controlled filtering
- All data collected via Spark SQL queries and converted to Pandas DataFrames
- Consistent dark-themed layout with a global color palette applied across all charts
- Visualizations include:
  - Hourly demand heatmap
  - Seasonal variation in ridership and trip behavior
  - Station usage maps
  - User type behavior breakdowns
  - Bike utilization and maintenance risk
- **Interactive dropdown widget** allows filtering charts by user type (All / Customer / Subscriber)
- Exported as a **self-contained HTML file** requiring no external dependencies

---

## 9. Key Metrics Summary

| Category | Metric | Value |
|---|---|---|
| Data Quality | Clean Record Rate | 99.92% |
| Data Quality | Noisy Records Removed | 1,018 |
| Dataset | Final Columns | 22 |
| Riders | Avg Age | 38.95 years |
| Trips | Avg Duration | 14.36 minutes |
| Trips | Avg Distance | 1.77 km |
| Trips | Avg Speed | 8.77 km/h |
| Peak Demand | Morning Rush | 8 AM (8.91% of daily trips) |
| Peak Demand | Evening Rush | 5 PM (9.74% of daily trips) |
| Busiest Station | Pershing Square North | 9,759 trips |
| Seasonality | Highest Ridership | Summer (449,670 trips) |
| ML Best Model | Random Forest Accuracy | 59.81% |
| ML Top Feature | `trip_speed_kmh` | 47.57% importance |

---

*Project completed as part of the Big Data & NoSQL course at German International University (GIU).*
