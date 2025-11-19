# StreamPro Data Engineering Assignment

This project implements an end-to-end data pipeline for the StreamPro platform.  
The goal is to transform raw user events and dimension files into an analytics-ready data model that enables Product to understand user engagement, drop-off behavior, and short-term retention.

## Process Overview

1. **Staging**  
   Load raw `events.jsonl`, `users.csv`, `videos.csv` into Delta tables  
   (`staging.events`, `staging.users`, `staging.videos`) with minimal cleaning and typing.

2. **Intermediate (user_sessions)**  
   Aggregate events to one row per `(user_id, session_id)` with:
   - session_start_ts / session_end_ts  
   - total_watch_time_s  
   - heart / like flags  
   - device_os, app_version, country  
   - user and video attributes  
   - session_number per user

3. **Mart (fact_user_session)**  
   Build `mart.fact_user_session` with:
   - reached_30s  
   - dropoff_under_10s  
   - next_session_start_ts / next_session_number  
   - retained_next_session_within_3d  

4. **Analysis**  
   Run Spark SQL on `mart.fact_user_session` to answer Q1–Q3 in a Databricks notebook.

To query JSON events together with the CSV dimensions, I:

- Load `events.jsonl` into `staging.events` using Spark, casting `timestamp` and `value`
  and selecting the relevant columns (user_id, account_id, video_id, session_id, event_name, etc.).
- Load the CSV dimensions `users.csv` and `videos.csv` into `staging.users` and `staging.videos`
  with proper typing.
- In the intermediate layer I join `staging.events` with `staging.users` on `user_id`
  and with `staging.videos` on `video_id`. This produces a unified session-level model where
  behavioral metrics from the JSON events can be analyzed together with user and content attributes
  from the CSV dimensions.

## 📌 Objectives

The pipeline supports analysis for:

### **Q1 — First-Session Engagement**
What % of new users reach at least **30 seconds of watch_time** in their first session?

### **Q2 — Genre-Based Early Retention**
Which **video genres** drive the highest **2nd-session retention within 3 days**?

### **Q3 — Platform Drop-off**
Is there a particular **device_os** or **app_version** where drop-off is abnormally high?

---

## 🧱 High-Level Architecture

```mermaid
flowchart LR

    subgraph RAW["Raw Files"]
        E[events.jsonl]
        U[users.csv]
        V[videos.csv]
    end

    subgraph STAGING["Staging Layer"]
        SE[staging.events]
        SU[staging.users]
        SV[staging.videos]
    end

    subgraph INTERMEDIATE["Intermediate Layer"]
        US[intermediate.user_sessions]
    end

    subgraph MART["Mart Layer"]
        FUS[mart.fact_user_session]
    end

    subgraph ANALYTICS["Analytics / Product Questions"]
        Q1["Q1: First-session ≥30s"]
        Q2["Q2: Genre retention"]
        Q3["Q3: Drop-off by device/app"]
    end

    E --> SE
    U --> SU
    V --> SV

    SE --> US
    SU --> US
    SV --> US

    US --> FUS

    FUS --> Q1
    FUS --> Q2
    FUS --> Q3


### How the design answers Q1–Q3

- **Q1 – % of new users with ≥30s in first session**
  - Use `mart.fact_user_session`
  - Filter `session_number = 1`
  - Compute `AVG(reached_30s)`

- **Q2 – Genre with highest 2nd-session retention within 3 days**
  - Use `mart.fact_user_session`
  - On `session_number = 1`, group by `first_genre`
  - Compute `AVG(retained_next_session_within_3d)`

- **Q3 – device_os / app_version with abnormal drop-off**
  - Use `mart.fact_user_session`
  - Group by `device_os` and by `(device_os, app_version)`
  - Compute `AVG(dropoff_under_10s)` and compare vs overall drop-off rate