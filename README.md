# StreamPro Data Engineering Assignment

This project implements an end-to-end data pipeline for the StreamPro platform.  
The goal is to transform raw user events and dimension files into an analytics-ready data model that enables Product to understand user engagement, drop-off behavior, and short-term retention.

---

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