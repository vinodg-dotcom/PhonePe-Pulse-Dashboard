# 🟣 PhonePe Pulse - Data Visualization & Analytics Dashboard

A comprehensive **ETL + Analytics Dashboard** application built using **Python, Streamlit, MySQL, and Plotly** that visualizes transaction, user, and insurance data from the PhonePe Pulse dataset.

![Python](https://img.shields.io/badge/Python-3.10+-blue?logo=python)
![Streamlit](https://img.shields.io/badge/Streamlit-1.45+-red?logo=streamlit)
![MySQL](https://img.shields.io/badge/MySQL-8.0+-orange?logo=mysql)
![Plotly](https://img.shields.io/badge/Plotly-6.0+-purple?logo=plotly)

---

## 📖 Project Overview

PhonePe Pulse provides real-time data on digital payment trends across India. This project:

1. **Extracts** data from PhonePe's open-source Pulse repository (JSON files)
2. **Transforms** nested JSON into clean, structured tabular format
3. **Loads** the data into a MySQL database
4. **Visualizes** the data through an interactive Streamlit dashboard with 7+ chart types

### Problem Statement
- PhonePe Pulse data is stored in **nested JSON files** organized by state, year, and quarter
- The data needs to be **extracted, cleaned, and transformed** into a queryable format
- Users need an **interactive dashboard** to explore trends, compare states, and analyze patterns

### Solution
A **one-click ETL pipeline** and a **real-time analytics dashboard** with an India map, interactive filters, and multiple visualization types.

---

## ✨ Features

| Feature | Description |
|---------|-------------|
| 🔄 **One-Click ETL** | Extracts JSON → Creates CSV → Loads into MySQL |
| 🗺️ **India Bubble Map** | Interactive map showing state-wise data |
| 📊 **7 Chart Types** | Bar, Pie, Line, Violin, Treemap, Sunburst, Year-over-Year |
| 🔍 **Dynamic Filters** | Filter by Data Type, Year, and Quarter |
| 📈 **Real-time Metrics** | Total transactions, payment value, avg. transaction |
| 🏆 **Top 10 Rankings** | States, Districts, and Pincodes |
| 📥 **Data Explorer** | Browse and download any table as CSV |
| 🌙 **Dark Theme** | PhonePe-inspired purple dark mode |

---

## 📋 Prerequisites

The following software must be installed before running this project:

| Software | Version | Download Link |
|----------|---------|---------------|
| Python | 3.10 or higher | https://www.python.org/downloads/ |
| MySQL Server | 8.0 or higher | https://dev.mysql.com/downloads/ |
| Git | 2.40 or higher | https://git-scm.com/downloads/ |
| VS Code (Optional) | Latest | https://code.visualstudio.com/ |

### Python Packages Required

| Package | Purpose |
|---------|---------|
| streamlit | Web dashboard framework |
| pandas | Data manipulation |
| plotly | Charts and India map |
| sqlalchemy | Database connection engine |
| mysql-connector-python | MySQL driver for Python |

Install all packages at once:
```bash
pip install -r requirements.txt