# 🟣 PhonePe Pulse - Data Visualization & Analytics Dashboard

A comprehensive **ETL + Analytics Dashboard** application built using **Python, Streamlit, MySQL, and Plotly** that visualizes transaction, user, and insurance data from the PhonePe Pulse dataset.

---

## 📖 Project Overview

PhonePe Pulse provides real-time data on digital payment trends across India. This project extracts data from PhonePe's open-source Pulse repository, transforms it into structured tables, loads it into a MySQL database, and presents it through an interactive Streamlit dashboard.

### Problem Statement
- PhonePe Pulse data is stored in **nested JSON files** organized by state, year, and quarter
- The data needs to be **extracted, cleaned, and transformed** into a queryable format
- Users need an **interactive dashboard** to explore trends, compare states, and analyze patterns

### Solution
This application provides a **one-click ETL pipeline** and a **real-time analytics dashboard** with 7+ chart types, interactive filters, and an India map visualization.

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

## 🛠️ Tech Stack

| Technology | Purpose |
|-----------|---------|
| **Python 3.10+** | Core programming language |
| **Streamlit** | Web application framework |
| **Plotly** | Interactive charts and maps |
| **Pandas** | Data manipulation and analysis |
| **MySQL** | Relational database for data storage |
| **SQLAlchemy** | Database connection and ORM |
| **mysql-connector-python** | MySQL driver for Python |

---

## 📁 Project Structure