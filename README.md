# Fraud Detection Dashboard

A Flask-based analytics dashboard designed for E-Invoice Audit Trail Analysis. This tool helps identify potential fraud indicators through duplicate record detection, credit note/invoice ratio analysis, and collaborative deck management.

## 🚀 Features

- **Duplicate Record Detection**: Automatically groups and flags duplicate invoices based on Buyer/Seller GSTIN, Date, and Document Number.
- **CRN/INV Ratio Analysis**: Analyzes the ratio of Credit Notes to Invoices for sellers, highlighting high-risk entities (Ratio > 0.5 or > 1.0).
- **Custom Decks & Comments**:
  - Upload analysis reports (PDF, CSV, XLSX).
  - Create custom virtual decks for audit logs.
  - Persistent commenting system for team collaboration on specific data sets.
- **Data Export**: Export filtered duplicate data and CRN ratios directly to CSV.

## 🛠️ Tech Stack

- **Backend**: Python (Flask)
- **Frontend**: Bootstrap 5, DataTables.js (Server-side processing), jQuery
- **Production Server**: Gunicorn

## 📋 Prerequisites

- Python 3.x
- Virtual Environment (`venv`)

## ⚙️ Installation & Setup

1. **Clone the repository**:
   ```bash
   git clone https://github.com/prachib-stack/Sample_Fraud_Test.git
   cd Sample_Fraud_Test
   ```

2. **Set up the environment**:
   ```bash
   # If venv is not already there
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Prepare Data**:
   Ensure your data files are in the `data/` directory:
   - `duplicates.csv`: Source for duplicate analysis.
   - `crn_ratio.json`: Source for seller ratios.
   - `1 Month Data.csv`: Raw source data.

4. **Run the application**:
   ```bash
   python app.py
   ```
   Open [http://localhost:5000](http://localhost:5000) in your browser.

## 📦 Deployment

This project is prepared for deployment on platforms like Render or Heroku using the included `Procfile` and `requirements.txt`.

---
*Created for E-Invoice Audit Trail Analysis &mdash; Feb 2026*
