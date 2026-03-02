import os
import pandas as pd

# Auto-detect uploaded Excel file
excel_files = [f for f in os.listdir() if f.endswith((".xlsx", ".xls"))]

if not excel_files:
    raise ValueError("No Excel file found. Please upload a valid Excel file.")

file_path = excel_files[0]  # take the first uploaded Excel file

# Read both sheets
issue_df = pd.read_excel(file_path, sheet_name="Issue Report")
received_df = pd.read_excel(file_path, sheet_name="Received Report")

  

# Clean columns 

issue_df.columns = issue_df.columns.str.strip() 

received_df.columns = received_df.columns.str.strip() 

  

# Normalize key columns 

issue_df["RouteCard No"] = issue_df["RouteCard No"].astype(str).str.strip() 

received_df["RouteCard No"] = received_df["RouteCard No"].astype(str).str.strip() 

issue_df["GK DC No"] = issue_df["GK DC No"].astype(str).str.strip() 

received_df["Subcon DC No"] = received_df["Subcon DC No"].astype(str).str.strip() 

issue_df["FG Item Code"] = issue_df["FG Item Code"].astype(str).str.strip() 

received_df["FG Item Code"] = received_df["FG Item Code"].astype(str).str.strip() 

issue_df["Supplier Name"] = issue_df["Supplier Name"].astype(str).str.strip() 

received_df["Supplier Name"] = received_df["Supplier Name"].astype(str).str.strip() 

  

# Aggregate Issue 

issue_grouped = issue_df.groupby(["RouteCard No", "GK DC No", "FG Item Code", "Supplier Name"], dropna=False)["Transfer Qty"].sum().reset_index().rename(columns={"GK DC No": "DC No", "Transfer Qty": "Issue_Qty"}) 

  

# Aggregate Received 

received_grouped = received_df.groupby(["RouteCard No", "Subcon DC No", "FG Item Code", "Supplier Name"], dropna=False)["Rcvd. Qty"].sum().reset_index().rename(columns={"Subcon DC No": "DC No", "Rcvd. Qty": "Received_Qty"}) 

  

# Merge and find mismatches 

# Merge
merged = issue_grouped.merge(
    received_grouped,
    on=["RouteCard No", "DC No", "FG Item Code", "Supplier Name"],
    how="outer"
)

# Fill missing values
merged["Issue_Qty"] = merged["Issue_Qty"].fillna(0)
merged["Received_Qty"] = merged["Received_Qty"].fillna(0)

# Calculate difference
merged["Difference"] = merged["Issue_Qty"] - merged["Received_Qty"]

# FINAL DATAFRAME (DO NOT FILTER)
final_df = merged.copy()