import pandas as pd
import json
import os

def check_missing_data():
    data_folder = "data/raw"
    results = {}
    
    # Check CSV files
    csv_files = ["city_zones.csv", "energy_meters.csv", "traffic_sensors.csv"]
    
    for file in csv_files:
        file_path = os.path.join(data_folder, file)
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            missing_info = {
                "total_rows": len(df),
                "total_columns": len(df.columns),
                "missing_values_per_column": df.isnull().sum().to_dict(),
                "total_missing": df.isnull().sum().sum(),
                "missing_percentage": (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100
            }
            results[file] = missing_info
            print(f"\n📊 {file}:")
            print(f"   Rows: {missing_info['total_rows']}, Columns: {missing_info['total_columns']}")
            print(f"   Total missing values: {missing_info['total_missing']}")
            print(f"   Missing percentage: {missing_info['missing_percentage']:.2f}%")
            
            # Show columns with missing data
            missing_cols = {k: v for k, v in missing_info['missing_values_per_column'].items() if v > 0}
            if missing_cols:
                print(f"   Columns with missing data: {missing_cols}")
            else:
                print("   ✅ No missing data found")
    
    # Check Parquet file
    parquet_file = os.path.join(data_folder, "weather_data.parquet")
    if os.path.exists(parquet_file):
        df_parquet = pd.read_parquet(parquet_file)
        missing_info = {
            "total_rows": len(df_parquet),
            "total_columns": len(df_parquet.columns),
            "missing_values_per_column": df_parquet.isnull().sum().to_dict(),
            "total_missing": df_parquet.isnull().sum().sum(),
            "missing_percentage": (df_parquet.isnull().sum().sum() / (len(df_parquet) * len(df_parquet.columns))) * 100
        }
        results["weather_data.parquet"] = missing_info
        print(f"\n📊 weather_data.parquet:")
        print(f"   Rows: {missing_info['total_rows']}, Columns: {missing_info['total_columns']}")
        print(f"   Total missing values: {missing_info['total_missing']}")
        print(f"   Missing percentage: {missing_info['missing_percentage']:.2f}%")
        
        missing_cols = {k: v for k, v in missing_info['missing_values_per_column'].items() if v > 0}
        if missing_cols:
            print(f"   Columns with missing data: {missing_cols}")
        else:
            print("   ✅ No missing data found")
    
    # Check JSON file
    json_file = os.path.join(data_folder, "air_quality.json")
    if os.path.exists(json_file):
        with open(json_file, 'r') as f:
            data = json.load(f)
        
        # Convert to DataFrame for easier analysis
        if isinstance(data, list):
            df_json = pd.DataFrame(data)
        else:
            df_json = pd.json_normalize(data)
        
        missing_info = {
            "total_rows": len(df_json),
            "total_columns": len(df_json.columns),
            "missing_values_per_column": df_json.isnull().sum().to_dict(),
            "total_missing": df_json.isnull().sum().sum(),
            "missing_percentage": (df_json.isnull().sum().sum() / (len(df_json) * len(df_json.columns))) * 100
        }
        results["air_quality.json"] = missing_info
        print(f"\n📊 air_quality.json:")
        print(f"   Rows: {missing_info['total_rows']}, Columns: {missing_info['total_columns']}")
        print(f"   Total missing values: {missing_info['total_missing']}")
        print(f"   Missing percentage: {missing_info['missing_percentage']:.2f}%")
        
        missing_cols = {k: v for k, v in missing_info['missing_values_per_column'].items() if v > 0}
        if missing_cols:
            print(f"   Columns with missing data: {missing_cols}")
        else:
            print("   ✅ No missing data found")
    
    return results

if __name__ == "__main__":
    print("🔍 Checking for missing data in all datasets...\n")
    results = check_missing_data()
    
    # Summary
    print("\n" + "="*50)
    print("📋 SUMMARY")
    print("="*50)
    
    files_with_missing = []
    for file, info in results.items():
        if info['total_missing'] > 0:
            files_with_missing.append(f"{file} ({info['total_missing']} missing values)")
    
    if files_with_missing:
        print("⚠️  Files with missing data:")
        for file in files_with_missing:
            print(f"   - {file}")
    else:
        print("✅ No missing data found in any dataset!")