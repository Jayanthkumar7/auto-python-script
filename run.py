from datetime import datetime, timedelta
from marine_data_upload import data_retrival_testing  # Replace with your actual module name or file

def main():
    # 📅 Today's date
    end_date = datetime.today().strftime('%Y-%m-%d')

    # 📅 8 days before today
    start_date = (datetime.today() - timedelta(days=8)).strftime('%Y-%m-%d')

    # ✅ Initialize and run all steps
    obj = data_retrival_testing(start_date=start_date, end_date=end_date)
    obj.get_copernicus_data()
    obj.get_open_meteo_data()
    obj.to_google_sheets()
    obj.cleanup_nc_files()

if __name__ == "__main__":
    main()
