class data_retrival_testing():
    
  def __init__(self,start_date,end_date):
    from datetime import datetime
    self.start_date = datetime.strptime(start_date, '%Y-%m-%d')
    self.end_date = datetime.strptime(end_date, '%Y-%m-%d')
    self.final_df = None

  def get_copernicus_data(self):

    import copernicusmarine


    import os
    from getpass import getpass

    os.environ["CMEMS_USERNAME"] = "swaran.rekulapally@gmail.com"
    os.environ["CMEMS_PASSWORD"] = "Swaran@2005"

    import copernicusmarine

    # Step 3: Login using environment variables
    copernicusmarine.login(
        username=os.environ["CMEMS_USERNAME"],
        password=os.environ["CMEMS_PASSWORD"]
    )

    import xarray as xr
    import pandas as pd
    import numpy as np
    from datetime import datetime, timedelta
    import gc

    # 🌍 Region and dataset setup
    phy_dataset_id = "cmems_mod_glo_phy_anfc_0.083deg_PT1H-m"
    bgc_dataset_id = "cmems_mod_glo_bgc-pft_anfc_0.25deg_P1D-m"
    variables_phy = ["so", "thetao", "uo", "vo"]
    variables_bgc = ["chl"]
    phy_depth = 0.49402499198913574
    bgc_depth = 0.4940253794193268
    # Updated time slots to include 08:00, 12:00, 16:00, and 20:00
    time_slots = ["08:00:00", "12:00:00", "16:00:00", "20:00:00"]


    ports = [
        {"name": "Visakhapatnam", "lat": 17.6868, "lon": 83.2185, "fishing_distance": 0.45},  # ~50 km
        {"name": "Kakinada", "lat": 16.9604, "lon": 82.2380, "fishing_distance": 0.45},       # ~30 km
        {"name": "Machilipatnam", "lat": 16.1875, "lon": 81.1389, "fishing_distance": 0.45},  # ~20 km
        {"name": "Krishnapatnam", "lat": 14.2500, "lon": 80.1200, "fishing_distance": 0.45}   # ~39 km
    ]

    # Generalized: Generate grid points for each port based on fishing distance
    grid_points = []
    point_id = 1

    for port in ports:
        lat_base = port["lat"]
        lon_base = port["lon"]
        dist = port["fishing_distance"]

        # Grid bounds dynamically based on fishing distance
        lat_min = lat_base - dist
        lat_max = lat_base + dist
        lon_min = lon_base
        lon_max = lon_base + (dist * 2)

        # Grid resolution (adjustable based on granularity desired)
        step = 0.4

        lat_range = np.arange(lat_min, lat_max + step, step)
        lon_range = np.arange(lon_min, lon_max + step, step)

        for lat in lat_range:
            for lon in lon_range:
                grid_points.append((round(lat, 4), round(lon, 4), port["name"], f"Point_{point_id}"))
                point_id += 1

    # Remove duplicates and print total points
    grid_points = list(dict.fromkeys(grid_points))
    print(f"Total grid points across all ports: {len(grid_points)}")

    all_dfs = []

    # 📅 Step 6: Loop over last 8 days
    delta_days = (self.end_date - self.start_date).days
    for i in range(delta_days+1):
        date_obj = self.start_date + timedelta(days=i)
        date = date_obj.strftime('%Y-%m-%d')
        chl_timestamp = f"{date}T00:00:00"
        bgc_filename = f"chl_{date}_000000.nc"

        try:
            # ✅ Download once per day for BGC (chl) - surface only
            copernicusmarine.subset(
                dataset_id=bgc_dataset_id,
                variables=variables_bgc,
                minimum_longitude=min(lon for _, lon, _, _ in grid_points),
                maximum_longitude=max(lon for _, lon, _, _ in grid_points),
                minimum_latitude=min(lat for lat, _, _, _ in grid_points),
                maximum_latitude=max(lat for lat, _, _, _ in grid_points),
                start_datetime=chl_timestamp,
                end_datetime=chl_timestamp,
                minimum_depth=bgc_depth,
                maximum_depth=bgc_depth,  # Surface only
                output_filename=bgc_filename
            )
            ds_bgc = xr.open_dataset(bgc_filename)
        except Exception as e:
            print(f"❌ Failed to fetch BGC data for {chl_timestamp}: {e}")
            continue

        for hour in time_slots:
            timestamp = f"{date}T{hour}"
            phy_filename = f"phy_{date}_{hour.replace(':', '')}.nc"

            try:
                # ⬇️ Download physical data
                copernicusmarine.subset(
                    dataset_id=phy_dataset_id,
                    variables=variables_phy,
                    minimum_longitude=min(lon for _, lon, _, _ in grid_points),
                    maximum_longitude=max(lon for _, lon, _, _ in grid_points),
                    minimum_latitude=min(lat for lat, _, _, _ in grid_points),
                    maximum_latitude=max(lat for lat, _, _, _ in grid_points),
                    start_datetime=timestamp,
                    end_datetime=timestamp,
                    minimum_depth=phy_depth,
                    maximum_depth=phy_depth,
                    output_filename=phy_filename
                )

                ds_phy = xr.open_dataset(phy_filename)

                data_list = []
                for lat, lon, port_name, point_id in grid_points:
                    try:
                        point_phy = ds_phy.sel(latitude=lat, longitude=lon, method='nearest')
                        if np.isnan(point_phy['thetao'].values):
                            continue  # Skip land points

                        point_bgc = ds_bgc.sel(latitude=lat, longitude=lon, method='nearest')
                        u = point_phy['uo'].values.item()
                        v = point_phy['vo'].values.item()
                        current_speed = np.sqrt(u**2 + v**2)

                        data = {
                            'time': timestamp,
                            'Port': port_name,
                            'PointID': point_id,
                            'latitude': lat,
                            'longitude': lon,
                            'SST_C': point_phy['thetao'].values.item(),
                            'SSS_psu': point_phy['so'].values.item(),
                            'Current_Speed_m_s': current_speed,
                            'Chlorophyll_a_mg_m3': point_bgc['chl'].values.item()
                        }

                        data_list.append(data)
                    except Exception as point_err:
                        continue  # Skip faulty point

                df = pd.DataFrame(data_list)
                all_dfs.append(df)

            except Exception as e:
                print(f"❌ Failed to fetch physical data for {timestamp}: {e}")
            finally:
                # Close datasets to prevent HDF errors
                if 'ds_phy' in locals():
                    ds_phy.close()
                if 'ds_bgc' in locals():
                    ds_bgc.close()
                gc.collect()  # Free memory

    # 💾 Step 7: Combine and save to CSV
    if all_dfs:
        final_df = pd.concat(all_dfs)
        self.final_df = final_df
        self. final_df.sort_values(by="time", inplace=True)
    else:
        print("⚠️ No data was fetched. Check credentials or dataset availability.")
    self.cleanup_nc_files()
  def get_open_meteo_data(self):
    import requests
    import pandas as pd
    from tqdm import tqdm
    import time

    print("🔄 Converting time column to datetime...")
    self.final_df['time'] = pd.to_datetime(self.final_df['time'])

    delay = 1  # seconds
    wave_data_records = []

    print("📍 Extracting unique lat/lon points...")
    unique_points = self.final_df[['latitude', 'longitude']].drop_duplicates()

    print(f"🌐 Fetching Open-Meteo wave height data for {len(unique_points)} locations...")

    for idx, row in tqdm(unique_points.iterrows(), total=unique_points.shape[0], desc="Fetching Wave Height"):
        lat = row['latitude']
        lon = row['longitude']

        try:
            location_df = self.final_df[(self.final_df['latitude'] == lat) & (self.final_df['longitude'] == lon)]
            start_time = location_df['time'].min().strftime("%Y-%m-%d")
            end_time = location_df['time'].max().strftime("%Y-%m-%d")

            url = (
                f"https://marine-api.open-meteo.com/v1/marine?"
                f"latitude={lat}&longitude={lon}"
                f"&hourly=wave_height&start_date={start_time}&end_date={end_time}&timezone=auto"
            )

            print(f"🌊 [{idx+1}/{len(unique_points)}] Requesting wave data for ({lat:.2f}, {lon:.2f}) from {start_time} to {end_time}...")

            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            if 'hourly' in data and 'time' in data['hourly']:
                times = pd.to_datetime(data['hourly']['time'])
                heights = data['hourly']['wave_height']

                for t, h in zip(times, heights):
                    wave_data_records.append({
                        'latitude': lat,
                        'longitude': lon,
                        'time': t,
                        'wave_height': h
                    })
            else:
                print(f"⚠️  No wave data found for ({lat}, {lon}).")

        except Exception as e:
            print(f"❌ Error at ({lat}, {lon}): {e}")

        time.sleep(delay)

    print("📊 Converting wave data to DataFrame...")
    wave_df = pd.DataFrame(wave_data_records)

    print("🧬 Merging wave height with original DataFrame...")
    self.final_df['time'] = pd.to_datetime(self.final_df['time'])
    wave_df['time'] = pd.to_datetime(wave_df['time'])

    self.final_df = pd.merge(self.final_df, wave_df, on=['latitude', 'longitude', 'time'], how='left')
    self.final_df['time'] = pd.to_datetime(self.final_df['time'])

    print("🧹 Dropping rows with missing wave height...")
    self.final_df.dropna(inplace=True)
    self.final_df.reset_index(drop=True, inplace=True)

    print("✅ Wave height data integration complete!")

  def to_google_sheets(self, spreadsheet_name='Marine_Observation', worksheet_name='Weekly_Report'):
    import pandas as pd
    import gspread
    from gspread_dataframe import set_with_dataframe
    from oauth2client.service_account import ServiceAccountCredentials
    from googleapiclient.discovery import build

    SERVICE_ACCOUNT_FILE = 'credentials.json'

    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive.file',
        'https://www.googleapis.com/auth/drive'
    ]

    creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
    client = gspread.authorize(creds)

    # Attempt to open or create spreadsheet
    try:
        spreadsheet = client.open(spreadsheet_name)
        print(f"✅ Opened existing spreadsheet: {spreadsheet_name}")
    except gspread.exceptions.SpreadsheetNotFound:
        spreadsheet = client.create(spreadsheet_name)
        print(f"📄 Created new spreadsheet: {spreadsheet_name}")

    # Share the spreadsheet with full access to specified users
    try:
        drive_service = build('drive', 'v3', credentials=creds)
        file_id = spreadsheet.id
        user_emails = ['swaran.rekulapally@gmail.com', 'jnallaga@gitam.in']

        for email in user_emails:
            permission = {
                'type': 'user',
                'role': 'writer',
                'emailAddress': email
            }
            drive_service.permissions().create(
                fileId=file_id,
                body=permission,
                sendNotificationEmail=False
            ).execute()

        print("✅ Shared spreadsheet with Swaran and Jayanth.")
    except Exception as e:
        print(f"❌ Failed to share spreadsheet: {e}")

    # Try to open or create worksheet
    try:
        worksheet = spreadsheet.worksheet(worksheet_name)
        worksheet.clear()
        print(f"🔄 Cleared existing worksheet: {worksheet_name}")
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows="1000", cols="20")
        print(f"➕ Created new worksheet: {worksheet_name}")

    # Upload DataFrame
    set_with_dataframe(worksheet, self.final_df)
    print(f"✅ Data uploaded to spreadsheet ➝ Sheet: '{worksheet_name}'")
    print(f"🔗 Spreadsheet URL: {spreadsheet.url}")

  def cleanup_nc_files(self):
        import os
        import glob

        nc_files = glob.glob("*.nc")
        if not nc_files:
            print("ℹ️ No .nc files found to delete.")
            return

        for file in nc_files:
            try:
                os.remove(file)
                print(f"🗑️ Deleted file: {file}")
            except Exception as e:
                print(f"❌ Failed to delete {file}: {e}")
