import os
import time
import requests
import pandas as pd
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from sqlalchemy import Engine, create_engine

# gets an environment variable to avoid "hard-coding" in the API key
API_KEY = os.environ.get("AVIATIONSTACK_API_KEY")
# the Aviationstack API base url
BASE_URL = "https://api.aviationstack.com/v1"

# pick a single day
start_date = "2025-09-20"  
# pick an airport (IATA code)
airport = "MCO"                 

# defining a new class that contains the logic to call the API
class AviationStackClient: 
    def __init__(self, api_key: Optional[str] = None, base_url: str = BASE_URL, timeout: int = 20):
        self.api_key = api_key or API_KEY
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        if not self.api_key:
            raise RuntimeError("AVIATIONSTACK_API_KEY not set")

    def _get(self, endpoint: str, params: Dict) -> Dict:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        p = {"access_key": self.api_key, **params}
        response = requests.get(url, params = p, timeout = self.timeout)
        # if the HTTP status code is 4xx or 5xx, this raises a requests.HTTPError
        response.raise_for_status()
        # parse the response as JSON and return
        return response.json()

    def fetch_all(self, endpoint: str, params: Dict, page_limit: int = 100) -> List[Dict]:
        results: List[Dict] = []
        offset = 0  
        while True:
            page_params = {**params, "limit": page_limit, "offset": offset}
            data = self._get(endpoint, page_params)
            items = data.get("data", [])
            results.extend(items)

            pg = data.get("pagination") or {}
            count = pg.get("count") or len(items)
            total = pg.get("total") or len(items)
            offset += count
            
            if offset >= total or count == 0:
                break

            time.sleep(0.3)
        
        return results
    

# normalize_flight accepts a single nested dictionary from the
#  fetch_all() method and returns a single normalized dictionary
def normalize_flight(rec: Dict[str, Any]) -> Dict[str, Any]:
    # Safely get the nested sections. If they're missing, fall back to "{}"
    dep = rec.get("departure") or {}
    arr = rec.get("arrival") or {}
    airline = rec.get("airline") or {}
    flight = rec.get("flight") or {}
    aircraft = rec.get("aircraft") or {}

    return {
        # high-level fields
        "flight_date": rec.get("flight_date"),
        "flight_status": rec.get("flight_status"),

        # airline/flight identity
        "airline_name": airline.get("name"),
        "airline_iata": airline.get("iata"),
        "airline_icao": airline.get("icao"),
        "flight_number": flight.get("number"),
        "flight_iata": flight.get("iata"),
        "flight_icao": flight.get("icao"),

        # departure info
        "dep_iata": dep.get("iata"),
        "dep_icao": dep.get("icao"),
        "dep_terminal": dep.get("terminal"),
        "dep_gate": dep.get("gate"),
        "dep_scheduled": dep.get("scheduled"),
        "dep_estimated": dep.get("estimated"),
        "dep_actual": dep.get("actual"),
        "dep_delay_min": dep.get("delay"),

        # arrival info
        "arr_iata": arr.get("iata"),
        "arr_icao": arr.get("icao"),
        "arr_terminal": arr.get("terminal"),
        "arr_gate": arr.get("gate"),
        "arr_baggage": arr.get("baggage"),
        "arr_scheduled": arr.get("scheduled"),
        "arr_estimated": arr.get("estimated"),
        "arr_actual": arr.get("actual"),
        "arr_delay_min": arr.get("delay"),

        # aircraft info
        "aircraft_registration": aircraft.get("registration"),
        "aircraft_iata": aircraft.get("iata"),
        "aircraft_icao": aircraft.get("icao"),
        "aircraft_icao24": aircraft.get("icao24"),
    }

# pandas section 
def clean_dataframe(items):
    # creates the initial data frame
    df = pd.DataFrame(items)
    # Convert times to datetime if present
    for col in ["dep_scheduled","dep_estimated","dep_actual","arr_scheduled","arr_estimated","arr_actual"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
    # Makes sure flight_date is a "Date" and not "datetime"
    if "flight_date" in df.columns:
        df["flight_date"] = pd.to_datetime(df["flight_date"], errors="coerce").dt.date
    return df

# fetch the raw dep and arr data starting from a given date and return a df with both sets of data combined
def collect_day(client: AviationStackClient, start_date: str, airport: str = airport):
    # (fetch_all returns a list of dictionaries)
    # Pull flights for a single day: departures FROM an airport 
    raw_dep = client.fetch_all("flights", {"dep_iata": airport, "flight_date": start_date})
    # Pull flights for the same day: arrivals TO an airport
    raw_arr = client.fetch_all("flights", {"arr_iata": airport, "flight_date": start_date})

    # Normalize each raw record "r" into a flat dictionary
    flat_dep = []
    flat_arr = []
    # (normalize_flight both takes in and returns a single dictionary)
    # The normalized data gets stored the lists: flat_dep, flat_arr
    for r in raw_dep:
        flat_dep.append(normalize_flight(r))
    for r in raw_arr:
        flat_arr.append(normalize_flight(r))

    # convert and clean the lists to a data frame and combine them to be ready for pandas 
    df_dep = clean_dataframe(flat_dep)
    df_arr = clean_dataframe(flat_arr)
    df_day = pd.concat([df_dep, df_arr], ignore_index=True)

    return df_day


def collect_week(client: AviationStackClient, start_date: str, days: int = 7, airport: str = airport):
    # this will be the starting day (converted to a datetime object and getting only the day)
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    # this list will store the data frames collected for each day
    frames = []

    for i in range(days):
        # adds a timedelta object to add the next day (based on the number iteration of the loop)
        day = start + timedelta(days=i)
        # converts the date back to a string for the "collect_day" function
        date_str = day.strftime("%Y-%m-%d")
        print(f"Collecting {date_str}...")
        # collects the data for the given day and returns it in a dataframe
        df_day = collect_day(client, date_str, airport=airport)
        # adds the data frame to the frames list
        frames.append(df_day)
        # adds some buffertime between days 
        time.sleep(0.3)
    # Concatenate all days into one DataFrame
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

# shows metrics like departures, arrivals, delayed dep/arr, and cancelled flights for a given day and airport
def daily_core_metrics(df, airport=airport):
    # total number of departures from "airport" for a given date, renamed to "departures"
    dep = df[df["dep_iata"] == airport].groupby("flight_date", dropna=False).size().rename("departures")
    # total number of arrivals to "airport" for a given date, renamed to "arrivals"
    arr = df[df["arr_iata"] == airport].groupby("flight_date", dropna=False).size().rename("arrivals")
    # both arrivals and departures concatinated together
    daily = pd.concat([dep, arr], axis=1).fillna(0).astype(int)

    # delays of 15 minutes or more & cancellations by day
    delayed_dep = (df[(df["dep_iata"] == airport) & (df["dep_delay_min"].fillna(0) >= 15)]
                   .groupby("flight_date").size().rename("delayed_departures"))
    delayed_arr = (df[(df["arr_iata"] == airport) & (df["arr_delay_min"].fillna(0) >= 15)]
                   .groupby("flight_date").size().rename("delayed_arrivals"))
    cancelled = (df[df["flight_status"] == "cancelled"]
                 .groupby("flight_date").size().rename("cancelled_flights"))

    return (daily.join(delayed_dep, how="left")
                 .join(delayed_arr, how="left")
                 .join(cancelled, how="left")
                 .fillna(0).astype(int))


# number of flights per airline touching an airport
def airline_mix_daily(df, airport=airport):
    # rows where "airport" is either the departure or arrival
    mask = (df["dep_iata"].eq(airport)) | (df["arr_iata"].eq(airport))
    out = (df[mask]
            .groupby(["flight_date", "airline_name","airline_iata"], dropna=False).size()
            .rename("flights").reset_index())
    # returns a data frame that shows the date, airline, airline_iata, and num of flights for an airport
    return out.sort_values(["flight_date", "flights"], ascending=[True, False])

# returns the top 15 destinations from and top origins to an airport 
def top_routes_from_airport_daily(df, airport, n=15):
    # popular destinations flying out of "airport"
    dep_counts = (df[df["dep_iata"] == airport]
                .groupby(["flight_date", "arr_iata"], dropna=False)
                .size().rename("flights").reset_index()
                .rename(columns={"arr_iata": "destination"}))
    # sorting by flight_data/flights and grouping by flight_date
    dep_top = (dep_counts
            .sort_values(["flight_date", "flights"], ascending=[True, False])
            .groupby("flight_date", group_keys=False)
            .head(n))
    
    # popular origins flying to "airport"
    arr_counts = (df[df["arr_iata"] == airport]
           .groupby(["flight_date", "dep_iata"], dropna=False)
           .size().rename("flights").reset_index()
           .rename(columns={"dep_iata": "origin"}))
    # sorting by flight_data/flights and grouping by flight_date
    arr_top = (arr_counts
           .sort_values(["flight_date", "flights"], ascending=[True, False])
           .groupby("flight_date", group_keys=False)
           .head(n))
    
    # replaces missing data with "Unknown"
    dep_top["destination"] = dep_top["destination"].fillna("Unknown")
    arr_top["origin"]      = arr_top["origin"].fillna("Unknown")
    
    return dep_top, arr_top

# initialize the client object (requires AVIATIONSTACK_API_KEY)
client = AviationStackClient() 

# # just collects data of a single day and prints to the console for testing purposes
# try:
#     # 1) Fetch + normalize + clean (one day)
#     test_df_day = collect_day(client=client, start_date=start_date, airport=airport)

#     # 2) Basic sanity checks
#     print(f"[TEST] Airport: {airport} | Date: {start_date}")
#     print(f"[TEST] df_day shape: {test_df_day.shape}")             # (rows, cols)
#     print(f"[TEST] Columns: {list(test_df_day.columns)}")

#     # peek a few rows (transpose for easier scanning of columns vs values)
#     print("[TEST] Sample rows:")
#     print(test_df_day.head(3).T)

#     # 3) Optionally run aggregations in-memory (still no writing)
#     daily = daily_core_metrics(test_df_day, airport=airport)
#     airlines = airline_mix_daily(test_df_day, airport=airport)
#     top_from, top_to = top_routes_from_airport_daily(test_df_day, airport=airport, n=15)

#     print(f"\n[TEST] daily_core_metrics:\n", daily)
#     print(f"\n[TEST] airline_mix (top 5):\n", airlines.head())
#     print(f"\n[TEST] top destinations from {airport}:\n", top_from)
#     print(f"\n[TEST] top origins to {airport}:\n", top_to)

# except Exception as e:
#     print("[Test] Failed with error:", repr(e))

# gets the enviroment variable for the database URL to use with create_engine() and returns the result
def get_engine_from_url():
    url = os.environ.get("DB_URL")
    if not url:
        raise RuntimeError("DB_URL not set")
    return create_engine(url, pool_pre_ping=True)

# # initialize the client object
# client = AviationStackClient() 

# saves the weekly data and drops duplicates
df_week = collect_week(client, start_date, days=7, airport=airport).drop_duplicates(
    subset=["flight_date","flight_iata","dep_scheduled","arr_scheduled"]
    )

# Core (arrivals vs departures + reliability)
daily = daily_core_metrics(df_week, airport=airport)
# -> columns: departures, arrivals, delayed_departures, delayed_arrivals, cancelled_flights

# Airline mix
airlines = airline_mix_daily(df_week, airport=airport)

# Top routes
top_from, top_to = top_routes_from_airport_daily(df_week, airport=airport, n=15)

# path to the folder that will store the flight data CSVs
path = "/Users/garymontero/Documents/CEN4930C - SASD/DIY_API_Project/API_flight_data/"

# (Optional) save for Tableau
daily.to_csv(f"{path}daily_core_metrics_{start_date}.csv", index=True, index_label="flight_date")   # index is flight_date
airlines.to_csv(f"{path}airline_mix_{start_date}.csv", index=False)
top_to.to_csv(f"{path}top_routes_to_{airport}_{start_date}.csv", index=False)
top_from.to_csv(f"{path}top_routes_from_{airport}_{start_date}.csv", index=False)

# get_engine_from_url() returns the result of create_engine(), which is then stored in a variable "engine"
engine = get_engine_from_url()

daily_out = daily.reset_index()
daily_out["flight_date"] = pd.to_datetime(daily_out["flight_date"]).dt.date
daily_out.to_sql("daily_metrics", engine, if_exists="replace", index=False)

airlines.to_sql("airline_mix_daily", engine, if_exists="replace", index=False)
top_from.to_sql(f"routes_from_{airport}_daily", engine, if_exists="replace", index=False)
top_to.to_sql(f"routes_to_{airport}_daily", engine, if_exists="replace", index=False)

# # Run this after the first week's data is saved
# daily_out = daily.reset_index()
# daily_out["flight_date"] = pd.to_datetime(daily_out["flight_date"]).dt.date
# daily_out.to_sql("daily_metrics", engine, if_exists="append", index=False)

# airlines.to_sql("airline_mix_daily", engine, if_exists="append", index=False)
# top_from.to_sql(f"routes_from_{airport}_daily", engine, if_exists="append", index=False)
# top_to.to_sql(f"routes_to_{airport}_daily", engine, if_exists="append", index=False)


# # this is a test function for saving to a CSV and the database
# def save_one_day_outputs(start_date=start_date, airport=airport):
#     # 1) collect one day
#     client = AviationStackClient()
#     df_day = collect_day(client, start_date=start_date, airport=airport)

#     # 2) aggregations (in-memory)
#     daily     = daily_core_metrics(df_day, airport=airport)
#     airlines  = airline_mix(df_day, airport=airport)
#     top_from, top_to = top_routes_from_airport_daily(df_day, airport=airport, n=15)

# path = "/Users/garymontero/Documents/CEN4930C - SASD/DIY_API_Project/API_flight_data/"

#     # 3) write CSVs (nice to eyeball or publish to Tableau)
#     daily.to_csv(f"{path}daily_core_metrics_{start_date}.csv", index=True)  # flight_date remains index
#     airlines.to_csv(f"{path}airline_mix_{start_date}.csv", index=False)
#     top_from.to_csv(f"{path}top_routes_from_{airport}_{start_date}.csv", index=False)
#     top_to.to_csv(f"{path}top_routes_to_{airport}_{start_date}.csv", index=False)
#     print("[SAVE] CSVs written for", start_date)

#     # 4) write to MySQL (env-driven engine)
#     engine = get_engine_from_url()

#     # Important: bring flight_date out of the index before writing
#     daily.reset_index().to_sql("one_day_metrics", engine, if_exists="replace", index=False)
#     airlines.to_sql("airline_mix", engine, if_exists="replace", index=False)
#     # FROM "airport" (destination)
#     top_from.to_sql(f"routes_from_{airport}", engine, if_exists="replace", index=False)
#     # TO "airport" (origin)
#     top_to.to_sql(f"routes_to_{airport}", engine, if_exists="replace", index=False)
#     print("[SAVE] MySQL tables written:", ["daily_metrics", "airline_mix", "aircraft_mix", f"routes_from_{airport}", f"routes_to_{airport}"])


# if __name__ == "__main__":
#     # run a one-day test write (CSV + DB)
#     save_one_day_outputs(start_date=start_date, airport=airport)