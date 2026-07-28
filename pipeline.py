import pandas as pd
import requests
import os
from dotenv import load_dotenv
from pprint import pprint
import pathlib
import argparse
from datetime import datetime
import re
import json
import sqlite3

load_dotenv()

def get_args():
    parser = argparse.ArgumentParser()
    
    parser.add_argument(
        '-t', '--type',
        type=str,
        help="String that denotes the type of data that will be queried from NASA's API"
    )
    
    parser.add_argument(
        '-s', '--start_date',
        type=str,
        help="Start date for API call. Must be in YYYY-MM-DD format"
    )
    
    parser.add_argument(
        '-e', '--end_date',
        type=str,
        help="End date for API call. Must be in YYYY-MM-DD format"
    )
    
    args = parser.parse_args()
    
    return args
    
def main(args=None):
    temp = ETLPipeline()
    if args is None:
        args = get_args()
        temp_out = temp.extract(args.type, args.start_date, args.end_date)
    pprint(temp_out)

class ETLPipeline():
    def __init__(self):
        self.api_key = os.getenv("API_KEY")
        self.refresh_check: bool = False
    
    def extract(self, ev_type: str, ev_start: str, ev_end: str, session: None | requests.Session = None):
        # ev_type = ev_type if ev_type is not None else self.event_type
        # ev_start = ev_start if ev_start is not None else self.startdate
        # ev_end = ev_end if ev_end is not None else self.enddate
        # session = session if session is not None else self.curr_session
        
        url_template = f"https://api.nasa.gov/DONKI/{ev_type}"
        url_params = {
            "startDate": ev_start,
            "endDate": ev_end,
            "api_key": self.api_key
        }
        
        raw_cache_path = pathlib.Path(__file__).parent / "data" / f"donki_{ev_type}_{ev_start}_{ev_end}.json"
        raw_cache_path.parent.mkdir(parents=True, exist_ok=True)
        if os.path.exists(raw_cache_path):
            with open(raw_cache_path, 'r', encoding='utf-8') as file:
                resp_json = json.load(file)
                return resp_json
        
        client = session or requests
        resp = client.get(url_template, url_params, timeout=(5, 30))
        resp.raise_for_status()
        
        resp_raw = resp.text
        raw_cache_path.write_text(resp_raw, encoding="utf-8")
        resp_json = resp.json()
        return resp_json
    
    @staticmethod
    def transform(data: list, data_type: str) -> dict | None:
        FLARE_CLASS_RE = re.compile(r"^\s*([ABCMX])\s*(\d+(?:\.\d+)?)\s*$", re.IGNORECASE)
        SOURCE_LOCATION_RE = re.compile(r"([NS])(\d+)([EW])(\d+)")
        CLASS_BASE = {
            "A": 1e-8,
            "B": 1e-7,
            "C": 1e-6,
            "M": 1e-5,
            "X": 1e-4,
        }
        
        df = pd.DataFrame(data)
        
        if df.size == 0:
            return None
        
        def flr_clean_filter(flr_df: pd.DataFrame) -> pd.DataFrame:
            df_copy = flr_df.copy(deep=True)
            
            extracted_flare_class = df_copy['classType'].str.extract(FLARE_CLASS_RE)
            extracted_source_location = df_copy['sourceLocation'].str.extract(SOURCE_LOCATION_RE)
            extracted_source_location.columns = ['lat_dir', 'lat_val', 'lon_dir', 'lon_val']
            lat_mag = pd.to_numeric(extracted_source_location['lat_val'], errors="coerce")
            lon_mag = pd.to_numeric(extracted_source_location['lon_val'], errors="coerce")
            
            
            df_copy['flare_class'] = extracted_flare_class[0].str.upper()
            df_copy['flare_magnitude'] = pd.to_numeric(extracted_flare_class[1], errors="coerce")
            df_copy['peak_flux_wm2'] = df_copy['flare_magnitude'] * df_copy['flare_class'].map(CLASS_BASE)
            
            df_copy['lat'] = lat_mag.where(extracted_source_location['lat_dir'] == 'N', -lat_mag)
            df_copy['lon'] = lon_mag.where(extracted_source_location['lon_dir'] == 'W', -lon_mag)
            
            df_copy['beginTime'] = pd.to_datetime(df_copy['beginTime'], format='ISO8601')
            df_copy['peakTime'] = pd.to_datetime(df_copy['peakTime'], format='ISO8601')
            df_copy['endTime'] = pd.to_datetime(df_copy['endTime'], format='ISO8601')
            df_copy['submissionTime'] = pd.to_datetime(df_copy['submissionTime'], format='ISO8601')
            
            df_copy['activeRegionNum'] = df_copy['activeRegionNum'].astype('Int64')
            
            df_copy = df_copy.drop('classType', axis=1)
            df_copy = df_copy.sort_values(by='versionId', ascending=False)
            df_copy = df_copy.drop_duplicates(subset='flrID')
            df_copy = df_copy.drop('sentNotifications', axis=1)
            
            return df_copy
        
        def gst_clean_filter(gst_df: pd.DataFrame) -> pd.DataFrame:
            df_copy = gst_df.copy(deep=True)
            
            df_copy['startTime'] = pd.to_datetime(df_copy['startTime'], format='ISO8601')
            df_copy['submissionTime'] = pd.to_datetime(df_copy['submissionTime'], format='ISO8601')
            
            df_copy = df_copy.sort_values(by='versionId', ascending=False)
            df_copy = df_copy.drop_duplicates(subset='gstID')
            df_copy = df_copy.drop('sentNotifications', axis=1)
            
            return df_copy
        
        def cme_clean_filter(cme_df: pd.DataFrame) -> pd.DataFrame:
            df_copy = cme_df.copy(deep=True)
            
            extracted_source_location = df_copy['sourceLocation'].str.extract(SOURCE_LOCATION_RE)
            extracted_source_location.columns = ['lat_dir', 'lat_val', 'lon_dir', 'lon_val']
            lat_mag = pd.to_numeric(extracted_source_location['lat_val'], errors="coerce")
            lon_mag = pd.to_numeric(extracted_source_location['lon_val'], errors="coerce")
            
            df_copy['startTime'] = pd.to_datetime(df_copy['startTime'], format='ISO8601')
            df_copy['submissionTime'] = pd.to_datetime(df_copy['submissionTime'], format='ISO8601')
            
            df_copy['activeRegionNum'] = df_copy['activeRegionNum'].astype('Int64')
            
            df_copy['lat'] = lat_mag.where(extracted_source_location['lat_dir'] == 'N', -lat_mag)
            df_copy['lon'] = lon_mag.where(extracted_source_location['lon_dir'] == 'W', -lon_mag)
            
            df_copy = df_copy.sort_values(by='versionId', ascending=False)
            df_copy = df_copy.drop_duplicates(subset='activityID')
            df_copy = df_copy.drop('sentNotifications', axis=1)
            
            return df_copy
        
        def event_links_filter(data_df: pd.DataFrame, id_col: str):
            linked_df = data_df.copy(deep=True)
            
            links = linked_df[[id_col, 'linkedEvents']].explode('linkedEvents')
            links['linkedEvents'] = links['linkedEvents'].str.get('activityID')
            
            links = links.dropna(subset='linkedEvents')
            links.columns = ['source_id', 'linked_id']
            
            return links.reset_index(drop=True)
            
        def kp_index_explode(data_df: pd.DataFrame):
            kp_df = data_df.copy(deep=True)
            
            kp_df = kp_df.dropna(subset=['allKpIndex'])
            
            kp_df_exploded = kp_df[['gstID','allKpIndex']].explode('allKpIndex').reset_index(drop=True)
            kp_df_exploded_normalize = pd.json_normalize(kp_df_exploded['allKpIndex'])
            
            kp_df_concat = pd.concat([kp_df_exploded, kp_df_exploded_normalize], axis=1)
            kp_df_concat = kp_df_concat.drop('allKpIndex', axis=1)
            
            kp_df_concat['observedTime'] = pd.to_datetime(kp_df_concat['observedTime'], format='ISO8601')
            
            return kp_df_concat
        
        def cme_analysis_filter(data_df: pd.DataFrame):
            cme_df = data_df.copy(deep=True)
            
            cme_df = cme_df.dropna(subset='cmeAnalyses')

            cme_df_analyses_explode = cme_df[['activityID', 'cmeAnalyses']].explode('cmeAnalyses').reset_index(drop=True)
            cme_df_analyses_normalize = pd.json_normalize(cme_df_analyses_explode['cmeAnalyses'])
            
            cme_df_concat = pd.concat([cme_df_analyses_explode, cme_df_analyses_normalize[['isMostAccurate', 'time21_5', 'halfAngle',
                                                                                           'speed', 'type', 'latitude',
                                                                                           'longitude', 'levelOfData']]], axis=1)
            
            cme_df_concat = cme_df_concat[cme_df_concat['isMostAccurate'] == True].reset_index(drop=True)
            cme_df_concat = cme_df_concat.sort_values(by='levelOfData', ascending=False)
            cme_df_concat = cme_df_concat.drop_duplicates(subset='activityID')
            
            cme_df_concat['time21_5'] = pd.to_datetime(cme_df_concat['time21_5'], format='ISO8601')
            
            cme_df_concat = cme_df_concat.drop('cmeAnalyses', axis=1)
            
            return cme_df_concat
            
        if data_type == "FLR":
            flares_df = flr_clean_filter(df)
            event_links_flr = event_links_filter(flares_df, 'flrID')
            flares_df = flares_df.drop('linkedEvents', axis=1)
            df_dict = {
                'flares': flares_df,
                'event_links': event_links_flr
            }
            return df_dict
        elif data_type == "GST":
            storms_df = gst_clean_filter(df)
            event_links_gst = event_links_filter(storms_df, 'gstID')
            storms_df = storms_df.drop('linkedEvents', axis=1)
            kp_filtered = kp_index_explode(storms_df)
            storms_df = storms_df.drop('allKpIndex', axis=1)
            
            temp_kp_filtered = kp_filtered.copy().groupby('gstID')
            kp_filtered_max_kp_time_diff = temp_kp_filtered.agg(
                max_kp = ('kpIndex', 'max'),
                first_obs = ('observedTime', 'min'),
                last_obs = ('observedTime', 'max')
            )
            
            kp_filtered_max_kp_time_diff = kp_filtered_max_kp_time_diff.reset_index()
            storms_df = pd.merge(storms_df, kp_filtered_max_kp_time_diff, on='gstID', how='left')
            
            storms_df['storm_duration_hours'] = storms_df['last_obs'] - storms_df['first_obs']
            storms_df['storm_duration_hours'] = storms_df['storm_duration_hours'].dt.total_seconds() / 3600
            
            df_dict = {
                'storms': storms_df,
                'event_links': event_links_gst,
                'kp_observations': kp_filtered
            }
            return df_dict
        elif data_type == "CME":
            cmes_df = cme_clean_filter(df)
            event_links_cme = event_links_filter(cmes_df, 'activityID')
            cmes_df = cmes_df.drop('linkedEvents', axis=1)
            cmes_analyses = cme_analysis_filter(cmes_df)
            cmes_df = cmes_df.drop('cmeAnalyses', axis=1)
            df_dict = {
                'cmes': cmes_df,
                'event_links': event_links_cme,
                'cme_analyses': cmes_analyses,
            }
            return df_dict
        else:
            raise ValueError(
                f"Unsupported data_type {data_type!r}; expected one of 'FLR', 'GST', 'CME'"
            )
    
if __name__ == "__main__":
    temp = ETLPipeline()
    temp_out = temp.extract("GST", "2024-05-01", "2024-06-01")
    transform_out = ETLPipeline.transform(temp_out, 'GST')
    for df in transform_out.values():
        print(df.shape)
    # for event in transform_out['activeRegionNum']:
    #     print(str(type(event)), ':', event)
    # print(transform_out[['link', 'linkedEvents']].head(10))