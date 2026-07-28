import pandas as pd
import requests
import os
from dotenv import load_dotenv
from pprint import pprint
import pathlib
import argparse
from datetime import datetime
import re

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
        
        client = session or requests
        resp = client.get(url_template, url_params, timeout=(5, 30))
        resp.raise_for_status()
        
        resp_raw = resp.text
        raw_cache_path = pathlib.Path(__file__).parent / "data" / f"donki_{ev_type}_{ev_start}_{ev_end}.json"
        raw_cache_path.parent.mkdir(parents=True, exist_ok=True)
        raw_cache_path.write_text(resp_raw, encoding="utf-8")
        resp_json = resp.json()
        return resp_json
    
    @staticmethod
    def transform(data: list):
        def class_to_flux(class_str: str | None) -> float | None:
            flare_class_re = re.compile(r"^\s*([ABCMX])\s*(\d+(?:\.\d+)?)\s*$", re.IGNORECASE)
            class_base = {
                "A": 1e-8,
                "B": 1e-7,
                "C": 1e-6,
                "M": 1e-5,
                "X": 1e-4,
            }
            if not class_str:
                return None
            match = flare_class_re.match(class_str)
            if match is None:
                return None
            
            letter, magnitude = match.groups()
            return float(magnitude) * class_base[letter.upper()]
        
        for event in data:
            print(event['flrID'], class_to_flux(event['classType']))
    
if __name__ == "__main__":
    temp = ETLPipeline()
    temp_out = temp.extract("FLR", "2024-05-01", "2024-06-01")
    ETLPipeline.transform(temp_out)