import requests
from bs4 import BeautifulSoup
import time

class NepalDataScraper:
    def __init__(self):
        self.dhm_url = "https://dhm.gov.np/hydrology/realtime-stream"
        self.nea_url = "https://nea.org.np/"

    def scrape_hydro(self):
        """Extracts table data from DHM Nepal."""
        try:
            response = requests.get(self.dhm_url, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser')
            table = soup.find('table')
            rows = table.find_all('tr')[1:]
            
            data = []
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 5:
                    # Clean -999999 values (Critical for data quality)
                    val = cols[4].text.strip()
                    level = float(val) if val and float(val) > -900000 else 0.0
                    
                    data.append({
                        "basin": cols[1].text.strip(),
                        "station": cols[2].text.strip(),
                        "water_level_m": level,
                        "timestamp": int(time.time())
                    })
            return data
        except Exception as e:
            print(f"DHM Scrape Error: {e}")
            return []