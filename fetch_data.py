import os
import requests
import pandas as pd
import io
from dotenv import load_dotenv

class Extract:
    """
    Contains all logic to fetch from API and write to data.csv
    """
    def __init__(self) -> None:
        self.base_url = os.environ.get("base-url")
        self.url = self.base_url+"/api/3/action/package_show"
        self.params = {"id": os.environ.get("params")}
        self.package = requests.get(self.url, params = self.params).json()
        self.MASTER_DATA_PATH = 'raw_data.csv'
        self.dtypes = {
            "_id":"int32",
            "OCCUPANCY_DATE":"datetime64",
            "ORGANIZATION_ID":"float32",
            "ORGANIZATION_NAME":"string",
            "SHELTER_ID":"float32",
            "SHELTER_GROUP":"string",
            "LOCATION_ID":"float32",
            "LOCATION_NAME":"string",
            "LOCATION_ADDRESS":"string",
            "LOCATION_POSTAL_CODE":"string",
            "LOCATION_CITY":"string",
            "LOCATION_PROVINCE":"string",
            "PROGRAM_ID":"float32",
            "PROGRAM_NAME":"string",
            "SECTOR":"category",
            "PROGRAM_MODEL":"category",
            "OVERNIGHT_SERVICE_TYPE":"category",
            "PROGRAM_AREA":"category",
            "SERVICE_USER_COUNT":"float32",
            "CAPACITY_TYPE":"category",
            "CAPACITY_ACTUAL_BED":"float32",
            "CAPACITY_FUNDING_BED":"float32",
            "OCCUPIED_BEDS":"float32",
            "UNOCCUPIED_BEDS":"float32",
            "UNAVAILABLE_BEDS":"float32",
            "CAPACITY_ACTUAL_ROOM":"float32",
            "CAPACITY_FUNDING_ROOM":"float32",
            "OCCUPIED_ROOMS":"float32",
            "UNOCCUPIED_ROOMS":"float32",
            "UNAVAILABLE_ROOMS":"float32",
            "OCCUPANCY_RATE_BED":"float32",
            "OCCUPANCY_RATE_ROOM":"float32"
        }
      
    def clear_file(self):
        """ 
        Clears the data.csv file before rewrite
        """
        if os.path.isfile(self.MASTER_DATA_PATH):
            with open(self.MASTER_DATA_PATH, 'w') as file:
                pass
    
    def fetch_data_from_api(self):
        """
        Iterates over each API resource, saves to .csv
        """
        for idx, resource in enumerate(self.package["result"]["resources"]):
            if resource["datastore_active"]:
                url = self.base_url + "/datastore/dump/" + resource["id"]
                resource_dump_data = requests.get(url).text
                headless_resource_dump_data = pd.read_csv(io.StringIO(resource_dump_data), header = None, skiprows = [0])
                headless_resource_dump_data.to_csv(self.MASTER_DATA_PATH, index = False, mode = 'a', header = False)
        self.add_headers()  
    
    def add_headers(self):
        """
        Adds headers to first row 
        """
        headless_data = pd.read_csv(self.MASTER_DATA_PATH, names = self.dtypes.keys())
        headless_data.to_csv(self.MASTER_DATA_PATH, index = False)
    
def main():
    load_dotenv()
    extract=Extract()
    extract.clear_file()
    extract.fetch_data_from_api()
    print("Complete!")

if __name__ == "__main__":
    main()

    