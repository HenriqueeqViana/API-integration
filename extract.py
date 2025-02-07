import logging
from sodapy import Socrata
import time

class ExtractAPI:
    def __init__(self, base_url, dataset_id, auth_token=None, timeout=100, max_retries=3, retry_delay=5):
        self.base_url = base_url
        self.dataset_id = dataset_id
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.client = Socrata(self.base_url, auth_token, timeout=self.timeout)

    def fetch_data(self, page=1, limit=2000, funding_year=None):
        retries = 0
        while retries < self.max_retries:
            try:
                where_clause = f"funding_year = '{funding_year}'" if funding_year else None
                results = self.client.get(
                    self.dataset_id,
                    limit=limit,
                    offset=(page - 1) * limit,
                    where=where_clause
                )
                return results
            except Exception as error:
                retries += 1
                logging.error("Request failed: %s", error)
                if retries < self.max_retries:
                    logging.info(f"Retrying... Attempt {retries}/{self.max_retries}")
                    time.sleep(self.retry_delay)
                else:
                    logging.error("Max retries reached. Giving up.")
                    return []

    def get_complete_data(self, funding_year):
        all_data = []
        page = 1
        while True:
            data = self.fetch_data(page, funding_year=funding_year)
            if not data:
                break
            all_data.extend(data)
            page += 1
        return all_data
