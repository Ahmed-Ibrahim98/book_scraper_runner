import requests
import time
import json
from pymongo import MongoClient

API_KEY = "5550404d8c3545d995eee4473cfd41cd"
PROJECT_ID = "824854"
SPIDER_NAME = "bookspider"


# ------------------- NETWORK UTILITIES -------------------

def check_internet(url="https://www.google.com", timeout=5):
    """
    Quick connectivity check.
    Tries to reach a known reliable host (Google).
    Returns True if reachable, False otherwise.
    """
    try:
        requests.get(url, timeout=timeout)
        return True
    except requests.RequestException:
        return False


def wait_for_connection():
    """
    Blocks execution until the internet connection is restored.
    Keeps retrying every 10 seconds.
    """
    print("⚠️ No internet connection. Waiting...")
    while not check_internet():
        time.sleep(10)
    print("✅ Internet connection restored.")


def safe_request(method, url, **kwargs):
    """
    Wrapper for requests:
    - Retries automatically on *connection errors* (e.g. WiFi drop, DNS fail).
    - Raises immediately on *non-network errors* (e.g. 400 Bad Request, 500).
    - Always ensures internet connection before retrying.
    """
    while True:
        if not check_internet():
            wait_for_connection()

        try:
            response = requests.request(method, url, **kwargs)
            return response  # let caller decide how to handle status_code
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:
            print(f"⚠️ Network error: {e}. Retrying after reconnect...")
            wait_for_connection()
        except requests.exceptions.RequestException as e:
            # This covers HTTP errors due to invalid request, auth, etc.
            print(f"❌ Non-retryable error: {e}")
            raise


# ------------------- ZYTE SPIDER API -------------------

def run_spider():
    """
    Starts a spider run on Zyte Cloud.
    Returns the job_id if successful, else raises an error.
    """
    start_url = "https://app.zyte.com/api/run.json"
    response = safe_request(
        "POST",
        start_url,
        auth=(API_KEY, ''),
        data={"project": PROJECT_ID, "spider": SPIDER_NAME},
    )

    if response.status_code != 200:
        raise RuntimeError(f"Failed to start job: {response.text}")

    job_data = response.json()
    job_id = job_data.get("jobid")

    if not job_id:
        raise RuntimeError(f"No jobid returned: {job_data}")

    print(f"🚀 Started job: {job_id}")
    return job_id


def wait_for_job(job_id):
    """
    Polls Zyte Cloud for job status until it's finished or failed.
    Returns the final state ("finished" or "failed").
    """
    status_url = f"https://app.zyte.com/api/jobs/list.json?project={PROJECT_ID}&job={job_id}"

    while True:
        response = safe_request("GET", status_url, auth=(API_KEY, ''))

        if response.status_code != 200:
            raise RuntimeError(f"Failed to fetch status: {response.text}")

        data = response.json()
        job_list = data.get("jobs", [])

        if not job_list:
            raise RuntimeError(f"No job info returned: {data}")

        job_status = job_list[0]
        state = job_status.get("state")

        if state in ("finished", "failed"):
            print(f"✅ Job {job_id} finished with state: {state}")
            return state

        print(f"⏳ Job {job_id} still running... waiting 10s")
        time.sleep(10)


def fetch_items(job_id):
    """
    Downloads all scraped items from Zyte storage for a given job.
    Returns a list of parsed JSON records.
    """
    items_url = f"https://storage.zyte.com/items/{job_id}"
    response = safe_request("GET", items_url, auth=(API_KEY, ''))

    if response.status_code != 200:
        raise RuntimeError(f"Failed to fetch items: {response.text}")

    items = []
    for line in response.iter_lines():
        if line:
            items.append(json.loads(line.decode("utf-8")))

    return items


# ------------------- MONGO STORAGE -------------------

def get_mongo_collection():
    """
    Connects to MongoDB (Atlas cluster).
    - Retries on connection errors.
    - Ensures 'upc' field has a unique index.
    Returns the 'books' collection handle.
    """
    password = "7zejV520X22r1HMx"
    uri = f"mongodb+srv://test:{password}@book-scraper.i3nbzid.mongodb.net/?retryWrites=true&w=majority&appName=book-scraper"

    while True:
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")  # quick connectivity check
            db = client["scrapy_data"]
            collection = db["books"]
            collection.create_index("upc", unique=True)  # enforce unique UPCs
            return collection
        except Exception as e:
            print(f"⚠️ MongoDB connection error: {e}. Retrying...")
            wait_for_connection()


def save_to_mongo(items):
    """
    Inserts or updates scraped items into MongoDB.
    Uses 'upc' field as a unique key to prevent duplicates.
    """
    collection = get_mongo_collection()

    if items:
        for item in items:
            collection.update_one(
                {"upc": item.get("upc")},   # match by unique UPC
                {"$set": item},             # update fields with new data
                upsert=True                 # insert if not found
            )
        print(f"📦 Upserted {len(items)} items into MongoDB!")
    else:
        print("ℹ️ No items to upsert.")


# ------------------- MAIN SCRIPT -------------------

if __name__ == "__main__":
    """
    Flow:
    1. Start the spider → get job_id.
    2. Wait for the job to finish (poll status).
    3. Once finished, fetch scraped items.
    4. Save items into MongoDB (insert/update).
    """
    job_id = None
    try:
        job_id = run_spider()
        if job_id:
            state = wait_for_job(job_id)
            if state == "finished":
                items = fetch_items(job_id)
                if items:
                    print(f"📥 Fetched {len(items)} items.")
                    save_to_mongo(items)
                else:
                    print("ℹ️ No items fetched after job finished.")
    except Exception as e:
        print(f"❌ Fatal error occurred: {e}")
