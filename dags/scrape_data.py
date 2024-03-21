from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import csv

def scrape_data():
    import requests
    from bs4 import BeautifulSoup

    scraped_data = []  # Initialize an empty list to store data

    for page_number in range(2, 12):
        url = f"https://www.ebay.co.uk/sch/i.html?_from=R40&_trksid=p2334524.m570.l1313&_nkw=grade+10+pokemon+card&_sacat=0&LH_TitleDesc=0&rt=nc&_odkw=grade+10+pokemon+card&_osacat=0&LH_BIN=1&_ipg=240&_pgn={page_number}"  # Your original URL here
        soup = get_data(url)
        scraped_data.extend(parse(soup))  # Add parsed data from each page

    # Save data to CSV file
    save_to_csv(scraped_data)

def save_to_csv(data):
    with open(r"C:\Users\Karan\DataEngineeringProject1\csv-files", "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["name", "price", "link", "postage_cost"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for item in data:
            writer.writerow(item)

# Get raw data function (unchanged)
def get_data(url):
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "html.parser")
    return soup

# Parse page data function (unchanged)
def parse(soup):
    results = soup.find_all("div", {"class": "s-item__info clearfix"})
    parsed_data = []
    for item in results:
        name = item.find("div", {"class": "s-item__title"}).text
        price = item.find("span", {"class": "s-item__price"}).text
        link = item.find("a", {"class": "s-item__link"})["href"]
        postageCost = str(item.find("span", {"s-item__shipping s-item__logisticsCost"}))
        parsed_data.append({
            "name": name,
            "price": price,
            "link": link,
            "postage_cost": postageCost
        })
    return parsed_data

# Define your DAG
with DAG(
    "scrape_data",
    start_date=datetime(2024, 3, 22),
    schedule_interval="daily",
    catchup=False
) as dag:

    # Task for data scraping
    scrape_task = PythonOperator(
        task_id="scrape_data_only",
        python_callable=scrape_data
    )
