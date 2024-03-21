from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

with DAG("scrape_data", start_date=datetime(2024, 3, 22),
         schedule_interval="daily", catchup=False) as dag:

    # Task for data scraping
    scrape_task = PythonOperator(
        task_id="scrape_data_only",
        python_callable=scrape_data
    )


def scrape_data():
    import requests
    from bs4 import BeautifulSoup

    scraped_data = []  # Initialize an empty list to store data

    for page_number in range(2, 12):
        url = f"https://www.ebay.co.uk/sch/i.html?_from=R40..."  # Your original URL here
        soup = get_data(url)
        scraped_data.extend(parse(soup))  # Add parsed data from each page

    # You can now use the scraped_data list for further processing
    # (e.g., print it, store it in a file, pass it to another task)
    print(scraped_data)  # Example printing

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