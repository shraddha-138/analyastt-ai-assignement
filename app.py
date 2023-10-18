from bs4 import BeautifulSoup
import pandas as pd 
import requests
import random
import time
import os.path

BASE_URL = "https://www.amazon.in"
HEADERS =  ({
    'User-Agent':"Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/118.0", "Accept-Encoding":"gzip, deflate", "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "DNT":"1","Connection":"close", "Upgrade-Insecure-Requests":"1"
})

# scraper function
# @param page: page number to be scraped
def scrape_catalogue(page):
    try:
        # send the http request 
        res = requests.get(f"{BASE_URL}/s?k=bags&page={page}", headers=HEADERS)
        print(f"Request status code for page {page}: {res.status_code}")
        if (res.status_code != 200):
            print("Error loading webpage, Error code=",res.status_code)
            return
        # parse the html content
        soup = BeautifulSoup(res.content, 'html.parser')
        # get all product rows
        rows = soup.find_all('div',class_='sg-col-20-of-24 s-result-item s-asin sg-col-0-of-12 sg-col-16-of-20 sg-col s-widget-spacing-small sg-col-12-of-16')

        # Select the product details section
        data_sections = []
        for x in rows:
            row_data = x.select('.s-list-col-right > .sg-col-inner > .a-section.a-spacing-small.a-spacing-top-small')
            if (len(row_data) > 0):
                prod_data = {}
                prod_data["url"]= row_data[0].find('a')['href'].split("ref=")[0]
                prod_data["name"] = row_data[0].select(".a-size-medium.a-color-base.a-text-normal")[0].text
                prod_data["price"]= row_data[0].find('span', class_="a-price-whole").text
                ratings = row_data[0].find('div', class_="a-row a-size-small")
                if ratings is not None:
                    prod_data["rating"] =  ratings.find('span', class_="a-icon-alt").text.split(" ")[0]
                    prod_data["number_of_reviews"] = ratings.find('span', class_="a-size-base").text
                else:
                    prod_data[ratings] = '0'
                    prod_data["number_of_reviews"] = '0'
                data_sections.append(prod_data)
        # check if the file exists and is empty
        if (not os.path.isfile('amazon_products_listing.csv') or os.stat('amazon_products_listing.csv').st_size == 0):
            df = pd.DataFrame(data_sections)
            df.to_csv('amazon_products_listing.csv', mode='a', header=[
            'url', 'name', 'price', 'rating', 'number_of_reviews'
            ], index=False)
        else:
            df = pd.DataFrame(data_sections)
            df.to_csv('amazon_products_listing.csv', mode='a', header=False, index=False)
        print(f"Page {page} scraped successfully")
    except Exception as e:
        print(e)
        ## Enhancement: implement logging
        print(f"Some error occurred while scraping page {page}")
        return
      
# function to scrape product description
# @param df: dataframe containing the product details
# @param i: index of the product to be scraped
def scrape_product_desc(df,i):
    try:
        url = df.iloc[i]['url']
        print(f"Scraping product {i+1} with url {BASE_URL}{url}")
        res = requests.get(f"{BASE_URL}{url}", headers=HEADERS)
        if (res.status_code != 200):
            print("Error loading webpage, Error code=",res.status_code)
            return
        print(f"Request status code for product {i+1}: {res.status_code}")
        # parse the html content
        soup = BeautifulSoup(res.content, 'html.parser')

         # get the meta description
        meta_description = soup.find_all('meta', attrs={'name':'description'})
        if (len(meta_description) > 0):
            df.at[i,'description'] = str( meta_description[0]['content'].strip())
            # print(meta_description[0]['content'].strip())

        # get the product description
        description = soup.find_all('div', id="productDescription")
        if (len(description) > 0):
            df.at[i,'product_description'] = str(description[0].text.strip())
            # print(description[0].text.strip())

        # get the asin
        df.at[i,'asin'] = str(url.split("/dp/")[1].split("/")[0])

        # get the manufacturer
        manufacturer = soup.find('div', id="detailBullets_feature_div").find_all('span', class_="a-list-item")
        if (manufacturer is not None):
            for x in manufacturer:
                if (x.text.strip().startswith("Manufacturer")):
                    df.at[i,'manufacturer'] = x.text.split(":")[1].replace("\n","").replace("  ", "").strip().encode('ascii', 'ignore').decode()
                    #### Note: encoding string to ascii to remove the special characters
                    # print(x.text.split(":")[1].replace("\n","").replace("  ", "").rstrip().encode('ascii', 'ignore').decode())
                    break
            
        # check if the file exists and is empty
        if (not os.path.isfile('amazon_products_listing_with_desc.csv') or os.stat('amazon_products_listing_with_desc.csv').st_size == 0):
            # add only current row to the file
            df.iloc[[i]].to_csv('amazon_products_listing_with_desc.csv', mode='a', header=[
            'url', 'name', 'price', 'rating', 'number_of_reviews', 'description', 'product_description', 'asin', 'manufacturer'
            ], index=False)
        else:
            df.iloc[[i]].to_csv('amazon_products_listing_with_desc.csv', mode='a', header=False, index=False)
        print(f"Product {i+1} scraped successfully\n ")
        
    except Exception as e:
        ## Enhancement: implement logging
        print(e)
        print(f"Some error occurred while scraping product {i+1}\n\n")
        return

# function to generate random delay
# @param upper_bound: upper bound of the delay (default = 30)
def random_delay(upper_bound = 30):
    delay = random.randint(20,upper_bound)
    time.sleep(delay)


if __name__ == "__main__":
    #############################################
    ## Scraping catalogue
    #############################################
    for page in range(1, 21):
        scrape_catalogue(page)
        random_delay()
    print("Catalogue scraping completed")


    #############################################
    ## Scrape product description
    #############################################
    print("Product description scraping started")

    # initialized the dataframe
    df = pd.read_csv('amazon_products_listing.csv')
    df['description'] = ""
    df["product_description"] = ""
    df["asin"] = ""
    df["manufacturer"] = ""
    i = 0
    while (i < len(df)):
        scrape_product_desc(df,i)
        random_delay(upper_bound=25)
        i += 1

    print(f"Finished scraping {i+1} products")


