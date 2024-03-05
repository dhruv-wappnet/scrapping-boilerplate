from playwright.sync_api import sync_playwright
import bs4
from database_functions import *

def fetch_and_load(conn, url):

    # initialize playwright object as a bot
    with sync_playwright() as bot:
        browser = bot.chromium.launch(
            headless=False,
            args=[
                "--blink-settings=imagesEnabled=false",
                '--disable-gpu',
                '--disable-extensions',
                '--no-sandbox',
                '--disable-dev-shm-usage'
            ]
        ) # Launching a Chromium browser instance

        # Creating a new browsing context
        context = browser.new_context()

        # Opening a new page in the browsing context
        page = context.new_page()

        # Navigating to a specific URL
        page.goto(url)

        # navigate within page and select search rules
        navigate_page(page)

        # get all profile links for page
        set_of_links = get_links(page)

        # insert links in the database
        bulk_insert_links(conn, set_of_links)

        # start extracting pages via links from database
        links = select_all_links(conn, 'table_name')

        # traversing through links
        for url in links:

            # extract page by page using url
            data = extract_page(url)
            if data:
                id = insert_data(conn, data, 'profile_url')
            else:
                print(f"No data found at {url}")
        
        # Closing the browsing context
        context.close()
    
        # Closing the browsing instance
        browser.close()
