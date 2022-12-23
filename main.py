import asyncio
import aiohttp
import aiosqlite
import multiprocessing
from bs4 import BeautifulSoup

N_PROCESSES = multiprocessing.cpu_count()

N_THREADS = 100

url_queue = asyncio.Queue()

semaphore = asyncio.Semaphore(N_THREADS)

SEED_URLS = [
    "https://www.bbc.com/news",
    "https://www.nytimes.com",
    "https://www.foxnews.com",
    "https://www.washingtonpost.com",
    "https://www.npr.org",
    "https://www.wsj.com",
    "https://www.reuters.com",
    "https://www.theguardian.com/us",
    "https://www.usatoday.com",
    "https://www.latimes.com",
    "https://www.abcnews.go.com",
    "https://www.aljazeera.com",
    "https://www.bloomberg.com",
    "https://www.cnbc.com",
    "https://www.foxbusiness.com",
    "https://www.huffpost.com",
    "https://www.msnbc.com",
    "https://www.nbcnews.com",
    "https://www.politico.com",
    "https://www.theatlantic.com",
    "https://www.thehill.com",
    "https://www.usnews.com",
    "https://www.vox.com",
    "https://www.washingtonexaminer.com",
    "https://www.washingtonmonthly.com",
    "https://www.washingtontimes.com",
    "https://www.vice.com",
    "https://www.axios.com",
    "https://www.businessinsider.com",
    "https://www.cbsnews.com",
    "https://www.chicagotribune.com",
    "https://www.cnn.com",
    "https://www.dailykos.com",
    "https://www.economist.com",
    "https://www.euronews.com",
    "https://www.forbes.com",
    "https://www.freep.com",
    "https://www.huffingtonpost.com",
    "https://www.independent.co.uk",
    "https://www.japantimes.co.jp",
    "https://www.jpost.com",
    "https://www.ladbible.com",
    "https://www.nationalreview.com",
    "https://www.nbcnews.com",
    "https://www.newsweek.com",
    "https://www.nydailynews.com",
    "https://www.nytimes.com",
    "https://www.politico.com",
    "https://www.reuters.com",
    "https://aljazeera.com",
    "https://www.bbc.com/news/world/middle_east",
    "https://www.cnn.com/middle-east",
    "https://www.foxnews.com/category/world/middle-east",
    "https://www.haaretz.com/middle-east-news",
    "https://www.huffpost.com/section/middle-east",
    "https://www.independent.co.uk/topic/middle-east",
    "https://www.jpost.com/middle-east",
    "https://www.msnbc.com/middle-east",
    "https://www.nbcnews.com/middle-east",
    "https://www.nytimes.com/section/world/middleeast",
    "https://www.reuters.com/places/middle-east",
    "https://www.theguardian.com/world/middleeast",
    "https://www.usatoday.com/topic/7e1c7c7a-6b1c-4b1f-8c1c-7c1c7c7a6b1c/middle-east",
    "https://www.washingtonpost.com/world/middle_east",
    "https://www.washingtonexaminer.com/tag/middle-east",
    "https://www.washingtonmonthly.com/tag/middle-east",
    "https://www.washingtontimes.com/topics/middle-east",
    "https://www.vice.com/en_us/topic/middle-east",
    "https://www.axios.com/middle-east",
    "https://www.businessinsider.com/middle-east",
    "https://www.cbsnews.com/news/middle-east",
    "https://www.chicagotribune.com/topic/middle-east",
    "https://www.cnn.com/middle-east",
    "https://www.dailykos.com/tag/middle-east",
    "https://www.economist.com/middle-east-and-africa",
    "https://www.euronews.com/tag/middle-east",
    "https://www.forbes.com/middle-east",
]


async def scrape_page(session, url):
    try:
        # Make an HTTP GET request to the URL
        await asyncio.sleep(1)
        async with session.get(url) as response:
            # Extract the page's HTML content
            try:
                html = await response.text()
            except UnicodeDecodeError:
                return None, None

            # Parse the HTML using Beautiful Soup
            soup = BeautifulSoup(html, "html.parser")
            # Extract the page's title and links
            title = soup.title.string if soup.title else None
            links = [a.get("href", "") for a in soup.find_all("a")]
            # Return the page's title and links
            return title, links
    except asyncio.TimeoutError:
        # Retry the request if a timeout occurred
        return None, None
    except aiohttp.client_exceptions.ServerDisconnectedError:
        # Retry the request if the server disconnected
        await asyncio.sleep(1)
        return await scrape_page(session, url)
    except aiohttp.client_exceptions.ClientConnectorError:
        # Handle connection errors
        return None, None
    except aiohttp.client_exceptions.InvalidURL:
        # Skip invalid URLs
        return None, None
    except Exception as e:
        print(e)
        return None, None


# Function to process URLs from the queue and add new URLs to the queue
async def process_queue(session):
    while not url_queue.empty():
        # Get the next URL from the queue
        url = await url_queue.get()
        # Scrape the webpage at the URL
        await semaphore.acquire()

        try:
            title, links = await scrape_page(session, url)

            if links is None or len(links) == 0:
                continue
            # Add the scraped links to the queue
            for link in links:
                if link.startswith("https"):
                    print(f"Adding link to queue: {link}")
                    url_queue.put_nowait(link)
            # Store the scraped data in the database
            async with aiosqlite.connect("ScrapedData.db") as conn:
                c = await conn.cursor()
                await c.execute(
                    "INSERT INTO pages (url, title) VALUES (?, ?)", (url, title)
                )
                await conn.commit()

        finally:
            # Release the permit
            semaphore.release()

# Function to start a scraping process
async def start_scraping_process(session, loop):
    # Create a task to process URLs from the queue
    task = loop.create_task(process_queue(session))
    # Wait for the task to complete
    await task


async def create_database():
    # Create a connection to the database
    conn = await aiosqlite.connect("ScrapedData.db")

    # Create a cursor
    c = await conn.cursor()

    # Execute an SQL statement to create the database
    await c.execute(
        "CREATE TABLE IF NOT EXISTS pages (id INTEGER PRIMARY KEY, url TEXT NULL, title TEXT NULL)"
    )

    await conn.commit()
    await conn.close()


async def main():
    await create_database()
    # Create an aiohttp session
    async with aiohttp.ClientSession() as session:
        # Start the specified number of scraping processes
        loop = asyncio.get_running_loop()

        tasks = [start_scraping_process(session, loop) for _ in range(N_PROCESSES)]
        # Add the seed URL to the queue

        for url in SEED_URLS:
            print(f"Adding link to queue: {url}")
            url_queue.put_nowait(url)

        # Wait for the processes to complete
        if tasks and len(tasks) > 0:
            await asyncio.gather(*tasks)


if __name__ == "__main__":
    # Run the main function
    asyncio.run(main())
