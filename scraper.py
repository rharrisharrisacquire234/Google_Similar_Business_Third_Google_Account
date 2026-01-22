import asyncio
import random
import os
import json
from dotenv import load_dotenv
from playwright.async_api import async_playwright
import gspread
from google.oauth2.service_account import Credentials

# -------------------------------------------------
# LOAD ENV
# -------------------------------------------------
load_dotenv()

GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "Sheet1")

# -------------------------------------------------
# CONFIG
# -------------------------------------------------
BUSINESS_QUERIES = [
    "Biodiversity Net Gain Survey Service in",
    "Biodiversity Consulting Service in",
    "Ecological Survey Service in",
    "Biodiversity Implementation Service in",
    "BNG Assessment Service in",
]

MAX_SCROLLS = 50

CITIES = [
    "Bath", "Birmingham", "Bradford", "Brighton and Hove", "Bristol",
    "Cambridge", "Canterbury", "Carlisle", "Chelmsford", "Chester",
    "Chichester", "Colchester", "Coventry", "Derby", "Doncaster",
    "Durham", "Ely", "Exeter", "Gloucester", "Hereford",
    "Kingston upon Hull", "Lancaster", "Leeds", "Leicester", "Lichfield",
    "Lincoln", "Liverpool", "London", "Manchester", "Milton Keynes",
    "Newcastle upon Tyne", "Norwich", "Nottingham", "Oxford",
    "Peterborough", "Plymouth", "Portsmouth", "Preston", "Ripon",
    "Salford", "Sheffield", "Southampton", "Southend-on-Sea",
    "St Albans", "Stoke-on-Trent", "Sunderland", "Truro", "Wakefield",
    "Wells", "Westminster", "Winchester", "Wolverhampton", "Worcester",
    "York"
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0 Safari/537.36"
)

# -------------------------------------------------
# GOOGLE SHEETS INIT (LOCAL + GITHUB ACTIONS)
# -------------------------------------------------
def init_google_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]

    creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")

    if creds_json:
        # GitHub Actions / Secrets
        creds_dict = json.loads(creds_json)
        credentials = Credentials.from_service_account_info(
            creds_dict,
            scopes=scopes
        )
    else:
        # Local development
        credentials = Credentials.from_service_account_file(
            "credentials.json",
            scopes=scopes
        )

    client = gspread.authorize(credentials)
    sheet = client.open_by_key(GOOGLE_SHEET_ID).worksheet(GOOGLE_SHEET_NAME)

    # Ensure header exists
    headers = sheet.row_values(1)
    if headers != ["business_name", "website"]:
        sheet.clear()
        sheet.append_row(["business_name", "website"])

    return sheet

# -------------------------------------------------
# FETCH EXISTING WEBSITES (DEDUP)
# -------------------------------------------------
def get_existing_websites(sheet):
    rows = sheet.get_all_values()
    return set(
        row[1].strip().lower()
        for row in rows[1:]
        if len(row) > 1 and row[1]
    )

# -------------------------------------------------
# SCRAPE
# -------------------------------------------------
async def scrape_city(page, city, business_query):
    query = f"{business_query} {city}"
    print(f"\n🔍 {business_query} | 🌆 {city}", flush=True)

    await page.goto(
        f"https://www.google.com/maps/search/{query.replace(' ', '+')}",
        timeout=60000
    )

    try:
        await page.wait_for_selector('div[role="feed"]', timeout=60000)
    except:
        return []

    results_panel = page.locator('div[role="feed"]')
    previous_count = 0

    for _ in range(MAX_SCROLLS):
        await results_panel.evaluate(
            "(panel) => panel.scrollBy(0, panel.scrollHeight)"
        )
        await page.wait_for_timeout(random.randint(1500, 2500))

        cards = page.locator('div[role="article"]')
        current_count = await cards.count()
        if current_count == previous_count:
            break
        previous_count = current_count

    results = []
    cards = page.locator('div[role="article"]')

    for i in range(await cards.count()):
        card = cards.nth(i)

        try:
            name = ""
            website = ""

            if await card.locator('a[data-value="Website"]').count():
                website = await card.locator(
                    'a[data-value="Website"]'
                ).get_attribute("href")

            if not website:
                continue

            if await card.locator('div.fontHeadlineSmall').count():
                name = (
                    await card.locator('div.fontHeadlineSmall')
                    .inner_text()
                ).strip()

            if name and website:
                results.append({
                    "business_name": name,
                    "website": website.strip()
                })

        except:
            continue

    print(f"✅ Found {len(results)} businesses", flush=True)
    return results

# -------------------------------------------------
# MAIN
# -------------------------------------------------
async def main():
    sheet = init_google_sheet()
    existing_websites = get_existing_websites(sheet)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"]
        )

        context = await browser.new_context(
            user_agent=USER_AGENT,
            locale="en-GB",
            timezone_id="Europe/London"
        )

        page = await context.new_page()

        for business_query in BUSINESS_QUERIES:
            print(f"\n🚀 STARTING: {business_query}", flush=True)

            for city in CITIES:
                try:
                    results = await scrape_city(page, city, business_query)

                    new_rows = []
                    for r in results:
                        website_key = r["website"].lower()
                        if website_key not in existing_websites:
                            new_rows.append(
                                [r["business_name"], r["website"]]
                            )
                            existing_websites.add(website_key)

                    if new_rows:
                        sheet.append_rows(new_rows, value_input_option="RAW")
                        print(f"💾 Saved {len(new_rows)} new rows", flush=True)

                    await page.wait_for_timeout(
                        random.randint(5, 8) * 1000
                    )

                except Exception as e:
                    print(f"🚫 Error in {city}: {e}", flush=True)
                    continue

        await context.close()
        await browser.close()

    print("\n🎉 SCRAPING COMPLETED — DATA SAVED TO GOOGLE SHEETS")

# -------------------------------------------------
# ENTRY
# -------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
