from batch_scraper.scrape_repos import ScrapeCMC


def lambda_handler(event, context):
    scraper = ScrapeCMC()
    description = scraper.scrape_project_description("ethereum")
    print(description)
    return description
