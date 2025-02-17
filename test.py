import time

from playwright.sync_api import sync_playwright
def get_html_content(url):
    with sync_playwright() as p:
        browser = p.firefox.launch(headless=True)  # 使用 Firefox（或 p.chromium / p.webkit）
        page = browser.new_page()
        response = page.goto(url)
        html_content = page.content()
        timestamp = int(time.time())
        filename = f"amazon_page_{timestamp}.html"
        with open(filename, "w", encoding="utf-8") as f:
            f.write(html_content)
        print(f"✅ 页面已保存至：{filename}")
        status_code = response.status if response else None
        browser.close()
        return html_content, status_code


if __name__ == '__main__':
    # url = "https://www.amazon.com/s?k=boxing+gym+equipment&crid=1SIJO3Q0IGNU9&sprefix=%2Caps%2C808&ref=nb_sb_ss_recent_5_0_recent"
    url = "https://www.amazon.com/s?k=boxing+gym+equipment&page=2&xpid=72nvLEBW1cgJE&crid=1SIJO3Q0IGNU9&qid=1739533978&sprefix=%2Caps%2C808&ref=sr_pg_2"
    html, status = get_html_content(url)
    print(f"Status Code: {status}")
    # print(html)
