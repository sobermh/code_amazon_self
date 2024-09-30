import csv
import multiprocessing
import os
import re
import threading
from selenium import webdriver
from selenium.webdriver.chrome.service import Service

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import time
import random
from bs4 import BeautifulSoup


def init_driver():
    # 设置Chrome选项
    options = webdriver.ChromeOptions()

    options.add_argument('--lang=en')
    prefs = {
        "translate_whitelists": {"en": "zh-CN"},  # 将英语翻译为中文
        "translate": {"enabled": "true"}
    }
    options.add_experimental_option("prefs", prefs)

    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",

    ]

    # options.add_argument("--headless")  # 无头模式
    options.add_argument("--disable-infobars")  # 禁止显示chrome的浏览器正在受到自动测试软件控制的通知栏
    options.add_argument("start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")  # 禁用自动化检测
    options.add_argument("--disable-extensions")  # 禁用扩展程序
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    options.add_argument("--lang=zh-CN")  # 添加此行以设置默认语言为中文
    chrome_driver_path = Service(r"chromedriver.exe")  # 替换为你本地 ChromeDriver 的路径

    # 选择可用的代理服务器
    options.add_argument(f'user-agent={random.choice(user_agents)}')
    proxy = "http://127.0.0.1:8890" # home
    # proxy = "https://127.0.0.1:8890" # ver
    options.add_argument(f'--proxy-server={proxy}')
    driver = webdriver.Chrome(service=chrome_driver_path, options=options)


    # 禁用webdriver特征，以防止被检测
    try:
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                })
            """
        })
    except:
        pass
    return driver


def save_html(filepath, html):
    with open(filepath, 'w', encoding='utf-8') as file:
        file.write(html)


def load_html(driver):
    # 定义滚动的暂停时间
    # scroll_pause_time = 1
    while True:
        # 获取当前滚动位置和页面高度
        scroll_height = driver.execute_script("return document.body.scrollHeight")
        window_height = driver.execute_script("return window.innerHeight")
        scroll_position = driver.execute_script("return window.scrollY")

        # 判断是否已经滚动到页面底部
        if scroll_position + window_height < scroll_height:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            wait = WebDriverWait(driver, 10)
            wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "#gridItemRoot")))
            # time.sleep(scroll_pause_time)  # 等待加载
        else:
            break
    return driver.page_source


def parse_product(url, soup_html, start_index=0):
    base_url = parse_region_url(url)
    product_html = soup_html.select("#gridItemRoot")  # 根据实际的类名调整选择器
    product_list = []
    for index, product in enumerate(product_html):
        product_info = {}
        a = product.find_all('a', class_='a-link-normal')[1].get('href')
        title = product.find_all('a', class_='a-link-normal')[1].text
        try:
            price = product.find_all('a', class_='a-link-normal')[3].text
        except IndexError:
            price = ''
        product_list.append({'rank': start_index+index+1,  'title': title, 'price': price, 'link': base_url+a})

    return product_list


def parse_product_info(url, soup_html, wait):
    wait.until(EC.presence_of_element_located((By.ID, "dp")))


def last_page(driver):
    last_page_element = driver.find_element(By.CLASS_NAME, "a-last")
    last_page_link = last_page_element.find_element(By.TAG_NAME, "a")
    last_page_link.click()


def scrape_amazon_products(url):
    driver = init_driver()
    open_url(driver, url)
    wait = WebDriverWait(driver, 10)
    try:
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#gridItemRoot")))
    except:
        return [{},]
    page_html = load_html(driver)
    soup = BeautifulSoup(page_html, 'html.parser')
    pro1_info = parse_product(url, soup)
    # 可能没有下一页
    try:
        last_page(driver)
        page_html = load_html(driver)
        pro2_info = parse_product(url, BeautifulSoup(page_html, 'html.parser'), start_index=50)
    except:
        pro2_info = []
    return pro1_info + pro2_info


# 创建锁对象
lock = threading.Lock()


def save_to_csv(second_category, third_category, min_category, data_file, data):
    with lock:
        with open(data_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            for item in data:
                row_data = [second_category, third_category, min_category]
                for key, value in item.items():
                    row_data.append(value)
                writer.writerow(row_data)
        print(f"{second_category}-{third_category}-{min_category} saved to {data_file} successfully.")


def format_csv(data_file):
    pass


def parse_second_category(url):
    driver = init_driver()
    open_url(driver, url)
    wait = WebDriverWait(driver, 10)
    wait.until(EC.presence_of_element_located((By.ID, "zg-left-col")))
    category_list = []
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    category_html = soup.select(
        "._p13n-zg-nav-tree-all_style_zg-browse-item__1rdKf._p13n-zg-nav-tree-all_style_zg-browse-height-large__1z5B8")
    for category in category_html:
        link_tag = category.find('a')
        if link_tag:
            category_name = link_tag.text.strip()
            category_link = link_tag['href']
            base_url = parse_region_url(url)
            category_list.append({'category': category_name, 'link': base_url+category_link})
    driver.quit()

    return category_list


def parse_third_category(url):
    driver = init_driver()
    open_url(driver, url)
    wait = WebDriverWait(driver, 10)
    wait.until(EC.presence_of_element_located((By.ID, "zg-left-col")))
    category_list = []
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    category_html = soup.select(
        "._p13n-zg-nav-tree-all_style_zg-browse-item__1rdKf._p13n-zg-nav-tree-all_style_zg-browse-height-large__1z5B8")
    for category in category_html:
        link_tag = category.find('a')
        if link_tag:
            category_name = link_tag.text.strip()
            category_link = link_tag['href']
            base_url = parse_region_url(url)
            category_list.append({'category': category_name, 'link': base_url+category_link})
    del category_list[0]
    driver.quit()
    return category_list


def parse_min_category(url):
    driver = init_driver()
    open_url(driver, url)
    wait = WebDriverWait(driver, 10)
    wait.until(EC.presence_of_element_located((By.ID, "zg-left-col")))
    category_list = []
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    category_html = soup.select(
        "._p13n-zg-nav-tree-all_style_zg-browse-item__1rdKf._p13n-zg-nav-tree-all_style_zg-browse-height-large__1z5B8")
    for category in category_html:
        link_tag = category.find('a')
        if link_tag:
            category_name = link_tag.text.strip()
            category_link = link_tag['href']
            base_url = parse_region_url(url)
            category_list.append({'category': category_name, 'link': base_url+category_link})
    del category_list[:2]
    driver.quit()
    return category_list


def init_csv(filename):
    # if os.path.exists(filename):
    #     print(f"{filename} already exists.")
    #     return
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['二级类目', '三级类目', '最小类', '排名', '名称', '价格', '上架日期', '链接'])
    print(f"{filename} created successfully.")


def open_url(driver, url):
    driver.get(url)
    time.sleep(1)


def parse_region_url(url):
    match = re.match(r'(https?://[^/]+)', url)
    base_url = match.group(0) if match else ""
    return base_url


def process_second_category(i, data_file):
    third_category = parse_third_category(i['link'])
    for k in third_category:
        min_category = parse_min_category(k['link'])
        threads = []
        products_list = []

        # 多线程处理 min_category
        for j in min_category:
            def selenium_task(category, url):
                products = scrape_amazon_products(url)
                products_list.append({category: products})

            thread = threading.Thread(target=selenium_task, args=(j["category"], j['link'],))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        for c in products_list:
            for key, value in c.items():
                save_to_csv(i["category"], k["category"], key, data_file, value)


if __name__ == '__main__':
    start = time.time()
    data_file = 'office_amazon_products.csv'
    init_csv(data_file)
    url = 'https://www.amazon.ae/gp/bestsellers/office-products/ref=zg_bs_unv_office-products_1_15193872031_2'
    second_category = parse_second_category(url)
    # 获取 CPU 核心数
    cpu_count = multiprocessing.cpu_count()  # 获取CPU核心数量
    with multiprocessing.Pool(processes=cpu_count) as pool:
        pool.starmap(process_second_category, [(i, data_file) for i in second_category])

    # for i in second_category:
    #     third_category = parse_third_category(i['link'])
    #     for k in third_category:
    #         min_category = parse_min_category(k['link'])
    #         threads = []
    #         products_list = []
    #         for j in min_category:
    #             def selenium_task(category, url):
    #                 products = scrape_amazon_products(url)
    #                 products_list.append({category: products})
    #             thread = threading.Thread(target=selenium_task, args=(j["category"], j['link'],))
    #             threads.append(thread)
    #             thread.start()
    #         for thread in threads:
    #             thread.join()
    #         for c in products_list:
    #             for key, value in c.items():
    #                 save_to_csv(i["category"], k["category"], key, data_file, value)
    end = time.time()
    print("Time taken:", end - start, "seconds")
