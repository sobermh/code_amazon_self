from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import datetime
import multiprocessing
import os
import re
import threading
from amazoncaptcha import AmazonCaptcha
from selenium import webdriver
from selenium.webdriver.chrome.service import Service

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import time
import random
from bs4 import BeautifulSoup


def translate_text(text):
    from googletrans import Translator

    translator = Translator()

    translated = translator.translate(text, src='zh-cn', dest='en')

    return translated.text


def save_html(filepath, html):
    with open(filepath, 'w', encoding='utf-8') as file:
        file.write(html)


lock = multiprocessing.Lock()


def process_second_category(i, csv_file, error_url_file):
    pro_driver = WebOp.init_driver()
    third_category = ParseData.scrape_third_category(pro_driver, i)
    for k in third_category:
        min_category = ParseData.scrape_min_category(pro_driver, k, third_category, i["category"])
        threads = []
        products_list = []

        def process_product(product):
            valid_price_value = ConditionOp.check_price(product, 50)
            if valid_price_value is None:
                return None
            pro_info = ParseData.scrape_product_info(valid_price_value["link"])
            valid_price_value.update(pro_info)
            valid_date_value = ConditionOp.check_date(valid_price_value, 720)
            if valid_date_value is None:
                return None
            return valid_date_value

        def check_condition(driver: webdriver.Chrome, products):
            remian_products = []
            for product in products:
                valid_price_value = ConditionOp.check_price(product, 50)
                if valid_price_value is None:
                    continue
                pro_info = ParseData.scrape_product_info(driver, valid_price_value["link"], error_url_file)
                valid_price_value.update(pro_info)
                valid_date_value = ConditionOp.check_date(valid_price_value, 180)
                if valid_date_value is None:
                    continue
                valid_soldby_value = ConditionOp.check_soldby(valid_date_value, "Amazon.ae")
                if valid_soldby_value is None:
                    continue
                remian_products.append(valid_soldby_value)
                # remian_products.append(valid_price_value)
            return remian_products
            # with ThreadPoolExecutor(max_workers=5) as executor:
            #     futures = [executor.submit(process_product, product) for product in products]
            #     for future in as_completed(futures):
            #         result = future.result()
            #         if result is not None:
            #             remian_products.append(result)
            # return remian_products

        for j in min_category:
            def selenium_task(category, url):
                driver = WebOp.init_driver()
                products = ParseData.scrape_products(driver, url)
                valid_products = check_condition(driver, products)
                products_list.append({category: valid_products})
                driver.quit()
            thread = threading.Thread(target=selenium_task, args=(j["category"], j['link'],))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        for c in products_list:
            for key, value in c.items():
                CsvOp.save_to_csv(i["category"], k["category"], key, csv_file, value)

        # break
    pro_driver.quit()


class WebOp:
    @staticmethod
    def read_max_driver():
        with open("maxdriver", "r") as f:
            return int(f.read())

    @staticmethod
    def write_max_driver():
        with open("maxdriver", "w") as f:
            max_driver = WebOp.read_max_driver() + 1
            f.write(str(max_driver))

    @staticmethod
    def check_max_driver():
        while True:
            read_max_driver = WebOp.read_max_driver()
            if read_max_driver < 10:
                break
            else:
                time.sleep(5)

    @staticmethod
    def init_driver():
        # WebOp.check_max_driver()
        options = webdriver.ChromeOptions()
        options.add_argument('--lang=en')
        # prefs = {
        #     "translate_whitelists": {"en": "zh-CN"},  # 将英语翻译为中文
        #     "translate": {"enabled": "true"}
        # }
        # options.add_experimental_option("prefs", prefs)

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
        # proxy = "http://127.0.0.1:8890"  # home
        proxy = "http://127.0.0.1:7890"  # ver
        options.add_argument(f'--proxy-server={proxy}')
        while True:
            try:
                driver = webdriver.Chrome(service=chrome_driver_path, options=options)
                break
            except Exception as e:
                time.sleep(30)

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

    @staticmethod
    def open_url(driver, url):
        driver.get(url)
        time.sleep(1)

    @staticmethod
    def close_driver(driver):
        driver.quit()
        try:
            WebOp.write_max_driver()
        except:
            pass

    @staticmethod
    def load_html(driver: webdriver.Chrome):
        # 定义滚动的暂停时间
        # scroll_pause_time = 1
        flag = 10
        while True:
            # 获取当前滚动位置和页面高度
            scroll_height = driver.execute_script("return document.body.scrollHeight")
            window_height = driver.execute_script("return window.innerHeight")
            scroll_position = driver.execute_script("return window.scrollY")

            # 判断是否已经滚动到页面底部
            if scroll_position + window_height < scroll_height:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(0.5)
            else:
                break
            flag -= 1
            if flag == 0:
                break
        return driver.page_source

    @staticmethod
    def last_page(driver):
        last_page_element = driver.find_element(By.CLASS_NAME, "a-last")
        last_page_link = last_page_element.find_element(By.TAG_NAME, "a")
        last_page_link.click()


class ParseData:

    @staticmethod
    def check_for_captcha(driver: webdriver.Chrome):
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        captcha_form = soup.find('form', {'method': 'get', 'action': '/errors/validateCaptcha'})
        if captcha_form is not None:
            return True
        else:
            return False

    @staticmethod
    def valid_for_captcha(driver: webdriver.Chrome):
        captcha = AmazonCaptcha.fromdriver(driver)
        captcha = AmazonCaptcha.fromlink(captcha.image_link)
        solution = captcha.solve()
        input_element = driver.find_element(By.ID, "captchacharacters")
        input_element.clear()
        input_element.send_keys(solution)

        submit_button = driver.find_element(By.CLASS_NAME, "a-button-text")
        submit_button.click()

    @staticmethod
    def init_web(driver: webdriver.Chrome, url, wait_condition):
        WebOp.open_url(driver, url)
        wait = WebDriverWait(driver, 5)
        start_time = time.time()  # 记录开始时间
        flag = False
        while True:
            if time.time() - start_time > 30:  # 如果已经超过30秒，就跳出循环
                break
            try:
                wait.until(EC.presence_of_element_located((wait_condition)))
                flag = True
                break
            except Exception as e:
                if ParseData.check_for_captcha(driver):
                    ParseData.valid_for_captcha(driver)
                else:
                    print(url)
                    break
        return flag

    @staticmethod
    def scrape_second_category(driver: webdriver.Chrome, url):
        wait_condition = (By.ID, "zg-left-col")
        ParseData.init_web(driver, url,  wait_condition)
        category_list = []
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        category_html = soup.select(
            "._p13n-zg-nav-tree-all_style_zg-browse-item__1rdKf._p13n-zg-nav-tree-all_style_zg-browse-height-large__1z5B8")
        for category in category_html:
            link_tag = category.find('a')
            if link_tag:
                category_name = link_tag.text.strip()
                category_link = link_tag['href']
                base_url = ParseData.parse_region_url(url)
                category_list.append({'category': category_name, 'link': base_url+category_link})
        return category_list

    @staticmethod
    def scrape_third_category(driver: webdriver.Chrome, i):
        wait_condition = (By.ID, "zg-left-col")
        ParseData.init_web(driver, i["link"], wait_condition)
        category_list = []
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        category_html = soup.select(
            "._p13n-zg-nav-tree-all_style_zg-browse-item__1rdKf._p13n-zg-nav-tree-all_style_zg-browse-height-large__1z5B8")
        for category in category_html:
            link_tag = category.find('a')
            if link_tag:
                category_name = link_tag.text.strip()
                category_link = link_tag['href']
                base_url = ParseData.parse_region_url(i["link"])
                category_list.append({'category': category_name, 'link': base_url+category_link})
        third_category = category_list[1:]
        return third_category

    @staticmethod
    def scrape_min_category(driver: webdriver.Chrome, k, third_categorys, second_category):
        wait_condition = (By.ID, "zg-left-col")
        ParseData.init_web(driver, k['link'], wait_condition)
        category_list = []
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        category_html = soup.select(
            "._p13n-zg-nav-tree-all_style_zg-browse-item__1rdKf._p13n-zg-nav-tree-all_style_zg-browse-height-large__1z5B8")
        for category in category_html:
            link_tag = category.find('a')
            if link_tag:
                category_name = link_tag.text.strip()
                category_link = link_tag['href']
                base_url = ParseData.parse_region_url(k['link'])
                category_list.append({'category': category_name, 'link': base_url+category_link})
        category = [item["category"] for item in category_list]
        third_category = [item["category"] for item in third_categorys]
        if set(category[1:]) == set(third_category):
            min_category = [k]
        else:
            if category[1] != second_category:
                min_category = [k]
            else:
                min_category = category_list[2:]
        return min_category

    @staticmethod
    def scrape_products(driver: webdriver.Chrome, url):
        def parse_product(url, soup_html, start_index=0):
            base_url = ParseData.parse_region_url(url)
            product_html = soup_html.select("#gridItemRoot")  # 根据实际的类名调整选择器
            product_list = []
            for index, product in enumerate(product_html):
                try:
                    a = product.find_all('a', class_='a-link-normal')[1].get('href')
                    title = product.find_all('a', class_='a-link-normal')[1].text
                    try:
                        price = product.find_all('a', class_='a-link-normal')[3].text
                    except IndexError:
                        price = ''
                    product_list.append({'rank': start_index+index+1,  'title': title,
                                        'price': price, 'link': base_url+a})
                except Exception as e:
                    continue
            return product_list
        wait_condition = (By.CSS_SELECTOR, "#gridItemRoot")
        load_flag = ParseData.init_web(driver, url, wait_condition)
        if not load_flag:
            return [{}, ]
        page_html = WebOp.load_html(driver)
        soup = BeautifulSoup(page_html, 'html.parser')
        pro1_list = parse_product(url, soup)
        # 可能没有下一页
        try:
            WebOp.last_page(driver)
            page_html = WebOp.load_html(driver)
            pro2_list = parse_product(url, BeautifulSoup(page_html, 'html.parser'), start_index=50)
        except:
            pro2_list = []
        return pro1_list + pro2_list

    @staticmethod
    def scrape_product_info(driver: webdriver.Chrome, url, scrape_product_info):
        product_info = {
            'dimensions': '',
            'date': '',
            'rank': '',
            "soldby": "",
        }
        WebOp.load_html(driver)
        wait_condition1 = (By.ID, "detailBullets_feature_div")
        load_flag1 = ParseData.init_web(driver, url, wait_condition1)
        if load_flag1:
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            info_section = soup.find('div', {'id': 'detailBullets_feature_div'})
            info_section2 = soup.find_all(
                'ul', {'class': 'a-unordered-list a-nostyle a-vertical a-spacing-none detail-bullet-list'})[1]
            for li in info_section.find_all('li'):
                text = li.get_text(strip=True)
                if 'Product Dimensions' in text:
                    product_info['dimensions'] = re.sub(r'[\u200e\u200f]', '', text.split(':')[-1].strip())
                elif 'Date First Available' in text:
                    product_info['date'] = re.sub(r'[\u200e\u200f]', '', text.split(':')[-1].strip())
            for li in info_section2.find_all('li'):
                text = li.get_text(strip=True)
                if 'Best Sellers Rank' in text:
                    ranks = re.findall(r'#(\d+)\s+in\s*([A-Za-z\s]+)',
                                       re.sub(r'[\u200e\u200f]', '', text.split(':')[-1].strip()))
                    rank_dict = {category.strip(): rank for rank, category in ranks}
                    product_info['rank'] = rank_dict
        else:
            wait_condition2 = (By.ID, "prodDetails")
            load_flag2 = ParseData.init_web(driver, url, wait_condition2)
            if load_flag2:
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                info_section = soup.find('div', {'id': 'prodDetails'})
                # 提取产品技术细节
                tech_table = info_section.find('table', {'id': 'productDetails_techSpec_section_1'})
                for row in tech_table.find_all('tr'):
                    key = row.find('th').get_text(strip=True)
                    value = row.find('td').get_text(strip=True)
                    if key == 'Product Dimensions':
                        product_info['dimensions'] = re.sub(r'[\u200e\u200f]', '', value)
                # 提取附加信息
                additional_table = info_section.find('table', {'id': 'productDetails_detailBullets_sections1'})
                for row in additional_table.find_all('tr'):
                    key = row.find('th').get_text(strip=True)
                    value = row.find('td').get_text(strip=True)
                    if key == 'Best Sellers Rank':
                        anks = re.findall(r'#(\d+)\s+in\s*([A-Za-z\s]+)',
                                          re.sub(r'[\u200e\u200f]', '', value))
                        rank_dict = {category.strip(): rank for rank, category in anks}
                        product_info['rank'] = rank_dict
                    elif key == 'Date First Available':
                        product_info['date'] = re.sub(r'[\u200e\u200f]', '', value)
            else:
                CsvOp.write_error_url(error_url_file, url)

        def parse_pro_soldby(html_soup: BeautifulSoup):
            sold_by_div = html_soup.find('div', {'id': 'offerDisplayFeatures_desktop'})
            sold_by_span = sold_by_div.find('span', {'class': 'a-size-small offer-display-feature-text-message'})
            return sold_by_span.text
        try:
            product_info["soldby"] = parse_pro_soldby(soup)
        except Exception:
            product_info["soldby"] = "未获取到卖家信息"
        return product_info

    @staticmethod
    def parse_region_url(url):
        match = re.match(r'(https?://[^/]+)', url)
        base_url = match.group(0) if match else ""
        return base_url


class CsvOp:
    @staticmethod
    def init_csv(filepath: str, rowddata):

        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(rowddata)

    @staticmethod
    def save_to_csv(second_category, third_category, min_category, csv_file, data):
        with lock:
            with open(csv_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                for item in data:
                    row_data = [second_category, third_category, min_category]
                    for key, value in item.items():
                        if key == 'rank':
                            row_data.insert(3, value)
                        elif key == 'title':
                            row_data.insert(4, value)
                        elif key == 'price':
                            row_data.insert(5, value)
                        elif key == 'date':
                            row_data.insert(6, value)
                        elif key == 'link':
                            row_data.insert(7, value)
                        elif key == "soldby":
                            row_data.insert(8, value)
                    writer.writerow(row_data)
            print(f"{second_category}-{third_category}-{min_category} saved to {csv_file} successfully.")

    @staticmethod
    def format_csv(csv_file):
        pass

    @staticmethod
    def write_error_url(error_url_file, url):
        with lock:
            with open(error_url_file, 'a') as f:
                writer = csv.writer(f)
                writer.writerow(url)


class ConditionOp:
    @staticmethod
    def check_price(product: dict, min_price):
        def extract_number(s):
            match = re.search(r'(\d+\.\d+)', s)
            if match:
                return float(match.group(1))
            else:
                return 0
        try:
            if product.get('price') is not None and extract_number(product.get('price')) >= min_price:
                product["price"] = extract_number(product.get('price'))
                return product
            else:
                return None
        except Exception:
            return None

    @staticmethod
    def check_date(product: dict, max_days):
        date_format = '%d %B %Y'
        date_str = product.get('date')
        if not date_str:
            return None

        try:
            date_obj = datetime.datetime.strptime(date_str, date_format).date()
            if datetime.datetime.now().date() - date_obj <= datetime.timedelta(days=max_days):
                product["date"] = f"{date_obj.year}/{date_obj.month}/{date_obj.day}"
                return product
            else:
                return None
        except ValueError:
            return None

    @staticmethod
    def check_soldby(product: dict, soldby):
        if product.get("soldby") and soldby.lower() in product.get("soldby").lower():
            return None
        return product


if __name__ == '__main__':
    start = time.time()
    now_str = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    data_file = f'office_amazon_products_{now_str}.csv'
    error_url_file = f'error_url_{now_str}.csv'
    pro_url = 'https://www.amazon.ae/gp/bestsellers/office-products/ref=zg_bs_nav_office-products_0'
    init_csv_data = ['二级类目', '三级类目', '最小类', '排名', '名称', '价格', '上架日期', '链接', '卖家']
    CsvOp.init_csv(data_file, init_csv_data)
    CsvOp.init_csv(error_url_file, [])
    driver = WebOp.init_driver()
    second_category = ParseData.scrape_second_category(driver, pro_url)
    driver.quit()
    # second = [{'category': 'Art & Craft Supplies', 'link': 'https://www.amazon.ae/gp/bestsellers/office-products/15172571031/ref=zg_bs_nav_office-products_1'}, {'category': 'Calendars, Planners & Personal Organizers', 'link': 'https://www.amazon.ae/gp/bestsellers/office-products/15172572031/ref=zg_bs_nav_office-products_1'}, {'category': 'Envelopes & Mailing Supplies', 'link': 'https://www.amazon.ae/gp/bestsellers/office-products/15172573031/ref=zg_bs_nav_office-products_1'}, {'category': 'Furniture & Lighting', 'link': 'https://www.amazon.ae/gp/bestsellers/office-products/15172574031/ref=zg_bs_nav_office-products_1'}, {'category': 'Office Electronics', 'link': 'https://www.amazon.ae/gp/bestsellers/office-products/15172575031/ref=zg_bs_nav_office-products_1'}, {'category': 'Office Paper Products', 'link': 'https://www.amazon.ae/gp/bestsellers/office-products/15172576031/ref=zg_bs_nav_office-products_1'}, {'category': 'Office Supplies', 'link': 'https://www.amazon.ae/gp/bestsellers/office-products/15172577031/ref=zg_bs_nav_office-products_1'}, {'category': 'Pens, Pencils & Writing Supplies', 'link': 'https://www.amazon.ae/gp/bestsellers/office-products/15172578031/ref=zg_bs_nav_office-products_1'}, {'category': 'School & Educational Supplies', 'link': 'https://www.amazon.ae/gp/bestsellers/office-products/15172579031/ref=zg_bs_nav_office-products_1'}]

    cpu_count = multiprocessing.cpu_count()
    cpu_count = 1
    with multiprocessing.Pool(processes=cpu_count) as pool:
        pool.starmap(process_second_category, [(i, data_file, error_url_file) for i in second_category[:1]])
        # pool.starmap(process_second_category, [(i, csv_file) for i in second_category])
    end = time.time()
    print("Time taken:", end - start, "seconds")

    # url = "https://www.amazon.ae/Montchi-Stickers-Motivational-Holographic-Classroom/dp/B0D4VJP48L/ref=zg_bs_g_15172571031_d_sccl_1/262-9087435-0427238?th=1"
    # print(parse_product_info(url))
