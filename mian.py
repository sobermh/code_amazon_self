from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import csv
import datetime
from functools import partial
import multiprocessing
import os
import queue
import re
import threading
from amazoncaptcha import AmazonCaptcha
from selenium import webdriver
from selenium.webdriver.chrome.service import Service

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException

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


class WebDriverPool:
    def __init__(self, size):
        self.size = size
        self.driver_queue = queue.Queue()
        for _ in range(size):
            self.driver_queue.put(WebOp.init_driver())
        self.executor = ThreadPoolExecutor(max_workers=size)

    def submit(self, func, *args, **kwargs):
        driver = self.driver_queue.get()
        future = self.executor.submit(func, driver, *args, **kwargs)
        # 当任务完成后，归还 WebDriver 到队列
        future.add_done_callback(lambda _: self.driver_queue.put(driver))
        return future

    def shutdown(self):
        while not self.driver_queue.empty():
            driver = self.driver_queue.get_nowait()
            driver.quit()
        self.executor.shutdown()


def process_second_category(second_category, pro_file, error_pro_file,max_category):
    driver = WebOp.init_driver()
    third_categorys = ParseData.scrape_third_category(driver, second_category["link"])
    driver.quit()

    for third_category in third_categorys:
        driver = WebOp.init_driver()
        min_categorys = ParseData.scrape_min_category(
            driver, third_category, third_categorys, second_category["category"])
        driver.quit()

        pool = WebDriverPool(10)  # 创建一个WebDriverPool实例
        products_list = []

        def check_condition(driver: webdriver.Chrome, products, second_category, third_category, min_category):
            remian_products = []
            for product in products:
                valid_price_value = ConditionOp.check_price(product, 50)
                if valid_price_value is None:
                    continue
                pro_info = ParseData.scrape_product_info(
                    driver, valid_price_value["link"], error_pro_file, second_category, third_category, min_category,max_category)
                valid_price_value.update(pro_info)
                valid_date_value = ConditionOp.check_date(valid_price_value, 180)
                if valid_date_value is None:
                    continue
                valid_soldby_value = ConditionOp.check_soldby(valid_date_value, "Amazon")
                if valid_soldby_value is None:
                    continue
                valid_rank_value = ConditionOp.check_rank(valid_soldby_value, 10000)
                if valid_rank_value is None:
                    continue
                remian_products.append(valid_rank_value)
            return remian_products

        def selenium_task(driver, category, url, second_category, third_category, min_category):
            products = ParseData.scrape_products(driver, url)
            valid_products = check_condition(driver, products, second_category, third_category, min_category)
            products_list.append({category: valid_products})

        futures = []
        for min_category in min_categorys:
            # print(min_category)
            try:
                future = pool.submit(
                    selenium_task, min_category["category"], min_category['link'], second_category["category"], third_category["category"], min_category["category"])
                futures.append(future)
            except Exception as e:
                with lock:
                    with open("error", "a+", encoding="utf-8") as f:
                        f.write(str(min_category) + "\n")
                        f.write(f"{e}\n")

        try:
            for future in futures:
                future.result()  # 等待任务完成
        except Exception as e:
            print(e)

        pool.shutdown()  # 关闭所有的WebDriver实例

        for c in products_list:
            for key, value in c.items():
                CsvOp.save_to_csv(second_category["category"], third_category["category"], key, pro_file, value)


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
        options.add_argument('--ignore-certificate-errors-spki-list')
        options.add_argument('--ignore-ssl-errors')
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

        prefs = {"profile.managed_default_content_settings.images": 2}
        options.add_experimental_option("prefs", prefs)

        # 选择可用的代理服务器
        options.add_argument(f'user-agent={random.choice(user_agents)}')
        # proxy = "http://127.0.0.1:7890"
        # options.add_argument(f'--proxy-server={proxy}')
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
        # time.sleep(1)

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
        max_scroll_times = 10
        scroll_height = 600  # 可以根据实际情况调整

        for i in range(max_scroll_times):
            # 计算下一个滚动的目标位置
            target_position = scroll_height * (i + 1)

            # 滚动到下一个位置
            driver.execute_script(f"window.scrollTo(0, {target_position});")

            # 等待内容加载
            time.sleep(0.5)  # 可以根据实际情况调整等待时间

            # 检查是否已经滚动到底部
            # scroll_position = driver.execute_script("return window.pageYOffset;")
            # total_height = driver.execute_script("return document.body.scrollHeight;")
            # window_height = driver.execute_script("return window.innerHeight;")
            # if scroll_position + window_height >= total_height:
            #     # 已经滚动到底部，退出循环
            #     break
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
    def init_web(driver: webdriver.Chrome, url, wait_condition: tuple):
        WebOp.open_url(driver, url)
        wait = WebDriverWait(driver, 5)
        start_time = time.time()  # 记录开始时间
        flag = False
        # while True:
        #     WebOp.load_html(driver)
        #     if time.time() - start_time > 60:  # 如果已经超过60秒，就跳出循环
        #         print("********************")
        #         print(url)
        #         break
        #     try:
        #         wait.until(EC.presence_of_element_located(wait_condition))
        #         # element = driver.find_element(*wait_condition)
        #         flag = True
        #         break
        #     except Exception as e:
        #         if ParseData.check_for_captcha(driver):
        #             ParseData.valid_for_captcha(driver)
        #         # else:
        #         #     print("---------------------")
        #         #     print("Timeout waiting for element to be present.")

        WebOp.load_html(driver)
        try:
            # wait.until(EC.presence_of_element_located(wait_condition))
            wait.until(EC.visibility_of_element_located(wait_condition))
        except Exception as e:
            if ParseData.check_for_captcha(driver):
                ParseData.valid_for_captcha(driver)
                WebOp.load_html(driver)
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
    def scrape_third_category(driver: webdriver.Chrome, second_category_url):
        wait_condition = (By.ID, "zg-left-col")
        ParseData.init_web(driver, second_category_url, wait_condition)
        category_list = []
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        category_html = soup.select(
            "._p13n-zg-nav-tree-all_style_zg-browse-item__1rdKf._p13n-zg-nav-tree-all_style_zg-browse-height-large__1z5B8")
        for category in category_html:
            link_tag = category.find('a')
            if link_tag:
                category_name = link_tag.text.strip()
                category_link = link_tag['href']
                base_url = ParseData.parse_region_url(second_category_url)
                category_list.append({'category': category_name, 'link': base_url+category_link})
        third_category = category_list[1:]
        return third_category

    @staticmethod
    def scrape_min_category(driver: webdriver.Chrome, third_category, third_categorys, second_category):
        wait_condition = (By.ID, "zg-left-col")
        ParseData.init_web(driver, third_category['link'], wait_condition)
        category_list = []
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        category_html = soup.select(
            "._p13n-zg-nav-tree-all_style_zg-browse-item__1rdKf._p13n-zg-nav-tree-all_style_zg-browse-height-large__1z5B8")
        for category in category_html:
            link_tag = category.find('a')
            if link_tag:
                category_name = link_tag.text.strip()
                category_link = link_tag['href']
                base_url = ParseData.parse_region_url(third_category['link'])
                category_list.append({'category': category_name, 'link': base_url+category_link})
        category = [item["category"] for item in category_list]
        third_category_list = [item["category"] for item in third_categorys]
        if set(category[1:]) == set(third_category_list):
            min_category = [third_category_list]
        else:
            if category[1] != second_category:
                min_category = [third_category_list]
            else:
                min_category = category_list[2:]
        return min_category

    @staticmethod
    def scrape_products(driver: webdriver.Chrome, url):
        def parse_product(url, soup_html, start_index=0):
            base_url = ParseData.parse_region_url(url)
            product_html = soup_html.select("#gridItemRoot")
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
        ParseData.init_web(driver, url, wait_condition)
        try:
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            pro1_list = parse_product(url, soup)
            # 可能没有下一页
            try:
                WebOp.last_page(driver)
                page_html = WebOp.load_html(driver)
                pro2_list = parse_product(url, BeautifulSoup(page_html, 'html.parser'), start_index=50)
            except:
                pro2_list = []
            return pro1_list + pro2_list
        except Exception as e:
            return [{}]

    @staticmethod
    def scrape_product_info(driver: webdriver.Chrome, url, error_pro_file, second_category, third_category, min_category, max_category):
        product_info = {
            'dimensions': '',
            'date': '',
            'rank': '',
            "soldby": "",
        }
        flag = 0
        for i in range(1):
            flag += 1
            wait_condition = (By.ID, "ask-btf-container")
            ParseData.init_web(driver, url, wait_condition)
            try:
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
                        # 移除 Unicode 控制字符
                        clean_value = re.sub(r'[\u200e\u200f]', '', value)
                        # 正则表达式匹配前的数字
                        match = re.search(fr'#([\d,]+)\s*in\s*{max_category}', clean_value)
                        # 如果匹配成功，提取数字，否则返回0
                        if match:
                            # 将逗号去掉后转换为整数
                            try:
                                number = int(match.group(1).replace(',', ''))
                            except:
                                number = -1
                        else:
                            number = clean_value if clean_value else 0
                        product_info['rank'] = number
                    elif key == 'Date First Available':
                        product_info['date'] = re.sub(r'[\u200e\u200f]', '', value)
            except Exception as e:
                try:
                    soup = BeautifulSoup(driver.page_source, 'html.parser')
                    info_section = soup.find('div', {'id': 'detailBulletsWrapper_feature_div'})
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
                            product_info['rank'] = rank_dict.get(max_category, 0)
                except Exception as e:
                    pass

            def parse_pro_soldby(html_soup: BeautifulSoup):
                sold_by_div = html_soup.find('div', {'id': 'offerDisplayFeatures_desktop'})
                sold_by_span = sold_by_div.find('span', {'class': 'a-size-small offer-display-feature-text-message'})
                return sold_by_span.text
            try:
                product_info["soldby"] = parse_pro_soldby(soup)
            except Exception:
                product_info["soldby"] = "未获取到卖家信息"

            def parse_pro_price(html_soup: BeautifulSoup):
                sold_by_div = html_soup.find('div', {'id': 'corePriceDisplay_desktop_feature_div'})
                sold_by_span = sold_by_div.find('span', {'class': 'a-price-whole'})
                price_text = sold_by_span.text
                price_numbers = re.sub(r'\D', '', price_text)  # 只保留数字，移除非数字字符
                return price_numbers
            try:
                product_info["price"] = parse_pro_price(soup)
            except Exception:
                pass

            def parse_pro_title(html_soup: BeautifulSoup):
                title_div = html_soup.find('div', {'id': 'titleSection'})
                title_span = title_div.find('span', {'id': 'productTitle'})
                return title_span.text.strip()
            try:
                product_info["title"] = parse_pro_title(soup)
            except Exception:
                pass

            if product_info["date"] == "" and product_info["rank"] == "":
                if flag == 1:
                    valid_soldby_value = ConditionOp.check_soldby(product_info, "Amazon")
                    if valid_soldby_value is None:
                        break
                    else:
                        CsvOp.write_error_url(error_pro_file, url, second_category, third_category, min_category)
                        break
            else:
                break
        print(product_info)
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
    def save_to_csv(second_category, third_category, min_category, pro_file, data: list):
        with lock:
            with open(pro_file, 'a', newline='', encoding='utf-8') as f:
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
            print(f"{second_category}-{third_category}-{min_category} saved to {pro_file} successfully.")

    @staticmethod
    def format_csv(pro_file):
        pass

    @staticmethod
    def write_error_url(error_pro_file, url, second_category, third_category, min_category):
        with lock:
            with open(error_pro_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([second_category, third_category, min_category, url])

    @staticmethod
    def remove_repeat_url(file):
        import pandas as pd

        # 读取csv文件
        df = pd.read_csv(file)

        # 删除URL列重复的行
        df = df.drop_duplicates(subset='链接')

        # 将结果写入新的csv文件
        df.to_csv(file, index=False)


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
        try:
            date_obj = datetime.datetime.strptime(date_str, date_format).date()
            if datetime.datetime.now().date() - date_obj <= datetime.timedelta(days=max_days):
                product["date"] = f"{date_obj.year}/{date_obj.month}/{date_obj.day}"
                return product
            else:
                return None
        except ValueError:
            return product

    @staticmethod
    def check_soldby(product: dict, soldby):
        if product.get("soldby") and (soldby.lower() in product.get("soldby").lower()):
            return None
        return product

    @staticmethod
    def check_rank(product: dict, rank):
        try:
            if product.get("rank"):
                if int(product.get("rank")) <= 0:
                    pass
                elif int(product.get("rank")) > rank:
                    return None
            return product
        except Exception:
            return product


def init_file():
    now_str = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    filename = f'ae_office_products_{now_str}'
    pro_file = f'{filename}.csv'
    error_pro_file = f'{filename}_error.csv'
    init_pro_file = ['二级类目', '三级类目', '最小类', '排名', '名称', '价格', '上架日期', '链接', '卖家']
    init_error_pro_file = ['二级类目', '三级类目', '最小类', '链接']
    CsvOp.init_csv(pro_file, init_pro_file)
    CsvOp.init_csv(error_pro_file, init_error_pro_file)
    return pro_file, error_pro_file


def load_error_data(error_pro_file):
    import csv
    error_list = []
    with open(error_pro_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)  # 创建 CSV 读取器
        # 跳过标题行
        headers = next(reader)
        print("标题行:", headers)  # 可选：打印标题行
        for row in reader:
            error_list.append(row)
    return error_list


def thread_task(data, pro_file, error_pro_file,max_category):
    # 单条数据的处理逻辑

    def process_single_item(driver, item, pro_file, error_pro_file):
        second_category, third_category, min_category, link = item
        pro_info = ParseData.scrape_product_info(driver, link, error_pro_file, second_category, third_category,
                                                 min_category,max_category)
        pro_info["link"] = link
        valid_date_value = ConditionOp.check_date(pro_info, 180)
        if valid_date_value is None:
            return
        valid_soldby_value = ConditionOp.check_soldby(valid_date_value, "Amazon")
        if valid_soldby_value is None:
            return
        valid_rank_value = ConditionOp.check_rank(valid_soldby_value, 10000)
        if valid_rank_value is None:
            return
        CsvOp.save_to_csv(second_category, third_category, min_category, pro_file, [valid_rank_value])
    pool = WebDriverPool(5)
    futures = []
    for item in data:
        try:
            future = pool.submit(process_single_item, item, pro_file, error_pro_file)
            futures.append(future)
        except Exception as e:
            with lock:
                with open("error", "a+", encoding="utf-8") as f:
                    f.write(f"{item}\n")
                    f.write(f"{e}\n")
    for future in futures:
        future.result()  # 等待任务完成

    pool.shutdown()


def retry_error_data(error_pro_file, pro_file,max_category):
    error_data = load_error_data(error_pro_file)
    # 清空文件内容，以便重新写入数据，除了标题行
    init_error_pro_file = ['二级类目', '三级类目', '最小类', '链接']
    CsvOp.init_csv(error_pro_file, init_error_pro_file)
    # print("error_data:", error_data)

    if len(error_data) < 3:
        # 如果数据少于三个，全放在第一个块
        chunks = [error_data, [], []]
    else:
        # 正常分成三块
        chunk_size = len(error_data) // 3
        remainder = len(error_data) % 3

        chunks = [
            error_data[:chunk_size],  # 第一块
            error_data[chunk_size:2 * chunk_size],  # 第二块
            error_data[2 * chunk_size:]  # 第三块
        ]
        # 如果有余数，把多的元素加到最后一个块
        if remainder:
            chunks[-1].extend(error_data[-remainder:])
        
    # 使用 ProcessPoolExecutor 创建多个进程
    with ProcessPoolExecutor(max_workers=3) as process_executor:
        for chunk in chunks:
            if chunk:  # 确保非空
                process_executor.submit(partial(thread_task, chunk, pro_file, error_pro_file, max_category))
        
        
if __name__ == '__main__':
    start = time.time()
    max_category = "Office Products"
    pro_file, error_pro_file = init_file()
    pro_url = 'https://www.amazon.ae/gp/bestsellers/pet-products/ref=zg_bs_nav_pet-products_0'
    driver = WebOp.init_driver()
    second_categorys = ParseData.scrape_second_category(driver, pro_url)
    driver.quit()
    # second = [{'category': 'Art & Craft Supplies', 'link': 'https://www.amazon.ae/gp/bestsellers/office-products/15172571031/ref=zg_bs_nav_office-products_1'}, {'category': 'Calendars, Planners & Personal Organizers', 'link': 'https://www.amazon.ae/gp/bestsellers/office-products/15172572031/ref=zg_bs_nav_office-products_1'}, {'category': 'Envelopes & Mailing Supplies', 'link': 'https://www.amazon.ae/gp/bestsellers/office-products/15172573031/ref=zg_bs_nav_office-products_1'}, {'category': 'Furniture & Lighting', 'link': 'https://www.amazon.ae/gp/bestsellers/office-products/15172574031/ref=zg_bs_nav_office-products_1'}, {'category': 'Office Electronics', 'link': 'https://www.amazon.ae/gp/bestsellers/office-products/15172575031/ref=zg_bs_nav_office-products_1'}, {'category': 'Office Paper Products', 'link': 'https://www.amazon.ae/gp/bestsellers/office-products/15172576031/ref=zg_bs_nav_office-products_1'}, {'category': 'Office Supplies', 'link': 'https://www.amazon.ae/gp/bestsellers/office-products/15172577031/ref=zg_bs_nav_office-products_1'}, {'category': 'Pens, Pencils & Writing Supplies', 'link': 'https://www.amazon.ae/gp/bestsellers/office-products/15172578031/ref=zg_bs_nav_office-products_1'}, {'category': 'School & Educational Supplies', 'link': 'https://www.amazon.ae/gp/bestsellers/office-products/15172579031/ref=zg_bs_nav_office-products_1'}]

    cpu_count = multiprocessing.cpu_count()
    cpu_count = 3
    with multiprocessing.Pool(processes=cpu_count) as pool:
        try:
            # pool.starmap(process_second_category, [(second_category, )for second_category in second_categorys[2:]])
            pool.starmap(process_second_category, [(second_category, pro_file,
                         error_pro_file,max_category)for second_category in second_categorys])
        except Exception as e:
            print(e)

    # error_pro_file = "ae_office_products_2024-10-18_10-40-51_error.csv"
    # pro_file = "ae_office_products_2024-10-18_10-40-51.csv"
    mid = time.time()
    for i in range(3):
        CsvOp.remove_repeat_url(error_pro_file)
        CsvOp.remove_repeat_url(pro_file)
        retry_error_data(error_pro_file, pro_file,max_category)

    end = time.time()
    print("mid-Time taken:", mid - start, "seconds")
    print("end-Time taken:", end - start, "seconds")

    # driver = WebOp.init_driver()
    # ParseData.scrape_product_info(
    #     driver, "https://www.amazon.ae/Loctite-1360694-Plastic-Adhesive-Multicolor/dp/B001F7E9VI/ref=zg_bs_g_15194024031_d_sccl_15/261-6336418-8804356?th=1", "", "", "", "")
    # driver.quit()
