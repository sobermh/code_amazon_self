import time
import random
import csv
import datetime
import locale
import multiprocessing
import os
import queue
import re
import regex

from amazoncaptcha import AmazonCaptcha
from openpyxl import load_workbook
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from functools import partial
from bs4 import BeautifulSoup
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor


from autofit_excel import Autofit
from notice_email import send_email_with_attachment


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
        # 禁止显示chrome的浏览器正在受到自动测试软件控制的通知栏
        options.add_argument("--disable-infobars")
        options.add_argument("start-maximized")
        options.add_argument(
            "--disable-blink-features=AutomationControlled")  # 禁用自动化检测
        options.add_argument("--disable-extensions")  # 禁用扩展程序
        options.add_experimental_option(
            "excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        options.add_argument("--lang=zh-CN")  # 添加此行以设置默认语言为中文
        chrome_driver_path = Service(
            r"chromedriver.exe")  # 替换为你本地 ChromeDriver 的路径

        prefs = {"profile.managed_default_content_settings.images": 2}
        options.add_experimental_option("prefs", prefs)

        # 选择可用的代理服务器
        options.add_argument(f'user-agent={random.choice(user_agents)}')
        # proxy = "http://127.0.0.1:7890"
        # options.add_argument(f'--proxy-server={proxy}')
        while True:
            try:
                driver = webdriver.Chrome(
                    service=chrome_driver_path, options=options)
                break
            except Exception as e:
                print(e)
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
            print(e)
        return driver

    @staticmethod
    def open_url(driver, url):
        driver.get(url)
        # time.sleep(1)

    @staticmethod
    def load_html(driver: webdriver.Chrome):
        try:
            # 定义滚动的暂停时间
            # scroll_pause_time = 1
            max_scroll_times = 10
            scroll_height = 600  # 可以根据实际情况调整

            for i in range(max_scroll_times):
                # 计算下一个滚动的目标位置
                target_position = scroll_height * (i + 1)

                # 滚动到下一个位置
                driver.execute_script(
                    f"window.scrollTo(0, {target_position});")

                # 等待内容加载
                time.sleep(0.5)

                # 检查是否已经滚动到底部
                # scroll_position = driver.execute_script("return window.pageYOffset;")
                # total_height = driver.execute_script("return document.body.scrollHeight;")
                # window_height = driver.execute_script("return window.innerHeight;")
                # if scroll_position + window_height >= total_height:
                #     # 已经滚动到底部，退出循环
                #     break
        except Exception as e:
            print(e)
        return driver.page_source

    @staticmethod
    def last_page(driver):
        last_page_element = driver.find_element(By.CLASS_NAME, "a-last")
        last_page_link = last_page_element.find_element(By.TAG_NAME, "a")
        last_page_link.click()


class ParseData:

    @staticmethod
    def check_for_throttled(driver: webdriver.Chrome):
        try:
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            throttle_message = soup.find(
                'pre', style='word-wrap: break-word; white-space: pre-wrap;')
            if throttle_message and 'Request was throttled' in throttle_message.text:
                return True
            else:
                return False
        except Exception as e:
            print(e)
            return True

    @staticmethod
    def check_for_captcha(driver: webdriver.Chrome):
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        captcha_form = soup.find(
            'form', {'method': 'get', 'action': '/errors/validateCaptcha'})
        if captcha_form is not None:
            return True
        else:
            return False

    @staticmethod
    def valid_for_captcha(driver: webdriver.Chrome):
        try:
            captcha = AmazonCaptcha.fromdriver(driver)
            captcha = AmazonCaptcha.fromlink(captcha.image_link)
            solution = captcha.solve()
            input_element = driver.find_element(By.ID, "captchacharacters")
            input_element.clear()
            input_element.send_keys(solution)

            submit_button = driver.find_element(By.CLASS_NAME, "a-button-text")
            submit_button.click()
        except Exception as e:
            print(e)

    @staticmethod
    def init_web(driver: webdriver.Chrome, url, wait_condition: tuple):
        WebOp.open_url(driver, url)
        wait = WebDriverWait(driver, 5)
        flag = False
        WebOp.load_html(driver)
        while True:
            if ParseData.check_for_throttled(driver):
                time.sleep(2)
                driver.refresh()
            else:
                break

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
                category_list.append(
                    {'category': category_name, 'link': base_url+category_link})
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
                category_list.append(
                    {'category': category_name, 'link': base_url+category_link})
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
                category_list.append(
                    {'category': category_name, 'link': base_url+category_link})
        category = [item["category"] for item in category_list]
        third_category_list = [item["category"] for item in third_categorys]
        if set(category[1:]) == set(third_category_list):
            min_category = [third_category_list]
        else:
            if category[1] != second_category:
                min_category = [third_category_list]
            else:
                min_category = category_list[2:]
        print(
            f"{second_category}-{third_category['category']} : {min_category}")
        return min_category

    @staticmethod
    def scrape_products(driver: webdriver.Chrome, url):
        def parse_product(url, soup_html, start_index=0):
            base_url = ParseData.parse_region_url(url)
            product_html = soup_html.select("#gridItemRoot")
            product_list = []
            for index, product in enumerate(product_html):
                try:
                    a = product.find_all(
                        'a', class_='a-link-normal')[1].get('href')
                    title = product.find_all(
                        'a', class_='a-link-normal')[1].text
                    try:
                        price = product.find_all(
                            'a', class_='a-link-normal')[3].text
                    except IndexError:
                        price = ''
                    product_list.append({'rank': start_index+index+1,  'title': title,
                                        'price': price, 'link': base_url+a})
                except Exception as e:
                    print(f'{e}')
                    continue
            return product_list
        wait_condition = (By.CSS_SELECTOR, "#gridItemRoot")
        ParseData.init_web(driver, url, wait_condition)
        pro_list = []
        try:
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            pro1_list = parse_product(url, soup)
            # 可能没有下一页
            try:
                WebOp.last_page(driver)
                page_html = WebOp.load_html(driver)
                pro2_list = parse_product(url, BeautifulSoup(
                    page_html, 'html.parser'), start_index=50)
            except:
                pro2_list = []
            pro_list = pro1_list + pro2_list
        except Exception as e:
            print(f"解析失败:{e}")
            pro_list = [{}]
        # print("*" * 15)
        # print(f"pro_list : {pro_list}")
        return pro_list

    @staticmethod
    def scrape_product_info(driver: webdriver.Chrome, url):
        product_info = {
            'dimensions': '',
            'date': '',
            'rank': '',
            "soldby": "",
        }
        wait_condition = (By.ID, "ask-btf-container")
        ParseData.init_web(driver, url, wait_condition)
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        def parse_pro_baseinfo1(soup: BeautifulSoup):
            product_info = {}
            try:
                info_section = soup.find('div', {'id': 'prodDetails'})
                # 提取产品技术细节
                tech_table = info_section.find(
                    'table', {'id': 'productDetails_techSpec_section_1'})
                for row in tech_table.find_all('tr'):
                    key = row.find('th').get_text(strip=True)
                    value = row.find('td').get_text(strip=True)
                    if "Dimensões" in key:
                        product_info['dimensions'] = re.sub(
                            r'[\u200e\u200f]', '', value)
                # 提取附加信息
                additional_table = info_section.find(
                    'table', {'id': 'productDetails_detailBullets_sections1'})
                for row in additional_table.find_all('tr'):
                    key = row.find('th').get_text(strip=True)
                    value = row.find('td').get_text(strip=True)
                    if key == 'Ranking dos mais vendidos':
                        clean_value = regex.sub(
                            r'\p{P}', '', value)  # 去掉所有的标点符号
                        match = regex.search(r'([\p{Nd}]+)', clean_value)
                        if match:
                            product_info['rank'] = int(match.group(1))
                    elif key == 'Disponível para compra desde':
                        product_info['date'] = re.sub(
                            r'[\u200e\u200f]', '', value)
            except Exception as e:
                print(f"第一个解析失败:{e}")
            return product_info

        def parse_pro_baseinfo2(soup: BeautifulSoup):
            product_info = {}
            try:
                info_section = soup.find(
                    'div', {'id': 'detailBulletsWrapper_feature_div'})
                info_section2 = soup.find_all(
                    'ul', {'class': 'a-unordered-list a-nostyle a-vertical a-spacing-none detail-bullet-list'})[1]
                for li in info_section.find_all('li'):
                    text = li.get_text(strip=True)
                    if 'Dimensões' in text:
                        product_info['dimensions'] = re.sub(
                            r'[\u200e\u200f]', '', text.split(':')[-1].strip())
                    elif 'Disponível' in text:
                        product_info['date'] = re.sub(
                            r'[\u200e\u200f]', '', text.split(':')[-1].strip())
                for li in info_section2.find_all('li'):
                    text = li.get_text(strip=True)
                    if 'Ranking dos mais vendidos' in text:
                        clean_value = regex.sub(
                            r'\p{P}', '', text.split(':')[-1].strip())
                        match = regex.search(r'([\p{Nd}]+)', clean_value)
                        if match:
                            product_info['rank'] = int(match.group(1))
            except Exception as e:
                pass
                # print(f"第二个解析失败{e}")
            return product_info

        product_info1 = parse_pro_baseinfo1(soup)
        product_info2 = parse_pro_baseinfo2(soup)
        for k, v in product_info.items():
            if product_info2.get(k, "") != "":
                product_info[k] = product_info2[k]
            else:
                try:
                    product_info[k] = product_info1[k]
                except:
                    pass

        def parse_pro_soldby(html_soup: BeautifulSoup):
            sold_by_div = html_soup.find(
                'div', {'id': 'merchantInfoFeature_feature_div'})
            sold_by_span = sold_by_div.find(
                'span', {'class': 'a-size-small offer-display-feature-text-message'})
            return sold_by_span.text
        try:
            product_info["soldby"] = parse_pro_soldby(soup)
        except Exception:
            product_info["soldby"] = "未获取到卖家信息"

        def parse_pro_price(html_soup: BeautifulSoup):
            sold_by_div = html_soup.find(
                'div', {'id': 'corePriceDisplay_desktop_feature_div'})
            sold_by_span = sold_by_div.find('span', {'class': 'a-price-whole'})
            price_text = sold_by_span.text
            price_numbers = re.sub(r'\D', '', price_text)  # 只保留数字，移除非数字字符
            return price_numbers
        try:
            product_info["price"] = parse_pro_price(soup)
        except Exception:
            pass

        def parse_pro_title(html_soup: BeautifulSoup):
            title_div = html_soup.find('div', {'id': 'centerCol'})
            title_span = title_div.find('span', {'id': 'productTitle'})
            return title_span.text.strip()
        try:
            product_info["title"] = parse_pro_title(soup)
        except Exception:
            pass
        print(product_info)
        return product_info

    @staticmethod
    def parse_region_url(url):
        match = re.match(r'(https?://[^/]+)', url)
        base_url = match.group(0) if match else ""
        return base_url


lock = multiprocessing.Lock()


class CsvOp:
    @staticmethod
    def init_csv(filepath: str, rowddata):
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(rowddata)

    @staticmethod
    def write_success_proinfo(second_category, third_category, min_category, pro_file, data: list):
        with lock:
            with open(pro_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                for item in data:
                    row_data = [second_category, third_category, min_category]
                    keys = ['rank', 'title', 'price', 'date', 'link', 'soldby']
                    for key in keys:
                        row_data.append(item.get(key, ''))
                    writer.writerow(row_data)
            print(
                f"{second_category}-{third_category}-{min_category} saved to {pro_file} successfully.")

    @staticmethod
    def format_csv(file: str):
        CsvOp.sort_success_proinfo(file)
        xlsx_file = CsvOp.csv_to_xlsx(file)
        wb = load_workbook(filename=xlsx_file)
        ws = wb.active
        Autofit(ws).autofit()
        wb.save(xlsx_file)
        return xlsx_file

    @staticmethod
    def sort_success_proinfo(file: str, colname="排名"):
        import pandas as pd
        try:
            df = pd.read_csv(file)
            if f"{colname}" in df.columns:
                df = df.sort_values(by=f'{colname}')
            df = df.reset_index(drop=True)

            df.to_csv(file, index=False)
        except Exception as e:
            print(e)

    @staticmethod
    def write_error_proinfo(error_pro_file, url, second_category, third_category, min_category):
        with lock:
            with open(error_pro_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(
                    [second_category, third_category, min_category, url])

    @staticmethod
    def remove_repeat_proinfo(file: str):
        import pandas as pd
        try:
            # 读取csv文件
            df = pd.read_csv(file)

            # 删除URL列重复的行
            if '链接' in df.columns:
                df = df.drop_duplicates(subset='链接')

            # 如果'名称'列存在，则删除'名称'列重复的行
            if '名称' in df.columns:
                df = df.drop_duplicates(subset='名称')

            df = df.reset_index(drop=True)
            # 将结果写入新的csv文件
            df.to_csv(file, index=False)
        except Exception as e:
            print(e)

    @staticmethod
    def init_proinfo_csv(max_category: str):
        now_str = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        filename = f'ae_{max_category}_{now_str}'
        pro_file = f'{filename}.csv'
        error_pro_file = f'{filename}_error.csv'
        init_pro_file = ['二级类目', '三级类目', '最小类',
                         '排名', '名称', '价格', '上架日期', '链接', '卖家']
        init_error_pro_file = ['二级类目', '三级类目', '最小类', '链接']
        CsvOp.init_csv(pro_file, init_pro_file)
        CsvOp.init_csv(error_pro_file, init_error_pro_file)
        return pro_file, error_pro_file

    @staticmethod
    def load_error_proinfo(error_pro_file):
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

    @staticmethod
    def translate_text(text):
        from googletrans import Translator

        translator = Translator()
        print(text)
        try:
            translated = translator.translate(text, src='en', dest='zh-cn')
        except AttributeError:
            print("Translation failed for text: ", text)

        return translated.text

    @staticmethod
    def csv_to_xlsx(csvpath):
        import pandas as pd
        df = pd.read_csv(csvpath)
        output_xlsx_path = csvpath.replace('.csv', '.xlsx')
        df.to_excel(output_xlsx_path, index=False)
        return output_xlsx_path


class ConditionOp:
    @staticmethod
    def check_price(product: dict, min_price=100):
        def extract_number(s):
            match = re.search(r'(\d+)', s)
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
            original_locale = locale.setlocale(locale.LC_TIME)
            locale.setlocale(locale.LC_TIME, 'pt_PT.UTF-8')
            date_obj = datetime.datetime.strptime(date_str, date_format).date()
            locale.setlocale(locale.LC_TIME, original_locale)
            if datetime.datetime.now().date() - date_obj <= datetime.timedelta(days=max_days):
                product["date"] = f"{date_obj.year}/{date_obj.month}/{date_obj.day}"
                return product
            else:
                return None
        except ValueError:
            return product

    @staticmethod
    def check_soldby(product: dict, soldby):
        if product.get("soldby") is None:
            return None
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

    @staticmethod
    def check_all(product: dict, max_days=180, soldby="Amazon", rank=10000):

        date_res = ConditionOp.check_date(product, max_days)
        if date_res is None:
            return None
        soldby_res = ConditionOp.check_soldby(date_res, soldby)
        if soldby_res is None:
            return None
        rank_res = ConditionOp.check_rank(soldby_res, rank)
        if rank_res is None:
            return None
        return rank_res


def process_parse_second_category(second_category, pro_file, error_pro_file):
    driver = WebOp.init_driver()
    third_categorys = ParseData.scrape_third_category(
        driver, second_category["link"])
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
                valid_price_value = ConditionOp.check_price(product)
                if valid_price_value is None:
                    continue
                pro_info = ParseData.scrape_product_info(
                    driver, valid_price_value["link"])
                if pro_info["date"] == "" and pro_info["rank"] == "":
                    valid_soldby_value = ConditionOp.check_soldby(
                        pro_info, "Amazon")
                    if valid_soldby_value is None:
                        continue
                    else:
                        CsvOp.write_error_proinfo(
                            error_pro_file, valid_price_value["link"], second_category, third_category, min_category)
                        continue
                else:
                    valid_price_value.update(pro_info)
                    pro = ConditionOp.check_all(valid_price_value)
                    if pro is None:
                        continue
                    remian_products.append(pro)
            return remian_products

        def selenium_task(driver, category, url, second_category, third_category, min_category):
            products = ParseData.scrape_products(driver, url)
            valid_products = check_condition(
                driver, products, second_category, third_category, min_category)
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
                CsvOp.write_success_proinfo(second_category["category"],
                                            third_category["category"], key, pro_file, value)


def process_retry_error_proinfo(data, pro_file, error_pro_file):
    def process_single_item(driver, item, pro_file, error_pro_file):
        second_category, third_category, min_category, link = item
        pro_info = ParseData.scrape_product_info(driver, link)
        pro_info["link"] = link
        if pro_info["date"] == "" and pro_info["rank"] == "":
            valid_soldby_value = ConditionOp.check_soldby(pro_info, "Amazon")
            if valid_soldby_value is None:
                return
            else:
                CsvOp.write_error_proinfo(
                    error_pro_file, link, second_category, third_category, min_category)
                return
        else:
            pro = ConditionOp.check_all(pro_info)
            if pro is None:
                return
            CsvOp.write_success_proinfo(
                second_category, third_category, min_category, pro_file, [pro_info])
    pool = WebDriverPool(10)
    futures = []
    for item in data:
        try:
            future = pool.submit(process_single_item, item,
                                 pro_file, error_pro_file)
            futures.append(future)
        except Exception as e:
            with lock:
                with open("error", "a+", encoding="utf-8") as f:
                    f.write(f"{item}\n")
                    f.write(f"{e}\n")
    for future in futures:
        future.result()  # 等待任务完成

    pool.shutdown()


def retry_error_data(error_pro_file, pro_file):
    error_data = CsvOp.load_error_proinfo(error_pro_file)
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
                process_executor.submit(partial(process_retry_error_proinfo, chunk,
                                        pro_file, error_pro_file))


def main():
    max_category = input("请输入最大类目名称:")
    pro_url = input("请输入类目链接:")
    start = time.time()
    pro_file, error_pro_file = CsvOp.init_proinfo_csv(max_category)
    driver = WebOp.init_driver()
    second_categorys = ParseData.scrape_second_category(driver, pro_url)
    driver.quit()
    # second = [{'category': 'Art & Craft Supplies', 'link': 'https://www.amazon.ae/gp/bestsellers/office-products/15172571031/ref=zg_bs_nav_office-products_1'}, {'category': 'Calendars, Planners & Personal Organizers', 'link': 'https://www.amazon.ae/gp/bestsellers/office-products/15172572031/ref=zg_bs_nav_office-products_1'}, {'category': 'Envelopes & Mailing Supplies', 'link': 'https://www.amazon.ae/gp/bestsellers/office-products/15172573031/ref=zg_bs_nav_office-products_1'}, {'category': 'Furniture & Lighting', 'link': 'https://www.amazon.ae/gp/bestsellers/office-products/15172574031/ref=zg_bs_nav_office-products_1'}, {'category': 'Office Electronics', 'link': 'https://www.amazon.ae/gp/bestsellers/office-products/15172575031/ref=zg_bs_nav_office-products_1'}, {'category': 'Office Paper Products', 'link': 'https://www.amazon.ae/gp/bestsellers/office-products/15172576031/ref=zg_bs_nav_office-products_1'}, {'category': 'Office Supplies', 'link': 'https://www.amazon.ae/gp/bestsellers/office-products/15172577031/ref=zg_bs_nav_office-products_1'}, {'category': 'Pens, Pencils & Writing Supplies', 'link': 'https://www.amazon.ae/gp/bestsellers/office-products/15172578031/ref=zg_bs_nav_office-products_1'}, {'category': 'School & Educational Supplies', 'link': 'https://www.amazon.ae/gp/bestsellers/office-products/15172579031/ref=zg_bs_nav_office-products_1'}]

    cpu_count = multiprocessing.cpu_count()
    cpu_count = 3
    with multiprocessing.Pool(processes=cpu_count) as pool:
        try:
            # pool.starmap(process_parse_second_category, [(second_category, pro_file,
            #                                               error_pro_file)for second_category in second_categorys[1:]])
            pool.starmap(process_parse_second_category, [(second_category, pro_file,
                         error_pro_file)for second_category in second_categorys])
        except KeyboardInterrupt:
            print("Interrupted by user. Terminating processes...")
            pool.terminate()
            pool.join()
            os._exit(1)  # 强行退出程序
        except Exception as e:
            print(e)

    mid = time.time()
    print("mid-Time taken:", mid - start, "seconds")
    for i in range(3):
        CsvOp.remove_repeat_proinfo(error_pro_file)
        CsvOp.remove_repeat_proinfo(pro_file)
        retry_error_data(error_pro_file, pro_file)
    xlsx_file = CsvOp.format_csv(pro_file)
    # 发送邮件

    def send_email(xlsx_file):
        attach_file = []
        if os.path.isabs(xlsx_file) == False:
            path = os.path.join(os.getcwd(), xlsx_file)
            attach_file.append(path)
        else:
            attach_file.append(xlsx_file)
        now_str = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        send_email_with_attachment(
            subject=f"BR_Amazon_{now_str}",
            body="最新的亚马逊筛选数据,This email has an attachment.",
            to_email="837671287@qq.com",
            from_email="409788696@qq.com",
            smtp_server="smtp.qq.com",
            smtp_port=587,
            login="409788696@qq.com",
            password="wkevznzegbjmbhbc",
            file_paths=attach_file
        )

    send_email(xlsx_file)
    end = time.time()
    print("end-Time taken:", end - start, "seconds")


if __name__ == '__main__':
    multiprocessing.freeze_support()
    try:
        main()
    except KeyboardInterrupt:
        print('\nKeyboard interrupt detected. Exiting...')
        os._exit(1)

    # driver = WebOp.init_driver()
    # pro_info = ParseData.scrape_product_info(
    #     driver, "https://www.amazon.com.br/Cadeira-Escrit%C3%B3rio-Secret%C3%A1ria-Cromada-Rodinha/dp/B0BWSJY84N/ref=zg_bs_g_furniture_d_sccl_2/142-0053349-4269762?psc=1")
    # driver.quit()

    # pro_file = "ae_Móveis_2024-10-24_14-31-36.csv"
    # CsvOp.remove_repeat_proinfo(pro_file)
    # CsvOp.format_csv(pro_file)

    # driver = WebOp.init_driver()
    # second_category = ParseData.scrape_second_category(
    #     driver, "https://www.amazon.com.br/gp/bestsellers/hi/ref=zg_bs_nav_hi_0")
    # print(second_category)

    # third_category = ParseData.scrape_third_category(
    #     driver, "https://www.amazon.com.br/gp/bestsellers/hi/17113550011/ref=zg_bs_nav_hi_1")
    # print(third_category)

    # third_categorys = [{'category': 'Armários', 'link': 'https://www.amazon.com.br/gp/bestsellers/hi/17113639011/ref=zg_bs_nav_hi_2_17113550011'}, {'category': 'Estantes Utilitárias', 'link': 'https://www.amazon.com.br/gp/bestsellers/hi/17113640011/ref=zg_bs_nav_hi_2_17113550011'}, {'category': 'Organizadores de Ferramentas',
    #                                                                                                                                                                                                                                                                                         'link': 'https://www.amazon.com.br/gp/bestsellers/hi/17113621011/ref=zg_bs_nav_hi_2_17113550011'}, {'category': 'Organização de Garagem', 'link': 'https://www.amazon.com.br/gp/bestsellers/hi/17113638011/ref=zg_bs_nav_hi_2_17113550011'}, {'category': 'Porta Escada', 'link': 'https://www.amazon.com.br/gp/bestsellers/hi/48724031011/ref=zg_bs_nav_hi_2_17113550011'}]
    # min_category = ParseData.scrape_min_category(
    #     driver, {'category': 'Armários', 'link': 'https://www.amazon.com.br/gp/bestsellers/hi/17113639011/ref=zg_bs_nav_hi_2_17113550011'}, third_categorys)
    # print(min_category)

    # driver.quit()
