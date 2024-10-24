# import time
# from amazoncaptcha import AmazonCaptcha
# from selenium import webdriver
# from selenium.webdriver.common.by import By


# for i in range(10):
#     driver = webdriver.Chrome()  # This is a simplified example
#     driver.get('https://www.amazon.ae/Itsy-Bitsy-Little-Birdie-Sculpture/dp/B082L3B9FM/ref=zg_bs_g_15194022031_d_sccl_28/261-3367906-9911827?psc=1')

#     driver.quit()


import datetime
import locale


now_str = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
print(now_str)

def check_date(product: dict, max_days):
    date_format = '%d %B %Y'
    date_str = product.get('date')
    try:
        locale.setlocale(locale.LC_TIME, 'pt_PT.UTF-8')
        date_obj = datetime.datetime.strptime(date_str, date_format).date()
        if datetime.datetime.now().date() - date_obj <= datetime.timedelta(days=max_days):
            product["date"] = f"{date_obj.year}/{date_obj.month}/{date_obj.day}"
            return product
        else:
            return None
    except ValueError:
        return product

print(check_date({'date': '4 maio 2023'}, 1000))