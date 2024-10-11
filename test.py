import time
from amazoncaptcha import AmazonCaptcha
from selenium import webdriver
from selenium.webdriver.common.by import By


for i in range(10):
    driver = webdriver.Chrome()  # This is a simplified example
    driver.get('https://www.amazon.ae/Itsy-Bitsy-Little-Birdie-Sculpture/dp/B082L3B9FM/ref=zg_bs_g_15194022031_d_sccl_28/261-3367906-9911827?psc=1')

    driver.quit()
