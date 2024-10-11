import time
from amazoncaptcha import AmazonCaptcha
from selenium import webdriver
from selenium.webdriver.common.by import By

driver = webdriver.Chrome()  # This is a simplified example
driver.get('https://www.amazon.com/errors/validateCaptcha')

captcha = AmazonCaptcha.fromdriver(driver)
captcha = AmazonCaptcha.fromlink(captcha.image_link)
solution = captcha.solve()
print(solution)
input_element = driver.find_element(By.ID, "captchacharacters")
input_element.clear()
input_element.send_keys(solution)

submit_button = driver.find_element(By.CLASS_NAME, "a-button-text")
submit_button.click()

time.sleep(5)
