import requests
import subprocess
from selenium import webdriver
import time
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import NoAlertPresentException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC  
import pandas as pd  
from fake_useragent import UserAgent

while True:
    try:
        x = 0
        while x < 10:
            try:
                response = requests.get('https://arbitr.sms19.ru/api/court_case/getToCardObserve').json()
                break
            except:
                x += 1
                time.sleep(10)
                if x == 10:
                    break

        for item in response:
            id = item['id']
            number = item["number"]

            df_proxies = pd.read_csv('/home/dmitry/Рабочий стол/airflow/dags/proxies.csv')
            proxy = df_proxies.sample(n=1)
            ip = proxy['ip'].values[0]
            port = proxy['port'].values[0]

            ua = UserAgent()
            user_agent = ua.random

            subprocess.Popen([
                '/usr/bin/google-chrome', 
                '--remote-debugging-port=9222', 
                f'--proxy-server=http://{ip}:{port}',
                f'--user-agent={user_agent}'
            ])

            options = webdriver.ChromeOptions()
            options.add_argument("auto-open-devtools-for-tabs")
            options.debugger_address="127.0.0.1:9222"

            driver = webdriver.Chrome(options=options)
            driver.get('https://kad.arbitr.ru/')
            window_before = driver.window_handles[0]
            time.sleep(2)

            try:
                    alert = driver.switch_to.alert
                    alert.dismiss()
                    print("Закрыл окно от браузера")
            except NoAlertPresentException:
                    print("Небыло окна от браузера")

            try:
                    driver.find_element(By.XPATH, "//*[@id='js']/div[13]/div[2]/div/div/div/div/a[1]").click()
                    print("Закрыл окно html")
            except NoSuchElementException:
                    print("Небыло окна html")

            try:
                button = WebDriverWait(driver, timeout=10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="js"]/div[4]/div/a')))
                webdriver.ActionChains(driver).move_to_element(button).click(button).perform()
            except:
                print("Небыло окна про Браузер")

            driver.find_element(By.XPATH, "//*[@id='sug-cases']/div/input").send_keys(number)

            print(number)

            button_1 = driver.find_element(By.ID, "b-form-submit")
            webdriver.ActionChains(driver).move_to_element(button_1).click(button_1).perform()

            try:
                elem = WebDriverWait(driver, timeout=10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a.num_case"))) 
                url = elem.get_attribute('href') 
                card_uuid = url.split('/')[-1]
                print('card_uuid')

                x = 0
                while x < 10:
                    try:
                        post = requests.post(f'https://arbitr.sms19.ru/api/court_case/setToObserve/{id}', json = {'card_uuid': card_uuid})
                        print("__post: ", post)
                        break
                    except:
                        x += 1
                        time.sleep(60)
                        if x == 10:
                            break
            except:
                no_results = WebDriverWait(driver, timeout=10).until(EC.element_to_be_clickable((By.CLASS_NAME, "b-noResults__padding")))
                text = no_results.find_element(By.TAG_NAME, "h2").text
                print(text)

            driver.delete_all_cookies()
            driver.close()

    except:
        time.sleep(10)
        driver.close()
