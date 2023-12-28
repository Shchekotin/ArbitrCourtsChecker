import time
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import NoAlertPresentException
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC  
import mysql.connector      
import pandas as pd      
from fake_useragent import UserAgent     

mydb = mysql.connector.connect(
  host="localhost",
  user="mufer",
  password="vRZVgh6c",
  database="Data_Science"
)

while True:
    try:
        x = 0
        while x < 10:
            try:
                response = requests.get('https://arbitr.sms19.ru/api/manual_check?token=6c0ab81e19a4b80f159d626fd8a11218').json()
                break
            except:
                x += 1
                time.sleep(10)
                if x == 10:
                    break

        for item in response:
            id = item['id']
            number = item["number"]
            inn = item["inn"]
            contact_id = item["contact_id"]
            last_name = item["contact"]["last_name"]
            name = item["contact"]["name"]
            second_name = item["contact"]["second_name"]

            df_proxies = pd.read_csv('/home/dmitry/Рабочий стол/airflow/dags/proxies.csv')
            proxy = df_proxies.sample(n=1)
            ip = proxy['ip'].values[0]
            port = proxy['port'].values[0]

            while True:
                ua = UserAgent()
                user_agent = ua.random

                if "Mozilla/5.0" in user_agent and "rv:" not in user_agent and 'iPad' not in user_agent and 'compatible' not in user_agent:
                    break

            user_agent_chrome = user_agent.split()
            user_agent_chrome[-2] = "Chrome/114.0.0.0"
            user_agent = " ".join(user_agent_chrome)

            options = webdriver.ChromeOptions()
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument('--disable-gpu')
            options.add_argument(f"user-agent={user_agent}")
            options.add_argument(f'--proxy-server={ip}:{port}')

            driver = webdriver.Chrome(options=options)
            driver.get('https://kad.arbitr.ru/')

            try:
                driver.find_element(By.XPATH, "//*[@id='js']/div[4]/div/a").click()
                print("Закрыл окно html")
            except:
                print("Небыло окна html")

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
  
            if inn:
                element_one = WebDriverWait(driver, timeout=10).until(
                    EC.element_to_be_clickable((By.XPATH, '//*[@id="sug-participants"]/div/textarea')))
                element_one.send_keys(inn) 
                time.sleep(2)

                button = WebDriverWait(driver, timeout=10).until(EC.presence_of_element_located((By.ID, "b-form-submit")))
                webdriver.ActionChains(driver).move_to_element(button).click(button).perform()

                try:
                    elem = WebDriverWait(driver, timeout=10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a.num_case")))
                    num_case = elem.get_attribute('innerHTML').strip()
                    url = elem.get_attribute('href') 
                    card_uuid = url.split('/')[-1]

                    if number == num_case:
                        status = 2
                    else:
                        status = 1

                    x = 0
                    while x < 10:
                        try:
                            post = requests.post(
                                f'https://arbitr.sms19.ru/api/manual_check/{id}?token=6c0ab81e19a4b80f159d626fd8a11218', 
                                json = {
                                    'status': status, 
                                    'correct_number': num_case,
                                    'card_uuid': card_uuid,
                                    'kad_name': None
                                }
                            )
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

                    if text == "Нет результатов":
                        element_one = WebDriverWait(driver, timeout=10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="sug-participants"]/div/textarea')))
                        element_one.clear() 
                        time.sleep(1)

                        element_one = WebDriverWait(driver, timeout=10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="sug-cases"]/div/input')))
                        element_one.send_keys(number)
                        time.sleep(2)

                        button = WebDriverWait(driver, timeout=10).until(EC.presence_of_element_located((By.ID, "b-form-submit")))
                        webdriver.ActionChains(driver).move_to_element(button).click(button).perform()

                        elem = WebDriverWait(driver, timeout=10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a.num_case")))
                        url = elem.get_attribute('href')
                        card_uuid = url.split('/')[-1]

                        try:
                            name_element = WebDriverWait(driver, timeout=10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="b-cases"]/tbody/tr/td[4]/div/div/span')))
                            kad_name = name_element.text.strip()

                            names = last_name + " " + name + " " + second_name

                            if names == kad_name:
                                status = 2
                            else:
                                status = 1
                        except:
                            status = 1
                            kad_name = None

                        x = 0
                        while x < 10:
                            try:
                                post = requests.post(
                                    f'https://arbitr.sms19.ru/api/manual_check/{id}?token=6c0ab81e19a4b80f159d626fd8a11218', 
                                    json = {
                                        'status': status, 
                                        'correct_number': None,
                                        'card_uuid': card_uuid,
                                        'kad_name': kad_name
                                    }
                                )
                                print("__post: ", post)
                                break
                            except:
                                x += 1
                                time.sleep(60)
                                if x == 10:
                                    break
            else:
                element_one = WebDriverWait(driver, timeout=10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="sug-cases"]/div/input')))
                element_one.send_keys(number)
                time.sleep(2)

                button = WebDriverWait(driver, timeout=10).until(EC.presence_of_element_located((By.ID, "b-form-submit")))
                webdriver.ActionChains(driver).move_to_element(button).click(button).perform()

                elem = WebDriverWait(driver, timeout=10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a.num_case")))
                url = elem.get_attribute('href')
                card_uuid = url.split('/')[-1]

                try:
                    name_element = WebDriverWait(driver, timeout=10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="b-cases"]/tbody/tr/td[4]/div/div/span')))
                    kad_name = name_element.text.strip()

                    names = last_name + " " + name + " " + second_name

                    if names == kad_name:
                        status = 2
                    else:
                        status = 1
                except:
                    status = 1
                    kad_name = None

                x = 0
                while x < 10:
                    try:
                        post = requests.post(
                            f'https://arbitr.sms19.ru/api/manual_check/{id}?token=6c0ab81e19a4b80f159d626fd8a11218', 
                            json = {
                                'status': status, 
                                'correct_number': None,
                                'card_uuid': card_uuid,
                                'kad_name': kad_name
                            }
                        )
                        print("__post: ", post)
                        break
                    except:
                        x += 1
                        time.sleep(60)
                        if x == 10:
                            break

            driver.delete_all_cookies()
            driver.close()

    except:
        time.sleep(10)
        driver.close()
