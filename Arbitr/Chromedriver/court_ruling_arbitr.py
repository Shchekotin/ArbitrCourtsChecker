import re
from bs4 import BeautifulSoup
import requests
from selenium import webdriver
import time
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import NoAlertPresentException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC  
import pandas as pd  
from fake_useragent import UserAgent 

print('start')
while True:
        x = 0
        while x < 10:
                try:
                        response = requests.get('https://arbitr.sms19.ru/api/card').json()
                        break
                except:
                        x += 1
                        print('error_response \n')
                        print(response)
                       
                        time.sleep(30)
                        if x == 10:
                                break

        for item in response:
                try:
                        print('item', item)

                        id = item['id']
                        uuid = item['uuid']

                        df_proxies = pd.read_csv('/home/dmitry/Рабочий стол/airflow/dags/proxies.csv')
                        proxy = df_proxies.sample(n=1)
                        ip = proxy['ip'].values[0]
                        port = proxy['port'].values[0]

                        ua = UserAgent()
                        user_agent = ua.random

                        options = webdriver.ChromeOptions()
                        options.add_argument("--disable-blink-features=AutomationControlled")
                        options.add_argument('--disable-gpu')
                        options.add_argument('--start-maximized')
                        options.add_argument(f"user-agent={user_agent}")
                        options.add_argument(f'--proxy-server={ip}:{port}')

                        driver = webdriver.Chrome(options=options)
                        driver.get(f'https://kad.arbitr.ru/Card/{uuid}')

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

                        try:
                                list_content = WebDriverWait(driver, timeout=10).until(EC.element_to_be_clickable((By.XPATH, "//*[@id='chrono_list_content']/div[1]/div/div[2]")))
                                webdriver.ActionChains(driver).move_to_element(list_content).click(list_content).perform()
                                print('Нажал_1')
                                try:
                                        if WebDriverWait(driver, timeout=30).until(EC.element_to_be_clickable((By.XPATH, "//*[@id='chrono_list_content']/div[2]"))):
                                                print('Отработал_1')
                                except:
                                        print('Не отработал_1')
                                        list_content = WebDriverWait(driver, timeout=10).until(EC.element_to_be_clickable((By.XPATH, "//*[@id='chrono_list_content']/div/div/div[2]/i")))
                                        webdriver.ActionChains(driver).move_to_element(list_content).click(list_content).perform()
                                        print('Нажал_2')
                                        try:
                                                if WebDriverWait(driver, timeout=30).until(EC.element_to_be_clickable((By.XPATH, "//*[@id='chrono_list_content']/div[2]"))):
                                                        print('Отработал_2')
                                        except:
                                                print('Не отработал_2')
                                                list_content = WebDriverWait(driver, timeout=10).until(EC.element_to_be_clickable((By.XPATH, "//*[@id='chrono_list_content']/div[1]/div/div[2]/i")))
                                                webdriver.ActionChains(driver).move_to_element(list_content).click(list_content).perform()
                                                print('Нажал_3')
                                                try:
                                                        if WebDriverWait(driver, timeout=30).until(EC.element_to_be_clickable((By.XPATH, "//*[@id='chrono_list_content']/div[2]"))):
                                                                print('Отработал_3')
                                                except:
                                                        print('Не отработал_3')
                        except:
                                print('Не нажал на кнопку')

                        try:
                                elements = WebDriverWait(driver, timeout=10).until(EC.element_to_be_clickable((By.XPATH, "//*[@id='chrono_list_content']/div[2]/div[2]/ul"))) 
                                last_element = elements.text
                                last_value = int(last_element[-3])
                                print('last_element', last_value)
                        except:
                                last_value = 1
                                print('last_element', last_value)

                        cases =[]
                        o = 0
                        while o < last_value:
                                if o > 0:
                                        Ctrl = WebDriverWait(driver, timeout=10).until(EC.presence_of_element_located((By.CLASS_NAME, "next")))
                                        webdriver.ActionChains(driver).move_to_element(Ctrl).click(Ctrl).perform()
                                        time.sleep(5)

                                outer_html = WebDriverWait(driver, timeout=10).until(EC.element_to_be_clickable((By.XPATH, "//*[@id='chrono_list_content']/div[2]")))
                                html = outer_html.get_attribute("outerHTML")
                                soup = BeautifulSoup(html, "html.parser")

                                books = soup.find_all('div', attrs={'class': 'b-chrono-item js-chrono-item b-chrono-cols page-break g-ec'})
                                for book in books:
                                        title_text = book.find('h2', class_='b-case-result js-case-result')
                                        title_result = title_text.text if title_text else None
                                        print('title_result: ', title_result)

                                        url_pdf = book.find('a', class_='b-case-result-text js-case-result-text js-case-result-text--doc_link')
                                        pdf = url_pdf['href'] if url_pdf else None
                                        print('___pdf: ', pdf)

                                        if pdf:
                                                is_bankrupt = 0

                                                try:
                                                        additional_info = book.find('span', class_='additional-info')
                                                        text_meeting = additional_info.text if additional_info else None
                                                        print('text_meeting: ', text_meeting)
                                                except:
                                                        print('no_text')
        
                                                if 'О завершении реализации имущества гражданина и освобождении гражданина от исполнения обязательств' in title_result or \
                                                'Прекратить производство по делу о банкротстве (ст.52, 86, 88, 119, 125, 149 ФЗ О несостоятельности, ст.150 АПК)' in title_result or \
                                                'О прекращении производства по заявлению/жалобе' in title_result or \
                                                'О завершении реализации имущества гражданина и неприменении правила об освобождении гражданина' in title_result or \
                                                'Завершить реализацию имущества гражданина. Освободить гражданина от исполнения обязательств' in title_result and pdf != None:
                                                        publish_info = book.find('p', class_='b-case-publish_info js-case-publish_info')
                                                        text = publish_info.text if publish_info else None
                                                        pattern = r'\d{2}\.\d{2}\.\d{4} г\. \d{2}:\d{2}:\d{2}'
                                                        match = re.search(pattern, text)
                                                        if match:
                                                                publish_date = match.group(0)

                                                        accepted = None
                                                        validity_date = None
                                                        is_bankrupt = 1

                                                        cases.append({
                                                                'link': pdf,
                                                                'accepted': accepted,
                                                                'decided_at': validity_date,
                                                                'created_at': publish_date,
                                                                'is_bankrupt': is_bankrupt,
                                                                'title': title_result,
                                                                'description': text_meeting,
                                                        })
                                                        print('___is_bankrupt: ', is_bankrupt)

                                                if 'о признании гражданина банкротом' in title_result or \
                                                'Признать гражданина банкротом' in title_result and pdf != None:
                                                        publish_info = book.find('p', class_='b-case-publish_info js-case-publish_info')
                                                        text = publish_info.text if publish_info else None
                                                        pattern = r'\d{2}\.\d{2}\.\d{4} г\. \d{2}:\d{2}:\d{2}'
                                                        match = re.search(pattern, text)
                                                        if match:
                                                                publish_date = match.group(0)

                                                        validity_date = None
                                                        accepted = 2

                                                        cases.append({
                                                                'link': pdf,
                                                                'accepted': accepted,
                                                                'decided_at': validity_date,
                                                                'created_at': publish_date,
                                                                'is_bankrupt': is_bankrupt,
                                                                'title': title_result,
                                                                'description': text_meeting,
                                                        })
                                                        print('___accepted__2: ', accepted)

                                                if 'Оставлено без движения' in text_meeting and pdf != None:
                                                        publish_info = book.find('p', class_='b-case-publish_info js-case-publish_info')
                                                        text = publish_info.text if publish_info else None
                                                        pattern = r'\d{2}\.\d{2}\.\d{4} г\. \d{2}:\d{2}:\d{2}'
                                                        match = re.search(pattern, text)
                                                        if match:
                                                                publish_date = match.group(0)

                                                        additional_info = book.find('span', class_='additional-info')
                                                        text = additional_info.text if additional_info else None
                                                        pattern = r'\d{2}\.\d{2}\.\d{4}'
                                                        match = re.search(pattern, text)
                                                        if match:
                                                                validity_date = match.group(0)

                                                        if 'Оставлено без движения' in text_meeting:
                                                                accepted = 0
                                                        else:
                                                                accepted = 1

                                                        cases.append({
                                                                'link': pdf,
                                                                'accepted': accepted,
                                                                'decided_at': validity_date,
                                                                'created_at': publish_date,
                                                                'is_bankrupt': is_bankrupt,
                                                                'title': title_result,
                                                                'description': text_meeting,
                                                        })

                                                if 'Дата и время судебного заседания' in text_meeting and pdf != None:
                                                        publish_info = book.find('p', class_='b-case-publish_info js-case-publish_info')
                                                        text = publish_info.text if publish_info else None
                                                        pattern = r'\d{2}\.\d{2}\.\d{4} г\. \d{2}:\d{2}:\d{2}'
                                                        match = re.search(pattern, text)
                                                        if match:
                                                                publish_date = match.group(0)

                                                        additional_info = book.find('span', class_='additional-info')
                                                        text = additional_info.text if additional_info else None
                                                        pattern = r'\d{2}\.\d{2}\.\d{4}'
                                                        match = re.search(pattern, text)
                                                        if match:
                                                                validity_date = match.group(0)

                                                        if 'Оставлено без движения' in text_meeting:
                                                                accepted = 0
                                                        else:
                                                                accepted = 1

                                                        if 'о признании гражданина банкротом' in title_result or \
                                                        'Признать гражданина банкротом' in title_result and pdf != None:
                                                                accepted = 2

                                                        cases.append({
                                                                'link': pdf,
                                                                'accepted': accepted,
                                                                'decided_at': validity_date,
                                                                'created_at': publish_date,
                                                                'is_bankrupt': is_bankrupt,
                                                                'title': title_result,
                                                                'description': text_meeting,
                                                        })

                                books = soup.find_all('div', attrs={'class': 'b-chrono-item js-chrono-item b-chrono-cols page-break g-ec even'})
                                for book in books:
                                        title_text = book.find('h2', class_='b-case-result js-case-result')
                                        title_result = title_text.text if title_text else None

                                        url_pdf = book.find('a', class_='b-case-result-text js-case-result-text js-case-result-text--doc_link')
                                        pdf = url_pdf['href'] if url_pdf else None
                                        print('___pdf: ', pdf)

                                        if pdf:
                                                is_bankrupt = 0

                                                try:
                                                        additional_info = book.find('span', class_='additional-info')
                                                        text_meeting = additional_info.text if additional_info else None
                                                        print('text_meeting: ', text_meeting)
                                                except:
                                                        print('no_text')

                                                if 'О завершении реализации имущества гражданина и освобождении гражданина от исполнения обязательств' in title_result or \
                                                'Прекратить производство по делу о банкротстве (ст.52, 86, 88, 119, 125, 149 ФЗ О несостоятельности, ст.150 АПК)' in title_result or \
                                                'О прекращении производства по заявлению/жалобе' in title_result or \
                                                'О завершении реализации имущества гражданина и неприменении правила об освобождении гражданина' in title_result or \
                                                'Завершить реализацию имущества гражданина. Освободить гражданина от исполнения обязательств' in title_result and pdf != None:
                                                        publish_info = book.find('p', class_='b-case-publish_info js-case-publish_info')
                                                        text = publish_info.text if publish_info else None
                                                        pattern = r'\d{2}\.\d{2}\.\d{4} г\. \d{2}:\d{2}:\d{2}'
                                                        match = re.search(pattern, text)
                                                        if match:
                                                                publish_date = match.group(0)

                                                        accepted = None
                                                        validity_date = None
                                                        is_bankrupt = 1

                                                        cases.append({
                                                                'link': pdf,
                                                                'accepted': accepted,
                                                                'decided_at': validity_date,
                                                                'created_at': publish_date,
                                                                'is_bankrupt': is_bankrupt,
                                                                'title': title_result,
                                                                'description': text_meeting,
                                                        })
                                                        print('___is_bankrupt: ', is_bankrupt)

                                                if 'о признании гражданина банкротом' in title_result or \
                                                'Признать гражданина банкротом' in title_result and pdf != None:
                                                        publish_info = book.find('p', class_='b-case-publish_info js-case-publish_info')
                                                        text = publish_info.text if publish_info else None
                                                        pattern = r'\d{2}\.\d{2}\.\d{4} г\. \d{2}:\d{2}:\d{2}'
                                                        match = re.search(pattern, text)
                                                        if match:
                                                                publish_date = match.group(0)

                                                        validity_date = None
                                                        accepted = 2

                                                        cases.append({
                                                                'link': pdf,
                                                                'accepted': accepted,
                                                                'decided_at': validity_date,
                                                                'created_at': publish_date,
                                                                'is_bankrupt': is_bankrupt,
                                                                'title': title_result,
                                                                'description': text_meeting,
                                                        })
                                                        print('___accepted__2: ', accepted)

                                                if 'Оставлено без движения' in text_meeting and pdf != None:
                                                        publish_info = book.find('p', class_='b-case-publish_info js-case-publish_info')
                                                        text = publish_info.text if publish_info else None
                                                        pattern = r'\d{2}\.\d{2}\.\d{4} г\. \d{2}:\d{2}:\d{2}'
                                                        match = re.search(pattern, text)
                                                        if match:
                                                                publish_date = match.group(0)

                                                        additional_info = book.find('span', class_='additional-info')
                                                        text = additional_info.text if additional_info else None
                                                        pattern = r'\d{2}\.\d{2}\.\d{4}'
                                                        match = re.search(pattern, text)
                                                        if match:
                                                                validity_date = match.group(0)

                                                        if 'Оставлено без движения' in text_meeting:
                                                                accepted = 0
                                                        else:
                                                                accepted = 1

                                                        cases.append({
                                                                'link': pdf,
                                                                'accepted': accepted,
                                                                'decided_at': validity_date,
                                                                'created_at': publish_date,
                                                                'is_bankrupt': is_bankrupt,
                                                                'title': title_result,
                                                                'description': text_meeting,
                                                        })

                                                if 'Дата и время судебного заседания' in text_meeting and pdf != None:
                                                        publish_info = book.find('p', class_='b-case-publish_info js-case-publish_info')
                                                        text = publish_info.text if publish_info else None
                                                        pattern = r'\d{2}\.\d{2}\.\d{4} г\. \d{2}:\d{2}:\d{2}'
                                                        match = re.search(pattern, text)
                                                        if match:
                                                                publish_date = match.group(0)

                                                        additional_info = book.find('span', class_='additional-info')
                                                        text = additional_info.text if additional_info else None
                                                        pattern = r'\d{2}\.\d{2}\.\d{4}'
                                                        match = re.search(pattern, text)
                                                        if match:
                                                                validity_date = match.group(0)

                                                        if 'Оставлено без движения' in text_meeting:
                                                                accepted = 0
                                                        else:
                                                                accepted = 1
                                                        
                                                        if 'о признании гражданина банкротом' in title_result or \
                                                        'Признать гражданина банкротом' in title_result and pdf != None:
                                                                accepted = 2

                                                        cases.append({
                                                                'link': pdf,
                                                                'accepted': accepted,
                                                                'decided_at': validity_date,
                                                                'created_at': publish_date,
                                                                'is_bankrupt': is_bankrupt,
                                                                'title': title_result,
                                                                'description': text_meeting,
                                                        })

                                o += 1
                                print('Прошел страницу', o)
                                if o == last_value:
                                        print('Завершил гулять по страницам')
                                        break
                        # if cases:
                        print('___cases: ', cases)
                        post = requests.post(f'https://arbitr.sms19.ru/api/card/{id}', json = {'documents': cases})
                        print('___post_json : ', post.json())
                        print('___post : ', post)
                        print('--------')

                        driver.close()

                except:
                        print('error_pars')
                        time.sleep(15)
                        driver.close()
