import pickle
import re
import time
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC  
from sqlalchemy import create_engine
import pandas as pd      
from fake_useragent import UserAgent     
from selenium.common.exceptions import WebDriverException

engine = create_engine('mysql+pymysql://mufer:vRZVgh6c@localhost:3306/data_collection', isolation_level="AUTOCOMMIT")
conn = engine.connect()

acc = [
    {'id': '1', 'name': 'Eryshkanov'}, 
    {'id': '2', 'name': 'Sidorov'}, 
    {'id': '3', 'name': 'Subkhangulov'}
]

o = 0
while o < 3:
    try:
        for row in acc:
            df_proxies = pd.read_csv('/home/dmitry/Рабочий стол/airflow/dags/proxies.csv')
            proxy = df_proxies.sample(n=1)
            ip = proxy['ip'].values[0]
            port = proxy['port'].values[0]
            name = row['name']
            id = row['id']

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
            driver.get('https://guard.arbitr.ru/#claims')

            with open(f'/home/dmitry/Рабочий стол/airflow/data/production_bfl/cookies_{name}.pkl', 'rb') as file:
                cookies = pickle.load(file)

            for cookie in cookies:
                driver.add_cookie(cookie)

            driver.get('https://guard.arbitr.ru/#claims')
            window_before = driver.window_handles[0]

            # time.sleep(67676)

            outer_html = WebDriverWait(driver, timeout=10).until(EC.element_to_be_clickable((By.XPATH, '//div[@class="b-pagination"]/a[last()-1]')))
            last_value = int(outer_html.text)
            print(last_value) # 242

            # last_value = 5

            i = 0
            while i < last_value:
                if i > 0:
                    Ctrl = WebDriverWait(driver, timeout=10).until(EC.presence_of_element_located((By.CLASS_NAME, "next")))
                    webdriver.ActionChains(driver).move_to_element(Ctrl).click(Ctrl).perform()
                    time.sleep(5)
                    
                q = 0
                while q < 3:
                    try:
                        outer_html = WebDriverWait(driver, timeout=10).until(EC.element_to_be_clickable((By.XPATH, "/html/body/div[1]/div/div[4]/div")))
                        html = outer_html.get_attribute("outerHTML")
                        soup = BeautifulSoup(html, "html.parser")

                        books = soup.find_all('li', attrs={'class': 'b-claims-recourse b-claims-recourse--odd js-claims-recourse'}) \
                            or soup.find_all('div', attrs={'class': 'b-claims-recourse b-claims-recourse--even js-claims-recourse'})

                        cases =[]
                        for book in books:
                            title = book.find('a', class_='b-claims-recourse-link js-claims-recourse-link')
                            title_text = title.text if title else None
                            title_text_strip = title_text.strip() if title_text else None

                            url = book.find('a', class_='b-claims-recourse-link js-claims-recourse-link')['href']
                            id_person = url.split('/')[-1]

                            status = book.find('div', class_='b-claims-recourse-status b-claims-recourse-status--accepted') \
                            or book.find('div', class_='b-claims-recourse-status b-claims-recourse-status--process') \
                            or book.find('div', class_='b-claims-recourse-status b-claims-recourse-status--rejected js-claims-recourse-status--rejected')
                            status_text = status.text if status else None
                            status_text_strip = status_text.strip() if status_text else None

                            lines = status_text_strip.split('\n') 
                            name_lines = lines[0].strip().split(' ') 
                            name = name_lines[0]

                            date_lines = lines[0].strip().split(' ') 
                            date = date_lines[1] + " " + date_lines[2]

                            if len(lines) > 2:
                                comment = lines[-1].strip() 
                            else:
                                comment = None

                            info = book.find('div', class_='b-claims-recourse-status b-claims-recourse-status--received')
                            info_text = info.text if info else None
                            info_text_strip = info_text.strip() if info_text else None
                            if info_text_strip:
                                date_of_application = re.search(r'\d{2}\.\d{2}\.\d{4}, \d{2}:\d{2}', info_text_strip)
                                if date_of_application:
                                    date_of_application = date_of_application.group()
                            else:
                                date_of_application = None

                            number = book.find('a', class_='b-claims-recourse-icon js-claims-recourse-link')
                            number_cases = number.text if number else None
                            number_cases_strip = number_cases.strip() if number_cases else None
                            if number_cases_strip:
                                case_number = re.search(r'\w+-\w+/\d+', number_cases_strip)
                                if case_number:
                                    case_number = case_number.group()
                            else:
                                case_number = None

                            cases.append({
                                'uuid': id_person,
                                'status': {
                                    'name': name,
                                    'date': date,
                                    'comment': comment,
                                },
                                'date_of_application': date_of_application,
                                'number': case_number
                            })
                        print('__cases: ', cases)
                        break
                    except WebDriverException as e:
                        print("WebDriverException occurred:", e)
                        time.sleep(20)
                        q += 1
                        if q == 3:
                            break

                j = 0
                while j < 3:
                    try:
                        driver.execute_script("window.open('https://tilda.cc/')")
                        driver.switch_to.window(driver.window_handles[1])

                        # response = requests.get('https://arbitr.sms19.ru/api/json/court_cases?token=6c0ab81e19a4b80f159d626fd8a11218')
                        # data = response.json()
                        # court_cases = pd.json_normalize(data)

                        cases_new = []
                        for case in cases:

                            uuid = case['uuid']
                            status_name = case['status']['name']
                            status_date = case['status']['date']
                            status_comment = case['status']['comment']
                            date_of_application = case['date_of_application']
                            number = case['number']

                            driver.get(f'https://my.arbitr.ru/Claim/#person_insolvency/{uuid}')

                            try:
                                element = WebDriverWait(driver, timeout=10).until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[1]/div[2]/div[5]/div/form/div/table/tbody/tr[3]/td[1]')))
                                html = element.get_attribute("outerHTML")
                                soup = BeautifulSoup(html, "html.parser")
                                lastname = soup.find('input', {'name': 'LastName'}).get('value')
                                print('lastname: ', lastname)

                                element = WebDriverWait(driver, timeout=10).until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[1]/div[2]/div[5]/div/form/div/table/tbody/tr[3]/td[3]')))
                                html = element.get_attribute("outerHTML")
                                soup = BeautifulSoup(html, "html.parser")
                                middlename = soup.find('input', {'name': 'MiddleName'}).get('value')
                                print('middlename: ', middlename)

                                element = WebDriverWait(driver, timeout=10).until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[1]/div[2]/div[5]/div/form/div/table/tbody/tr[3]/td[2]')))
                                html = element.get_attribute("outerHTML")
                                soup = BeautifulSoup(html, "html.parser")
                                firstname = soup.find('input', {'name': 'FirstName'}).get('value')
                                print('firstname: ', firstname)

                                element = WebDriverWait(driver, timeout=10).until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[1]/div[2]/div[5]/div/form/div/table/tbody/tr[6]/td[1]')))
                                html = element.get_attribute("outerHTML")
                                soup = BeautifulSoup(html, "html.parser")
                                birthdate = soup.find('input', {'name': 'BirthDate'}).get('value')
                                print('birthdate: ', birthdate)

                                element = WebDriverWait(driver, timeout=10).until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[1]/div[2]/div[5]/div/form/div/table/tbody/tr[4]/td[1]')))
                                html = element.get_attribute("outerHTML")
                                soup = BeautifulSoup(html, "html.parser")
                                inn = soup.find('input', {'name': 'INN'}).get('value')
                                print('inn: ', inn)

                                element = WebDriverWait(driver, timeout=10).until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[1]/div[2]/div[5]/div/form/div/table/tbody/tr[5]/td[2]')))
                                html = element.get_attribute("outerHTML")
                                soup = BeautifulSoup(html, "html.parser")
                                doc_ser = soup.find('input', {'name': 'DocSeries'}).get('value')
                                print('doc_ser: ', doc_ser)

                                element = WebDriverWait(driver, timeout=10).until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[1]/div[2]/div[5]/div/form/div/table/tbody/tr[5]/td[3]')))
                                html = element.get_attribute("outerHTML")
                                soup = BeautifulSoup(html, "html.parser")
                                doc_num = soup.find('input', {'name': 'DocNumber'}).get('value')
                                print('doc_num: ', doc_num)

                                element = WebDriverWait(driver, timeout=10).until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[1]/div[2]/div[5]/div/form/div/table/tbody/tr[4]/td[2]')))
                                html = element.get_attribute("outerHTML")
                                soup = BeautifulSoup(html, "html.parser")
                                snils = soup.find('input', {'name': 'Snils'}).get('value')
                                print('snils: ', snils)

                                is_claim = 1

                            except:
                                element = WebDriverWait(driver, timeout=10).until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[1]/div[2]/div[5]/div/form/div/table/tbody/tr[1]/td')))
                                html = element.get_attribute("outerHTML")
                                soup = BeautifulSoup(html, "html.parser")
                                OrgFormId = soup.find('input', {'name': 'OrgFormId'}).get('value')
                                print('OrgFormId: ', OrgFormId)

                                if OrgFormId == '00000000-0000-0000-0000-000000000000':
                                    inn = None
                                    birthdate = None
                                    doc_ser = None
                                    doc_num = None
                                    snils = None
                                    is_claim = 0
                                else:
                                    raise WebDriverException()

                            time.sleep(5)

                            cases_new.append({
                                'uuid': uuid,
                                'status': {
                                    'name': status_name,
                                    'date': status_date,
                                    'comment': status_comment,
                                },
                                'date_of_application': date_of_application,
                                'case': {
                                    'first_name': firstname,
                                    'last_name': lastname,
                                    'middle_name': middlename,
                                    'birthdate': birthdate,
                                    'number': number,
                                    'inn': inn,
                                    'doc_ser': doc_ser,
                                    'doc_num': doc_num,
                                    'is_claim': is_claim,
                                    'snils': snils,
                                }
                            })

                        print('___cases_new: ', cases_new)

                        x = 0
                        while x < 10:
                            try:
                                post = requests.post(f'https://arbitr.sms19.ru/api/court_case/lk/{id}',json = {'cases': cases_new})
                                print('___post: ', post)
                                break
                            except:
                                x += 1
                                time.sleep(60)
                                if x == 10:
                                    break

                        driver.close()
                        driver.switch_to.window(window_before)
                        break
                    except WebDriverException as e:
                        print("WebDriverException occurred:", e)
                        driver.close()
                        driver.switch_to.window(window_before)
                        time.sleep(20)
                        j += 1
                        if j == 3:
                            break

                i += 1
                if i == last_value:
                    break

            driver.get('https://guard.arbitr.ru/#claims')
            cookies = driver.get_cookies()

            with open(f'/home/dmitry/Рабочий стол/airflow/data/production_bfl/cookies_{name}.pkl', 'wb') as file:
                pickle.dump(cookies, file)

            driver.close()

        break

    except WebDriverException as e:
        print("WebDriverException occurred:", e)
        time.sleep(320)
        o += 1
        if o == 3:
            break
