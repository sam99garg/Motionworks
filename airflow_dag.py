import time
from datetime import datetime

import numpy as np
from airflow import DAG
from airflow.operators.bash import BashOperator

# from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# import selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.ui import WebDriverWait

# from random import randint


# from airflow.operators.python import BranchPythonOperator
# from airflow.utils.edgemodifier import Label
# from airflow.utils.trigger_rule import TriggerRule


"""import multiprocessing
from multiprocessing import Pool
import pandas as pd"""


# --------------------------------- Creating Sub-lists -------------------------------------------
def sub_lists(ti):
    list_of_airports = ti.xcom_pull(id="airports", task_id="airport_list")
    list_of_airports = np.array_split(list_of_airports, 2)
    print(list_of_airports)
    return list_of_airports


# ------------------------------ Extraacting airport list -----------------------------------
def airports(ti):
    URL = "https://www.transtats.bts.gov/Data_Elements.aspx"
    DRIVER_PATH = "/opt/homebrew/bin/chromedriver"

    options = Options()
    options.headless = True
    options.add_argument("--window-size=1920,1200")

    browser = webdriver.Chrome(options=options, executable_path=DRIVER_PATH)
    browser.get(URL)

    lst = []
    try:
        element = WebDriverWait(browser, 5).until(
            EC.presence_of_element_located((By.ID, "AirportList"))
        )

        for data in element.find_elements_by_tag_name("option"):
            lst.append(data.text)

    except Exception:
        print("Failed Airport extraction")

    browser.quit()
    ti.xcom_push(key="airports", value=lst[:2])


# -------------------------------------------------------------------------------

"""def sep1(ti):
    airports = ti.xcom_pull(task_id='break_list')
    airports = airports[0]
    return airports

#--------------------------------------------------------------------------------

def sep2(ti):
    airports = ti.xcom_pull(task_id='break_list')
    airports = airports[1]
    return airports
"""
# -------------------------------------------------------------------------------


def data1(ti):
    airports = ti.xcom_pull(task_id="break_list")
    airports = airports[0]
    URL = "https://www.transtats.bts.gov/Data_Elements.aspx"
    DRIVER_PATH = "/opt/homebrew/bin/chromedriver"

    options = Options()
    options.headless = True
    options.add_argument("--window-size=1920,1200")

    browser = webdriver.Chrome(options=options, executable_path=DRIVER_PATH)
    browser.get(URL)

    d = {"Origin": [], "Destination": []}

    try:
        for airport in airports:
            for case in ["Origin", "Destination"]:
                time.sleep(5)

                elem = browser.find_element_by_id("Link_" + case)
                elem.click()

                try:
                    time.sleep(5)

                    # select airport
                    select = Select(browser.find_element_by_id("AirportList"))

                    # select by visible text
                    select.select_by_visible_text(airport)

                    # select carrier
                    Select(
                        browser.find_element_by_id("CarrierList")
                    ).select_by_visible_text("All U.S. and Foreign Carriers")

                    time.sleep(1)
                    # hit submit!
                    elem = browser.find_element_by_id("Submit")
                    elem.click()

                    time.sleep(3)

                    try:
                        element = WebDriverWait(browser, 2).until(
                            EC.presence_of_element_located((By.ID, "GridView1"))
                        )
                        datas = element.find_elements_by_class_name("dataTDRight")

                        list_of_rows = []
                        for data in datas:
                            list_of_cells = ["All U.S. and Foreign Carriers", airport]
                            for cell in data.find_elements_by_tag_name("td"):
                                text = cell.text.replace("&nbsp;", "")
                                list_of_cells.append(text)
                            list_of_rows.append(list_of_cells)
                            d[case].append(list_of_cells)

                        # print(case, '     ', airports, '   Done!')
                    except Exception:
                        pass
                        # print(e)
                        # print("\n\n\nFailed: ", case, airports, "\n\n\n")

                except Exception:
                    pass
                    # print(e)
                    # print("\n\n\nError!!!!!!  ",airports, case,"\n\n\n")
    except Exception:
        pass
        # print(e)
        # print("\n\n\ncase error:", airports, "\n\n\n")

    browser.quit()
    return "hooray"


# ------------------------------------------------------------------------------


def data2(ti):
    airports = ti.xcom_pull(task_id="break_list")
    airports = airports[1]
    URL = "https://www.transtats.bts.gov/Data_Elements.aspx"
    DRIVER_PATH = "/opt/homebrew/bin/chromedriver"

    options = Options()
    options.headless = True
    options.add_argument("--window-size=1920,1200")

    browser = webdriver.Chrome(options=options, executable_path=DRIVER_PATH)
    browser.get(URL)

    d = {"Origin": [], "Destination": []}

    try:
        for airport in airports:
            for case in ["Origin", "Destination"]:
                time.sleep(5)

                elem = browser.find_element_by_id("Link_" + case)
                elem.click()

                try:
                    time.sleep(5)

                    # select airport
                    select = Select(browser.find_element_by_id("AirportList"))

                    # select by visible text
                    select.select_by_visible_text(airport)

                    # select carrier
                    Select(
                        browser.find_element_by_id("CarrierList")
                    ).select_by_visible_text("All U.S. and Foreign Carriers")

                    time.sleep(1)
                    # hit submit!
                    elem = browser.find_element_by_id("Submit")
                    elem.click()

                    time.sleep(3)

                    try:
                        element = WebDriverWait(browser, 2).until(
                            EC.presence_of_element_located((By.ID, "GridView1"))
                        )
                        datas = element.find_elements_by_class_name("dataTDRight")

                        list_of_rows = []
                        for data in datas:
                            list_of_cells = ["All U.S. and Foreign Carriers", airport]
                            for cell in data.find_elements_by_tag_name("td"):
                                text = cell.text.replace("&nbsp;", "")
                                list_of_cells.append(text)
                            list_of_rows.append(list_of_cells)
                            d[case].append(list_of_cells)

                        # print(case, '     ', airports, '   Done!')
                    except Exception:
                        pass
                        # print(e)
                        # print("\n\n\nFailed: ", case, airports, "\n\n\n")

                except Exception:
                    pass
                    # print(e)
                    # print("\n\n\nError!!!!!!  ",airports, case,"\n\n\n")
    except Exception:
        pass
        # print(e)
        # print("\n\n\ncase error:", airports, "\n\n\n")

    browser.quit()
    return "hooray"


# -----------------------------------------------------------------------------


# --------------------------------------------------------------------------------

with DAG(
    "my_dag", start_date=datetime(2022, 10, 2), schedule="@daily", catchup=False
) as dag:

    airport_list = PythonOperator(
        task_id="airport_list", python_callable=airports, provide_context=True
    )

    break_list = PythonOperator(task_id="break_list", python_callable=sub_lists)

    save_df_a = PythonOperator(task_id="save_df_a", python_callable=data1)

    save_df_b = PythonOperator(task_id="save_df_b", python_callable=data2)

    hooray = BashOperator(task_id="hooray", bash_command="echo 'Hooray!!'")

    airport_list >> break_list >> [save_df_a, save_df_b] >> hooray
