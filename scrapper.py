from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
import time
import random
import datetime
import re
from selenium.webdriver.chrome.options import Options
import json
from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers=["broker:29092"],  # Change as per your Kafka broker address
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def randomSleep(x):
    time.sleep(x + 2 * (random.random() - 0.5))


def click_by_xpath(driver, xpath):
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, xpath)))
    close_btn = driver.find_element(By.XPATH, xpath)
    ActionChains(driver).move_to_element(close_btn).click().perform()


def get_element(driver, category, id, all=False):
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((category, id)))
    return (
        driver.find_elements(category, id) if all else driver.find_element(category, id)
    )


def set_chrome_options() -> Options:
    """Sets chrome options for Selenium.
    Chrome options for headless browser is enabled.
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    )
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)
    chrome_options.add_argument("--disable-notifications")  # Disable notifications
    chrome_options.add_argument("window-size=1200x600")
    chrome_prefs = {}
    chrome_options.experimental_options["prefs"] = chrome_prefs
    chrome_prefs["profile.default_content_settings"] = {"images": 2}
    return chrome_options


def open_webpage(count):
    data_list = []

    driver = webdriver.Chrome(options=set_chrome_options())
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )

    try:
        driver.get("https://tiktok.com/foryou")

        # close button
        click_by_xpath(driver, '//div[@aria-label="Close"]')
        main_div = get_element(driver, By.ID, "main-content-homepage_hot")
        feed_div = get_element(main_div, By.XPATH, "//div")
        first_video = feed_div.find_element(By.XPATH, "//div")
        first_video.click()
        print("clicked on the first video !@")
        print("\n")
        print("\n")
        try:
            botDetector = get_element(
                driver, By.XPATH, '//div[@id="tiktok-verify-ele"]'
            )
            print("bot detector len ", botDetector)
            return

        except:
            print("escaped!")
            pass

        for i in range(count, 10):
            try:
                botDetector = driver.find_element(
                    By.XPATH, '//div[@id="tiktok-verify-ele"]'
                )
                print("bot detector len ", botDetector)
                return

            except:
                print("escaped!")

            count += 1
            randomSleep(2)
            URL = driver.current_url
            Account = get_element(
                driver, By.XPATH, '//span[@data-e2e="browse-username"]'
            ).text

            likes = get_element(
                driver, By.XPATH, '//strong[@data-e2e="browse-like-count"]'
            ).text

            Comments = get_element(
                driver, By.XPATH, '//strong[@data-e2e="browse-comment-count"]'
            ).text

            Saved = get_element(
                driver,
                By.XPATH,
                "/html/body/div[1]/div[2]/div[4]/div/div[2]/div[1]/div/div[1]/div[2]/div/div[1]/div[1]/button[3]/strong",
            ).text

            Caption = get_element(
                driver, By.XPATH, '//div[@data-e2e="browse-video-desc"]'
            ).text
            DateColected = datetime.datetime.now().strftime("%Y-%m-%d")
            Hashtags = re.findall(r"#\w+", Caption)
            DatePosted = get_element(
                driver,
                By.XPATH,
                "/html/body/div[1]/div[2]/div[4]/div/div[2]/div[1]/div/div[1]/div[1]/div[1]/a[2]/span[2]/span[3]",
            ).text

            RawData = get_element(
                driver, By.XPATH, '//div[@data-e2e="search-comment-container"]'
            ).text

            # Date Views
            videoData = {
                "Account": Account,
                "URL": URL,
                "likes": likes,
                "comments": Comments,
                "saved": Saved,
                "Caption": Caption,
                "Hashtags": Hashtags,
                "DateColected": DateColected,
                "DatePosted": DatePosted,
                "RawData": RawData,
            }
            print(videoData)
            producer.send("video_data", videoData)
            producer.flush()

            data_list.append(videoData)
            # put the data inside a json file
            print("\n")

            click_by_xpath(driver, '//button[@aria-label="Go to next video"]')

    except Exception as e:
        print(e)
        print("broke cuz of error")
        driver.quit()
        open_webpage(count)

    finally:
        driver.quit()


if __name__ == "__main__":
    open_webpage(0)
