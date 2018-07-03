import os
import datetime
import math
from utils import CHROMEDRIVER_PATH, init_webdriver, mp_func
import pandas as pd
from selenium.common.exceptions import StaleElementReferenceException, WebDriverException
import time

DESKTOP = os.path.join(os.environ['USERPROFILE'], 'Desktop')
PRODUCT_INDEX_PATH = os.path.join(DESKTOP, "product_index_" + datetime.datetime.now().strftime("%Y%m%d-%H"))


class DKDL:
    def __init__(self, dl_spg_path, chromedriver_path=CHROMEDRIVER_PATH,
                 product_index_path=PRODUCT_INDEX_PATH):

        self.chromedriver_path = chromedriver_path
        self.dl_spg_path = dl_spg_path

        if os.path.isdir(product_index_path):
            self.product_index_path = product_index_path

        else:
            os.mkdir(product_index_path)
            self.product_index_path = product_index_path

        self.dl_spg_df = pd.read_excel(self.dl_spg_path)
        self.

    def get_dl_path(self, spg_data):
        spg_url = spg_data[0]
        num_page = spg_data[1]

        pg_url_key = spg_url.split('/')[-3]
        spg_url_key = spg_url.split('/')[-2]

        batch_num = 'batch_' + str(math.ceil(num_page/100))
        dl_path = os.path.join(self.product_index_path, pg_url_key, spg_url_key, batch_num)

        return dl_path

    def dl_page(self, spg_data):
        spg_url = spg_data[0]
        dl_path = self.get_dl_path(spg_data)

        load_url_attempts = 1

        while load_url_attempts <= 5:
            try:
                browser = init_webdriver(mode='dl', dl_path=dl_path)
                break

            except (WebDriverException, StaleElementReferenceException):
                if load_url_attempts < 5:
                    print('spg_url', spg_url, 'crashed.', 'Load attempt:', load_url_attempts)
                else:
                    print('spg_url', spg_url, 'all load attempts have crashed.')
                    return
                load_url_attempts += 1

        dl_attempts = 1
        while dl_attempts <= 15:
            try:
                browser.set_page_load_timeout(60)
                browser.get(spg_url)
                browser.implicitly_wait(2)
                # browser.maximize_window()

                dl_xpath = "//*[@id='content']/div[@class='mid-wrapper']" \
                           "/div[@class='dload-btn']" \
                           "/form[@class='download-table']" \
                           "/input[@class='button']"

                browser.find_element_by_xpath(dl_xpath).click()
                browser.implicitly_wait(2)

                break

            except (WebDriverException, StaleElementReferenceException):
                if dl_attempts < 15:
                    print('spg_url', spg_url, 'crashed.', 'dl attempt:', dl_attempts)
                else:
                    print('spg_url', spg_url, 'all dl attempts have crashed.')
                dl_attempts += 1

        time.sleep(6)
        browser.quit()
        time.sleep(1)

    def dl_all(self):
        dl_spg_list = self.enum_dl_spg()
        mp_func(self.dl_page, dl_spg_list, has_return=False)

    def enum_dl_spg(self):
        dl_spg_list = []
        for url, num_page in zip(self.dl_spg_df['spg_url'], self.dl_spg_df['num_page']):
            for i in range(1, num_page+1):
                dl_spg_list.append([url + '?&page=' + str(i) + '&pageSize=500', i])
        return dl_spg_list

def main():
    dkdl = DKDL(r"metadata/dl_spg.xlsx")
    dkdl.dl_all()

if __name__ == '__main__':
    main()
