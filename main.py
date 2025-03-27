"""抓取交通部觀光署的公開資料(來臺旅客_按居住地分析、中華民國國民出國_按目的地分析)"""

import os
import re
from random import uniform
from time import sleep
from typing import Dict, Literal, Set, Text, Tuple, Union

import arrow
import polars as pl
import polars.selectors as cs
import requests
import settings
import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from selenium.webdriver.chrome.options import Options


class TbStatsMonthly(object):
    def __init__(self, cat: Literal["inbound", "outbound"]):
        """抓取交通部觀光局的公開資料

        Args:
            cat (Literal[&quot;inbound&quot;, &quot;outbound&quot;]): 規定輸入的參數，用此參數取得settings中的dictionary的value
        """
        self.cat_dic = settings.cat_dict[cat]
        self.last_month = arrow.now().shift(months=-1).format("YYYY-MM")

    def _get_month_set(
        self, final_month: Text, sql_month_dic: Dict[Text, Text]
    ) -> Set[Text]:
        """與SQL的資料進行比對，確認哪些month的資料需在網站抓取

        Args:
            final_month (Text): 網站上最新的月份
            sql_month_dic (Dict[Text, Text]): 從sql中獲取的月份資料

        Returns:
            Set[Text]: 需要在網站抓取資料的月份set
        """
        start_month = arrow.get("2008-01")
        end_month = arrow.get(final_month)

        month_set = {
            date_arrow.format("YYYY-MM")
            for index, date_arrow in enumerate(
                arrow.Arrow.range("months", start_month, end_month)
            )
        }
        month_set = {month for month in month_set if sql_month_dic.get(month) is None}

        return month_set

    def _download_xlsx(self, month: Text):
        """從網站下載xlsx

        Args:
            month (Text): 需要下載的month的資料
        """
        headers = {
            "accept": "text/plain",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7,ja;q=0.6",
            # "authorization": "",
            "connection": "keep-alive",
            "host": "stat.taiwan.net.tw",
            "referer": self.cat_dic["url"],
            "sec-ch-ua": '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        }
        base_url = self.cat_dic["api_url"].format(
            int(month.split("-")[0]) - 1911,
            int(month.split("-")[-1]),
            int(month.split("-")[-1]),
        )
        response = requests.get(base_url, headers=headers)
        sleep(uniform(3, 5))
        if response.status_code == 200:
            with open(
                self.cat_dic["src_path"].joinpath(
                    "{}_{}{}.xlsx".format(
                        self.cat_dic["cat"], month.split("-")[0], month.split("-")[-1]
                    )
                ),
                "wb",
            ) as file:
                file.write(response.content)
        else:
            pass

    @staticmethod
    def _set_options() -> Union[uc.ChromeOptions, Options]:
        """設定options

        Returns:
            Union[uc.ChromeOptions, Options]: 設定的options
        """
        options = uc.ChromeOptions()
        # options = webdriver.ChromeOptions()
        options.add_argument("--incognito")
        options.add_argument("user-agengt={}".format(UserAgent().random))

        return options

    def _get_final_month(self) -> Union[Text, None]:
        """確認網站上最新資料的月份

        Raises:
            current_except: 如果有錯誤則拋出錯誤

        Returns:
            Union[Text, None]: 網站上最新資料的月份
        """
        current_except = None
        try:
            driver = uc.Chrome(headless=False, options=self._set_options())
            base_url = self.cat_dic["url"]
            driver.get(base_url)
            driver.implicitly_wait(200)
            sleep(uniform(3, 5))

            soup = BeautifulSoup(driver.page_source, "lxml")
            now_year = soup.select('select[formControlName="year"] > option')[0].text
            now_month = soup.select('select[formControlName="monthEnd"] > option')[
                -1
            ].text
            final_month = "{}-{}".format(
                re.search(r"\((\d{4})\)", now_year).group(1),
                now_month.replace("月", "").zfill(2),
            )
        except Exception as e:
            print(e)
            return None
        else:
            return final_month
        finally:
            driver.quit()
            if current_except:
                raise current_except

    def _xlsx_to_db(self, month: Text, cat: Literal["inbound", "outbound"]):
        """將xlsx的資料，整理之後upsert db

        Args:
            month (Text): 抓取的資料為哪個月
            cat (Literal[&quot;inbound&quot;, &quot;outbound&quot;]): 作為要選擇哪一種處理方式
        """
        lf = pl.read_excel(
            self.cat_dic["src_path"].joinpath(
                "{}_{}{}.xlsx".format(
                    self.cat_dic["cat"], month.split("-")[0], month.split("-")[-1]
                )
            )
        ).lazy()
        column_list = lf.collect().columns
        mapping = {}
        result_list = []
        if cat == "inbound":
            for index, column in enumerate(column_list):
                if index == 0:
                    mapping[column] = "continent"
                elif index == 1:
                    mapping[column] = "country"
                elif index == 2:
                    mapping[column] = "country_02"
                elif index == 3:
                    mapping[column] = "total"
                elif index == 4:
                    mapping[column] = "oversears_chinese"
                elif index == 5:
                    mapping[column] = "foreigners"
                else:
                    break
            result_list += (
                lf.select(cs.by_index(range(0, 6)))
                .rename(mapping)
                .with_columns(pl.col("total").str.extract(r"^(\d+)$", 1).cast(pl.Int64))
                .filter(pl.col("total").is_not_null())
                .with_columns(
                    pl.col("country").fill_null(strategy="forward"),
                    pl.col("continent").fill_null(strategy="forward"),
                )
                .with_columns(
                    continent=pl.when(pl.col("country") == "未列明 Unstated")
                    .then(pl.lit("未列明"))
                    .otherwise(pl.col("continent")),
                    country=pl.when(pl.col("country").str.contains("東南亞地區"))
                    .then(pl.col("country_02"))
                    .otherwise(pl.col("country")),
                    YEAR=pl.lit(int(month.split("-")[0])),
                    MONTH=pl.lit(int(month.split("-")[-1])),
                )
                .filter(~pl.col("country").str.contains("Total"))
                .drop("country_02")
                .cast(
                    {
                        "total": pl.Int64,
                        "oversears_chinese": pl.Int64,
                        "foreigners": pl.Int64,
                    }
                )
                .collect()
                .to_dicts()
            )
        elif cat == "outbound":
            for index, column in enumerate(column_list):
                if index == 0:
                    mapping[column] = "continent"
                elif index == 1:
                    mapping[column] = "country"
                elif index == 2:
                    mapping[column] = "total"
                else:
                    break
            result_list += (
                lf.select(cs.by_index(range(0, 3)))
                .rename(mapping)
                .with_columns(pl.col("total").str.extract(r"^(\d+)$", 1).cast(pl.Int64))
                .filter(pl.col("total").is_not_null())
                .filter(~pl.col("country").str.contains("Total"))
                .with_columns(
                    pl.col("continent").fill_null(strategy="forward"),
                )
                .with_columns(
                    continent=pl.when(pl.col("country") == "其他 Others")
                    .then(pl.lit("其他"))
                    .otherwise(pl.col("continent")),
                    YEAR=pl.lit(int(month.split("-")[0])),
                    MONTH=pl.lit(int(month.split("-")[-1])),
                )
                .collect()
                .to_dicts()
            )

        # 此處為要插入sql的資料
        print(result_list)

    def _check_last_month(self) -> Tuple[bool, Dict[Text, Text]]:
        """從SQL中獲取已經有的月份資料，確認最新的資料的月份是否為上個月

        Returns:
            Tuple[bool, Dict[Text, Text]]: 最新的資料是否為上個月、SQL中已有的月份資料
        """
        # 此處假設此為從sql中篩選出的結果
        sql_month_list = [{"year": 2020, "month": 1}]
        sql_month_dic = {
            f"{item['year']}-{str(item['month']).zfill(2)}": f"{item['year']}-{str(item['month']).zfill(2)}"
            for item in sql_month_list
        }

        is_last_month = True if sql_month_dic.get(self.last_month) else False

        return is_last_month, sql_month_dic

    def _delete_xlsx(self):
        """刪除xlsx"""
        xlsx_list = self.cat_dic["src_path"].glob(f"{self.cat_dic['cat']}_*.xlsx")
        for xlsx in xlsx_list:
            os.remove(xlsx)

    def _run_all(self):
        is_last_month, sql_month_dic = self._check_last_month()
        if is_last_month is False:
            final_month = self._get_final_month()
            if final_month:
                month_set = self._get_month_set(final_month, sql_month_dic)
                if month_set:
                    for month in month_set:
                        self._download_xlsx(month)
                        self._xlsx_to_db(month, self.cat_dic["cat"])

    def main(self):
        try:
            self._run_all()
        except Exception as e:
            print(e)
            print("程序出現錯誤！")
        else:
            print("程序執行完成！")
        finally:
            self._delete_xlsx()
            print("無論成功或失敗，我一定會執行！")


if __name__ == "__main__":
    cat_list = ["inbound", "outbound"]
    for cat in cat_list:
        TbStatsMonthly(cat=cat).main()
