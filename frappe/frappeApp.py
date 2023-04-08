import datetime
import sqlite3

import dearpygui.dearpygui as dpg

from gui.fundChart import FundChart
from gui.fundTab import FundTab
from gui.fundTable import FundTable
from frappeController import FrappeController
from gui.fundTree import FundTree
from logger import CustomLogger
import pandas as pd

# config 파일 불러오기
import json
with open('config.json') as f:
    config = json.load(f)

# 로그 실행
logger = CustomLogger()

# # 연환산수익률 계산
# def cal_annualized_rate(fund_df):
#     # 가장 최근 데이터
#     latest = fund_df.iloc[-1]
#
#     # 가장 과거 데이터
#     first = fund_df.iloc[0]
#
#     # 기간 계산
#     period = (latest['AsOfDate'] - first['AsOfDate']).days
#
#     # 기간 수익률
#     rev = float((latest['AdjustedNAV'] - first['AdjustedNAV']) / first['AdjustedNAV'])
#
#     # 연도 계산
#     year = period / 365
#
#     return (1 + rev) ** (1 / year) - 1


class FrappeApp:
    controller: FrappeController = None
    target_date: str = None

    def __init__(self):
        self.controller = FrappeController()
        self.target_date = None

    def run(self):
        # RA process 시각화 버튼 id
        button_id_list = []
        for i in range(6):
            button_id_list.append(dpg.generate_uuid())

        # RA process 리셋
        def reset_process_callback(sender, app_data, user_data):
            # process tab을 삭제하기 위해 id로 접근하기
            app = user_data["app"]
            main = dpg.get_item_children(app, slot=1)[1]
            main_child = dpg.get_item_children(main, slot=1)

            # 첫번째 child는 메뉴바이므로, 두번째 child부터 삭제
            for idx in range(1, len(main_child)):
                dpg.delete_item(main_child[idx])

            # 단계를 진행할 버튼 다시 활성화
            for i in range(len(button_id_list)):
                dpg.configure_item(button_id_list[i], show=True)

            logger.log_warning("모든 프로세스를 리셋했습니다.")

        # 데이터가 군데군데 불러와지는 경우가 있어서 추가
        def load_recent_fund_callback(sender, app_data, user_data):
            symbol_tuple = tuple(self.controller.fund_df['total_fund_df']['asset_id'])

            with sqlite3.connect(self.controller.local_db_file) as conn:
                logger.log_warning("펀드들의 최신 Trading 데이터를 가져옵니다.")
                self.controller.dump_fund_trading_data(conn, symbol_tuple)
            logger.log_warning("최신 Trading 데이터 로딩 끝")

        def load_recent_bm_callback(sender, app_data, user_data):
            bm_symbol_tuple = tuple(config["ASSET_CLASS_MAP"].values())

            with sqlite3.connect(self.controller.local_db_file) as conn:
                logger.log_warning("BM 지표들의 최신 데이터를 가져옵니다.")
                self.controller.dump_bm_price_data(conn, bm_symbol_tuple)
            logger.log_warning("최신 BM 데이터 로딩 끝")

        # 화면 구성
        with dpg.window(label="Robo", width=1300, height=1000, pos=[0, 0]) as app:
            # 메뉴바
            with dpg.menu_bar():
                dpg.add_menu_item(label="RA GUI")
                with dpg.menu(label="LOG"):
                    dpg.add_menu_item(label="Hide", callback=lambda: dpg.hide_item(logger.window_id))
                    dpg.add_menu_item(label="Call", callback=lambda: dpg.show_item(logger.window_id))

                with dpg.menu(label="Process"):
                    dpg.add_menu_item(label="Reset", callback=reset_process_callback, user_data={"app": app})
                    with dpg.menu(label="Load Recent data"):
                        dpg.add_menu_item(label="Fund", callback=load_recent_fund_callback)
                        dpg.add_menu_item(label="BM", callback=load_recent_bm_callback)

            # MAIN 화면-날짜 선택 및 프로세스 전체 진행
            now = datetime.datetime.now().strftime("%Y-%m-%d")
            input_id = dpg.add_input_text(label='날짜 입력', width=300, pos=[300, 100], hint='YYYY-MM-DD 형식으로 입력해주세요.',
                                          default_value=now)

            radio_id = dpg.add_radio_button(pos=[300, 130], items=["ALL"]+list(config["RISK_TYPE"].keys()), default_value='ALL')

            now = datetime.datetime.now()
            default_value = {'sec': 0, 'min': 0, 'hour': 0,
                             'month_day': now.day, 'month': now.month - 1, 'year': now.year - 1900,
                             'week_day': 4, 'year_day': 7, 'daylight_savings': 0}
            dpg.add_date_picker(label='날짜고르기', pos=[700, 100], callback=self.get_date_callback,
                                default_value=default_value, user_data={"input_id": input_id})

            dpg.add_button(label='Process', pos=[600, 300], callback=self.process_start_callback,
                           user_data={
                               "button_id_list": button_id_list,
                               "main_window": app,
                               "input_id": input_id,
                               "radio_id": radio_id})

        # 화면 가득 채우게
        dpg.set_primary_window(app, True)

        self.change_to_korean()
        dpg.start_dearpygui()

    def get_date_callback(self, sender, app_data, user_data):
        date_dict = dpg.get_value(sender)
        # print(date_dict)
        day = date_dict['month_day']
        month = date_dict['month'] + 1
        year = date_dict['year'] + 1900
        date = f"{year}-{month:02d}-{day:02d}"
        dpg.set_value(user_data["input_id"], date)

    # RA 프로세스 진행
    def process_start_callback(self, sender, app_data, user_data):
        button_id_list = user_data["button_id_list"]
        main_id = user_data["main_window"]
        radio_id = user_data["radio_id"]
        input_id = user_data["input_id"]
        self.target_date = dpg.get_value(input_id)
        radio_key = dpg.get_value(radio_id)

        # 기존에 있는 탭은 삭제
        child_list = dpg.get_item_children(main_id, slot=1)
        if len(child_list) > 5:
            for child_id in child_list[6:]:
                dpg.delete_item(child_id)

        # 프로세스 바
        with dpg.group(label='RA process', parent=main_id):
            with dpg.theme() as theme:
                dpg.add_theme_color(dpg.mvThemeCol_PlotHistogram, (0, 204, 102))
            progress_bar = dpg.add_progress_bar(pos=[210, 350], default_value=0.0, overlay="0%")
            dpg.set_item_theme(dpg.last_item(), theme)

        # 올바른 데이터 타입을 받았는지 확인
        try:
            # 미래 날짜를 선택하면 에러 던지기
            # TODO: 너무 과거 날짜는?
            if datetime.datetime.strptime(self.target_date, "%Y-%m-%d") > datetime.datetime.now():
                raise ValueError
        except ValueError:
            logger.log_critical("날짜를 제대로 입력해주세요.")
            # 잘못된 값이면 입력창 초기화
            dpg.set_value(input_id, '')
            return

        # ---날짜를 제대로 입력 받았으면 해당 날짜를 타겟으로 RA 프로세스 진행
        # 전체 펀드 불러오기
        logger.log("전체 펀드 유니버스 로딩")
        total_fund_df = self.controller.load_funds_info(self.controller.customer_db_adaptor, self.target_date)
        self.controller.fund_df["total_fund_df"] = total_fund_df
        dpg.configure_item(progress_bar, default_value=0.16, overlay="16%")

        # screen 단계
        logger.log("Screening 시작. 끝날 때까지 기다려주세요.")
        selected_fund_df = self.controller.screening(self.controller.fund_df["total_fund_df"], self.target_date)
        self.controller.fund_df["selected_fund_df"] = selected_fund_df
        logger.log("Screening 끝")
        dpg.configure_item(progress_bar, default_value=0.32, overlay="32%")

        # preselect 단계
        logger.log("Pre-Selection 시작. 끝날 때까지 기다려주세요.")
        preselected_fund_df = self.controller.preselecting(self.controller.fund_df["selected_fund_df"],
                                                           self.target_date)

        self.controller.fund_df["preselected_fund_df"] = preselected_fund_df
        logger.log("Pre-Selection 끝.")
        dpg.configure_item(progress_bar, default_value=0.5, overlay="52%")

        # postselect 단계
        logger.log("Post-Selection 시작. 끝날 때까지 기다려주세요.")
        asset_class_top_5_df = preselected_fund_df.sort_values(by="period_return", ascending=False).groupby(
            "asset_class_symbol").head(5)
        asset_class_top_5_df = asset_class_top_5_df.sort_values(by=["asset_class_symbol", "period_return"],
                                                                ascending=[False, False])

        self.controller.fund_df["postselected_fund_df"] = asset_class_top_5_df
        logger.log("Post-Selection 끝.")
        dpg.configure_item(progress_bar, default_value=0.66, overlay="68%")

        # weighting, allocation 단계
        logger.log("Allocation 시작. 끝날 때까지 기다려주세요.")
        postselected_fund_df = self.controller.fund_df["postselected_fund_df"]

        # 사용자가 선택한 위험 유형
        user_risk_type = config["RISK_TYPE"] if radio_key == 'ALL' else {radio_key: config["RISK_TYPE"][radio_key]}

        weight_by_risk = self.controller.weighting(self.target_date, user_risk_type)
        portfolio_by_risk = self.controller.select_portfolio(postselected_fund_df, weight_by_risk)

        self.controller.fund_df["weight_by_risk"] = weight_by_risk
        self.controller.fund_df["portfolio_by_risk"] = portfolio_by_risk
        logger.log("Allocation 끝.")
        dpg.configure_item(progress_bar, default_value=0.82, overlay="84%")

        # correction 단계
        logger.log("Correction 시작. 끝날 때까지 기다려주세요.")
        postselected_fund_df = self.controller.fund_df["postselected_fund_df"]
        new_weight_by_risk, new_portfolio_by_risk = self.controller.correcting(postselected_fund_df, weight_by_risk)

        self.controller.fund_df["new_weight_by_risk"] = new_weight_by_risk
        self.controller.fund_df["new_portfolio_by_risk"] = new_portfolio_by_risk
        logger.log("Correction 끝.")
        dpg.configure_item(progress_bar, default_value=1, overlay="100%")

        with dpg.group(label='RA process', parent=main_id):
            main_tab_id = dpg.generate_uuid()
            tab = FundTab(self.controller)
            user_data = {"parent": main_tab_id, "target_date": self.target_date}

            # 프로세스 버튼들
            button_id_list[0] = dpg.add_button(label='Screen', pos=[250, 380], callback=tab.screen_tab_callback, user_data=user_data)
            button_id_list[1] = dpg.add_button(label='Categorization', pos=[360, 380], callback=tab.categorization_tab_callback, user_data=user_data)
            button_id_list[2] = dpg.add_button(label='Pre-selection', pos=[520, 380], callback=tab.preselect_tab_callback, user_data=user_data)
            button_id_list[3] = dpg.add_button(label='Post-selection', pos=[680, 380], callback=tab.postselect_tab_callback, user_data=user_data)
            button_id_list[4] = dpg.add_button(label='Allocation', pos=[830, 380], callback=tab.allocation_tab_callback, user_data=user_data)
            button_id_list[5] = dpg.add_button(label='Correction', pos=[950, 380], callback=tab.correction_tab_callback, user_data=user_data)

            # 탭 그리기
            tab.draw_tab_window(self.target_date, main_tab_id)

    # 한글 글꼴로 바꾸기
    def change_to_korean(self):
        with dpg.font_registry():
            f = "./font/malgun.ttf"
            with dpg.font(file=f, size=16, default_font=True):
                dpg.add_font_range_hint(dpg.mvFontRangeHint_Korean)


if __name__ == '__main__':
    app = FrappeApp()
    app.run()
