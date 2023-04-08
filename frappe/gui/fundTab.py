import sqlite3

import dearpygui.dearpygui as dpg

from frappeController import FrappeController
from gui.frappeComponent import FrappeComponent
from gui.fundTable import FundTable
from gui.fundTree import FundTree
from logger import CustomLogger

# 로그 실행
logger = CustomLogger()


class FundTab(FrappeComponent):
    def draw_tab_window(self, target_date: str, main_tab_id: int):
        # 탭 바
        with dpg.tab_bar(id=main_tab_id, label="RA엔진 프로세스", pos=[0, 450]):
            # 전체 펀드 출력
            with dpg.tab(label="전체 펀드 유니버스"):
                logger.log("전체 펀드 유니버스 로딩")

                # 전체 펀드 불러오기
                total_fund_df = self.controller.fund_df["total_fund_df"]

                # 전체 펀드 중 특정 column 추출
                total_fund_df = total_fund_df[
                    ['asset_id', 'asset_name', 'risk_type_name', 'asset_class_name', 'asset_class_symbol',
                     'investment_area_name', 'fund_bm_name']]

                # 시각화
                with dpg.group():
                    dpg.add_text(f"업데이트 날짜: {target_date}")
                    dpg.add_same_line(spacing=20)
                    dpg.add_text(f"펀드 개수: {len(total_fund_df)}")
                    dpg.add_same_line(spacing=20)
                total_table = FundTable(self.controller)
                total_table.draw_table(total_fund_df, target_date)

    # ------- 단계별 시각화 callback
    def screen_tab_callback(self, sender, app_data, user_data):
        dpg.configure_item(sender, show=False)

        parent = user_data["parent"]
        target_date = user_data["target_date"]

        # controller에서 데이터 가져오기
        selected_fund_df = self.controller.fund_df["selected_fund_df"][
            ['asset_id', 'asset_name', 'risk_type_name', 'asset_class_name', 'asset_class_symbol',
             'investment_area_name', 'fund_bm_name']]

        # 시각화
        with dpg.tab(label="After Screening", parent=parent) as screen:
            with dpg.group():
                dpg.add_text(f"업데이트 날짜: {target_date}")
                dpg.add_same_line(spacing=20)
                dpg.add_text(f"펀드 개수: {len(selected_fund_df)}")
                dpg.add_same_line(spacing=20)
            screened_table = FundTable(self.controller)
            screened_table.draw_table(selected_fund_df, target_date)

    def categorization_tab_callback(self, sender, app_data, user_data):
        dpg.configure_item(sender, show=False)

        parent = user_data["parent"]
        target_date = user_data["target_date"]

        # controller에서 데이터 가져오기
        selected_fund_df = self.controller.fund_df["selected_fund_df"][
            ['asset_id', 'asset_name', 'risk_type_name', 'asset_class_name', 'asset_class_symbol',
             'investment_area_name', 'fund_bm_name']]

        # 시각화
        with dpg.tab(label="Categorization", parent=parent):
            with dpg.group():
                dpg.add_text(f"업데이트 날짜: {target_date}")
                dpg.add_same_line(spacing=20)
                dpg.add_text(f"펀드 개수: {len(selected_fund_df)}")
                dpg.add_same_line(spacing=20)
            category_tree = FundTree(self.controller)
            category_tree.draw_fund_tree(selected_fund_df, target_date)

    def preselect_tab_callback(self, sender, app_data, user_data):
        dpg.configure_item(sender, show=False)

        parent = user_data["parent"]
        target_date = user_data["target_date"]

        # controller에서 데이터 가져오기
        preselected_fund_df = self.controller.fund_df["preselected_fund_df"]

        # 시각화
        with dpg.tab(label="Pre-Selection", parent=parent):
            with dpg.group():
                dpg.add_text(f"업데이트 날짜: {target_date}")
                dpg.add_same_line(spacing=20)
                dpg.add_text(f"펀드 개수: {len(preselected_fund_df)}")
                dpg.add_same_line(spacing=20)
            preselect_tree = FundTree(self.controller)
            preselect_tree.draw_fund_tree(preselected_fund_df, target_date)

    def postselect_tab_callback(self, sender, app_data, user_data):
        dpg.configure_item(sender, show=False)

        parent = user_data["parent"]
        target_date = user_data["target_date"]

        # controller에서 데이터 가져오기
        asset_class_top_5_df = self.controller.fund_df["postselected_fund_df"]

        # 시각화
        with dpg.tab(label="Post-Selection", parent=parent):
            with dpg.group():
                dpg.add_text(f"업데이트 날짜: {target_date}")
                dpg.add_same_line(spacing=20)
                dpg.add_text(f"펀드 개수: {len(asset_class_top_5_df)}")
                dpg.add_same_line(spacing=20)
            postselect_tree = FundTree(self.controller)
            postselect_tree.draw_fund_tree(asset_class_top_5_df, target_date)

    def allocation_tab_callback(self, sender, app_data, user_data):
        dpg.configure_item(sender, show=False)

        parent = user_data["parent"]
        target_date = user_data["target_date"]

        # controller에서 데이터 가져오기
        weight_by_risk = self.controller.fund_df["weight_by_risk"]
        portfolio_by_risk = self.controller.fund_df["portfolio_by_risk"]

        # 시각화
        with dpg.tab(label="Allocation", parent=parent):
            with dpg.group():
                dpg.add_text(f"업데이트 날짜: {target_date}")
                dpg.add_same_line(spacing=20)

            allocation_tree = FundTree(self.controller)
            allocation_tree.draw_portfolio_tree(weight_by_risk, portfolio_by_risk, target_date)

    def correction_tab_callback(self, sender, app_data, user_data):
        dpg.configure_item(sender, show=False)

        parent = user_data["parent"]
        target_date = user_data["target_date"]

        # controller에서 데이터 가져오기
        new_weight_by_risk = self.controller.fund_df["new_weight_by_risk"]
        new_portfolio_by_risk = self.controller.fund_df["new_portfolio_by_risk"]

        # 시각화
        with dpg.tab(label="Correction", parent=parent):
            with dpg.group():
                dpg.add_text(f"업데이트 날짜: {target_date}")
                dpg.add_same_line(spacing=20)
                # dpg.add_button(label="Correction", callback=self.correction_tab_callback,
                #                user_data={"parent": parent, "date": date})

            allocation_tree = FundTree(self.controller)
            allocation_tree.draw_portfolio_tree(new_weight_by_risk, new_portfolio_by_risk, target_date)