import dearpygui.dearpygui as dpg
import pandas as pd

from gui.frappeComponent import FrappeComponent
from gui.fundChart import FundChart
from gui.fundTable import FundTable


class FundTree(FrappeComponent):
    # 펀드 트리 시각화
    def draw_fund_tree(self, fund_df: pd.DataFrame, target_date: str):
        # 자산군으로 펀드들 그룹화
        asset_class_set = fund_df.groupby(fund_df['asset_class_symbol'])

        # 채권, 주식, 그 외로 자산군 이름 분류
        bond_classes = []
        stock_classes = []
        etc_classes = []
        bond_fund_df = pd.DataFrame()
        stock_fund_df = pd.DataFrame()
        etc_fund_df = pd.DataFrame()

        for asset_class, asset_funds in asset_class_set:
            if 'BOND' in asset_class:
                # 이름 분류 -> key 로 list 만들기
                bond_classes.append(asset_class)
                # 펀드들 분류 -> group append
                bond_fund_df = bond_fund_df.combine_first(asset_funds)
            elif 'STOCK' in asset_class or 'GOLD' in asset_class:
                stock_classes.append(asset_class)
                stock_fund_df = stock_fund_df.combine_first(asset_funds)
            else:   # ETC
                etc_classes.append(asset_class)
                etc_fund_df = etc_fund_df.combine_first(asset_funds)

        # 트리 출력
        # TODO: for??
        with dpg.tree_node(label=f"주식\t\t총 {len(stock_fund_df)}개 펀드", default_open=True):
            self.draw_child_tree(stock_fund_df, sorted(stock_classes), target_date)

        with dpg.tree_node(label=f"채권\t\t총 {len(bond_fund_df)}개 펀드", default_open=True):
            self.draw_child_tree(bond_fund_df, sorted(bond_classes), target_date)

        with dpg.tree_node(label=f"기타\t\t총 {len(etc_fund_df)}개 펀드"):
            self.draw_child_tree(etc_fund_df, sorted(etc_classes), target_date)

    # 세부 자산군 별 트리 그리기
    def draw_child_tree(self, fund_df: pd.DataFrame, asset_classes: list, target_date: str):
        # 자산군 내의 세부 자산군별로 출력
        for asset_cls in asset_classes:
            # 해당 자산의 펀드 집합
            asset_fund_df = fund_df[fund_df['asset_class_symbol'] == asset_cls]
            # 세부 자산 종류, 펀드 개수 출력
            with dpg.tree_node(label=f"{asset_cls}\t {len(asset_fund_df)}개"):
                screened_table = FundTable(self.controller)
                screened_table.draw_table(asset_fund_df, target_date)

    # 위험 성향 별 포트폴리오 시각화
    # TODO: 순서 재배열, 주식 좌르르르르 펀드 좌르르르
    def draw_portfolio_tree(self, weight_by_risk: dict, portfolio_by_risk: dict, target_date: str):
        for risk_type, _ in weight_by_risk.items():
            # with dpg.tree_node(label=risk_type, default_open=True, parent=allocation):
            with dpg.tree_node(label=risk_type):
                # weighting 결과 표
                # TODO: 자료 구조... dict value가 그냥 dataframe이면?
                weight_df = pd.DataFrame(weight_by_risk[risk_type], index=[0])
                weight_table = FundTable(self.controller)
                weight_table.draw_table(weight_df, target_date, show_chart=False)

                # 포트폴리오 표
                dpg.add_text("추천 포트폴리오")
                with dpg.table(header_row=True, policy=dpg.mvTable_SizingFixedFit, row_background=True,
                               reorderable=True,
                               resizable=True, no_host_extendX=False, hideable=True, precise_widths=True,
                               borders_innerV=True, delay_search=True, borders_outerV=True, borders_innerH=True,
                               borders_outerH=True):
                    dpg.add_table_column(label="자산군")
                    dpg.add_table_column(label="펀드 이름")
                    dpg.add_table_column(label=f"비중(합계:{sum(portfolio_by_risk[risk_type]['weight'])})")

                    # TODO: 여기서 정렬 필요
                    res = portfolio_by_risk[risk_type].groupby('asset_class_symbol')
                    for asset_class, fund_group in res:
                        dpg.add_text(asset_class)
                        dpg.add_table_next_column()
                        # 펀드 이름 출력
                        for i in range(len(fund_group)):
                            fund_name = dpg.add_text(fund_group.iloc[i]['asset_name'])
                            dpg.add_clicked_handler(fund_name, callback=self.chart_callback, user_data={
                                'target_date': target_date,
                                'asset_id': fund_group.iloc[i]['asset_id'],
                                'asset_name': fund_group.iloc[i]['asset_name'],
                                'asset_class_symbol': fund_group.iloc[i]['asset_class_symbol']
                            })
                        dpg.add_table_next_column()
                        # 비중 출력
                        for i in range(len(fund_group)):
                            dpg.add_text(f"{fund_group.iloc[i]['weight']}%")
                        dpg.add_table_next_column()

    # 차트 콜백 함수
    def chart_callback(self, sender, app_data, user_data):
        chart = FundChart(self.controller)
        chart.draw_chart(user_data)
