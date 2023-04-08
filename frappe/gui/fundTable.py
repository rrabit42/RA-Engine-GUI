import dearpygui.dearpygui as dpg

# 테이블 형식으로 시각화
import pandas as pd

from gui.fundChart import FundChart
from gui.frappeComponent import FrappeComponent


class FundTable(FrappeComponent):
    # 표 시각화
    def draw_table(self, fund_df: pd.DataFrame, target_date: str, show_chart: bool=True):
        # TODO: sort되게 바꾸기 왜 안되니!!!!!!!
        with dpg.table(header_row=True, policy=dpg.mvTable_SizingFixedFit, row_background=True, reorderable=True,
                       resizable=True, no_host_extendX=False, hideable=True, precise_widths=True,
                       borders_innerV=True, delay_search=True, borders_outerV=True, borders_innerH=True,
                       borders_outerH=True, sort_multi=True, sortable=True):
            for col_name in fund_df.columns:
                dpg.add_table_column(label=col_name, width_fixed=True, no_header_width=False, default_sort=True)

            for row in range(fund_df.shape[0]):
                for col in range(fund_df.shape[1]):
                    cell = dpg.add_text(fund_df.iloc[row][col])
                    if show_chart:
                        dpg.add_clicked_handler(cell, callback=self.chart_callback, user_data={
                            'target_date': target_date,
                            'asset_id': fund_df.iloc[row]['asset_id'],
                            'asset_name': fund_df.iloc[row]['asset_name'],
                            'asset_class_symbol': fund_df.iloc[row]['asset_class_symbol']
                        })

                    if not (row == fund_df.shape[0] and col == fund_df.shape[1]):
                        dpg.add_table_next_column()

    # 차트 콜백 함수
    def chart_callback(self, sender, app_data, user_data):
        chart = FundChart(self.controller)
        chart.draw_chart(user_data)

