import math
import pandas as pd

import datetime
from dateutil.relativedelta import relativedelta

import dearpygui.dearpygui as dpg

from logger import CustomLogger
from gui.frappeComponent import FrappeComponent


logger = CustomLogger()


class FundChart(FrappeComponent):
    # 펀드 차트 시각화
    def draw_chart(self, user_data):
        # x, y축 ID 발급
        xaxis = dpg.generate_uuid()
        yaxis = dpg.generate_uuid()

        # bm 그래프 ID 발급(etc class를 위해 2개 발급)
        # 0: KR_STOCK, 1: DM_STOCK
        # TODO: map으로?
        bm_line = [dpg.generate_uuid(), dpg.generate_uuid()]

        # 펀드 symbol, name, 자산군 정보 받기
        target_date = user_data['target_date']
        asset_id = user_data['asset_id']
        name = user_data['asset_name']
        asset_class = user_data['asset_class_symbol']

        with dpg.window(label=name, width=1200, height=800, on_close=self.close_callback):
            # LOG: asset_id 출력
            logger.log_info(f"Click: {name} ({asset_id})")

            ## 펀드 정보 보여주기
            # asset_id를 이용해서 데이터 불러오기
            trade_df = self.controller.get_fund_trade_df(asset_id, target_date)

            # ## 그래프 그리기 준비
            # bm 데이터 df 불러와서 fund 데이터와 합치기
            bm_df, bm_name = self.controller.get_bm_price_df(asset_class, target_date)
            bm_fund_df = self.controller.concat_fund_bm_df(trade_df, bm_df)

            # TODO: pre-selection 이후에 누르는거면 낭비임
            # 펀드와 BM의 상관계수 구하기
            if asset_class == "ETC":
                fund_bm_corr = "N/A"
                # column명 접근을 위해
                asset_class_li = ["KR_STOCK", "DM_STOCK"]
            else:
                # 계산에 필요한 column 남기기
                # corr_df = bm_fund_df[['AdjustedNAV', asset_class]].astype('float')
                corr_df = bm_fund_df[['AdjustedNAV', asset_class]]
                # 상관계수 계산
                corr_df = self.controller.cal_spearman_corr(corr_df)
                fund_bm_corr = corr_df.iloc[0][1]

                # column명 접근을 위해
                asset_class_li = [asset_class]

            # index를 x좌표로 쓰기 위해서 원래 index였던 AsOfDate를 컬럼에 넣기
            bm_fund_df = bm_fund_df.reset_index()
            bm_fund_df = bm_fund_df.rename(columns={'index': 'AsOfDate'})

            # 날짜 표시용
            dates = bm_fund_df['AsOfDate'].astype('str').to_list()

            # fund 그래프용 x, y
            # bm_fund_df = bm_fund_df.dropna(axis=0)
            fund_x = bm_fund_df.index.values.astype('int').tolist()
            fund_y = bm_fund_df['AdjustedNAV'].values.astype('float').tolist()

            # bm 그래프용 x, y
            bm_x = bm_fund_df.index.values.astype('int').tolist()
            bm_y = self.change_bm_y(asset_class_li, bm_fund_df, fund_y)
            # bm_y = {}
            # for i in range(len(asset_class_li)):
            #     # bm 가격 그대로 출력: key값은 bm_name, value는 해당 bm의 price list
            #     bm_y[asset_class_li[i]] = bm_fund_df[asset_class_li[i]].values.astype('float').tolist()

            # x와 date pair 만들기(날짜 표시용)
            date_label = []
            for label, x in zip(dates, bm_x):
                date_label.append((label, x))

            # index인 AsOfDate를 column으로 바꾸기
            trade_df = trade_df.reset_index()
            # 가장 최근 데이터
            latest = trade_df.iloc[-1]

            # 기준가 및 전일대비 정보
            with dpg.group(pos=[100, 50]):
                # 전일대비 수익 계산
                diff = float(trade_df[-2:]['NAV'].diff().dropna())
                # 전일대비 수익률 계산
                diff_per = float(trade_df[-2:]['NAV'].pct_change().dropna()) * 100
                dpg.add_text(f"{latest['NAV']:.2f}", color=[255, 0, 0] if diff >= 0 else [3, 78, 162])
                dpg.add_same_line(spacing=5)
                date = latest['AsOfDate'].strftime('%Y-%m-%d')
                dpg.add_text(f"기준가 | {date}")
                dpg.add_text("전일대비:")
                dpg.add_same_line(spacing=5)
                dpg.add_text(f"{diff:.2f}({diff_per:.2f}%)", color=[255, 0, 0] if diff >= 0 else [3, 78, 162])

            # 기간별 수익률
            with dpg.group(pos=[400, 50]):
                # 주말은 포함 X 마지막에서 날짜 계산
                dpg.add_text(f"올해 수익률: {self.cal_this_year_rate(trade_df):.2f}%")
                dpg.add_same_line(spacing=20)
                dpg.add_text(f"1개월 수익률: {self.cal_yield(trade_df, 'months', 1)}")
                dpg.add_same_line(spacing=20)
                dpg.add_text(f"3개월 수익률: {self.cal_yield(trade_df, 'months', 3)}")
                dpg.add_text(f"1년 수익률: {self.cal_yield(trade_df, 'years', 1)}")
                dpg.add_same_line(spacing=20)
                dpg.add_text(f"3년 수익률: {self.cal_yield(trade_df, 'years', 3)}")
                dpg.add_same_line(spacing=20)
                dpg.add_text(f"상관 계수: {fund_bm_corr}")

            with dpg.group(horizontal=True, pos=[850, 110]):
                dpg.add_button(label="fit x", callback=self.fix_x_callback,
                               user_data={"date_label": date_label, "fund_x": fund_x, "xaxis": xaxis})
                dpg.add_button(label="fit y",
                               callback=lambda: dpg.set_axis_limits(yaxis, min(fund_y) - 300, max(fund_y) + 300))
                dpg.add_button(label="unlock x limits", callback=lambda: dpg.set_axis_limits_auto(xaxis))
                dpg.add_button(label="unlock y limits", callback=lambda: dpg.set_axis_limits_auto(yaxis))

            # 기간 조절 버튼
            user_data = {
                "bm_fund_df": bm_fund_df,
                "date_label": date_label,
                "xaxis": xaxis,
                "yaxis": yaxis,
                "asset_class_li": asset_class_li,
                "bm_line": bm_line,
            }
            with dpg.group(horizontal=True, pos=[400, 110]):
                dpg.add_button(label="1개월", callback=self.period_button_callback,
                               user_data=user_data | {"period": 1, "unit": "months"})
                dpg.add_button(label="3개월", callback=self.period_button_callback,
                               user_data=user_data | {"period": 3, "unit": "months"})
                dpg.add_button(label="1년", callback=self.period_button_callback,
                               user_data=user_data | {"period": 1, "unit": "years"})
                dpg.add_button(label="5년", callback=self.period_button_callback,
                               user_data=user_data | {"period": 5, "unit": "years"})
                dpg.add_button(label="10년", callback=self.period_button_callback,
                               user_data=user_data | {"period": 10, "unit": "years"})

            # 그래프
            with dpg.plot(label=name, width=1000, height=500, pos=[100, 140]):
                dpg.add_plot_legend()

                # x축
                dpg.add_plot_axis(dpg.mvXAxis, label="x", id=xaxis)
                dpg.set_axis_limits(xaxis, min(fund_x), max(fund_x))  # x축의 최소,최대는 index의 범위

                # x축 날짜 라벨
                self.set_custom_x_axis_ticks(xaxis, date_label[min(fund_x):])

                # y축
                dpg.add_plot_axis(dpg.mvYAxis, label="y", id=yaxis)
                dpg.set_axis_limits(dpg.last_item(), min(fund_y), max(fund_y))

                # 데이터 그리기
                dpg.add_line_series(fund_x, fund_y, label="FUND", parent=yaxis)

                for idx in range(len(bm_y)):
                    # trade 와 bm data의 offset 계산
                    bm_y_li = bm_y[asset_class_li[idx]]
                    offset = fund_y[0] - bm_y_li[0]
                    move_bm_y = [bm_y_li[i] + offset for i in range(len(bm_y_li))]

                    dpg.add_line_series(bm_x, move_bm_y, label=bm_name[idx], parent=yaxis, id=bm_line[idx])

                # TODO: 최고, 최저 표시
                # dpg.add_drag_point
                # dpg.add_text_point(x=0, y=0,label="test")
                # dpg.add_text_point(x=0, y=10,label="test2")

            # 펀드 투자 정보
            with dpg.group(pos=[100, 650]):
                dpg.add_text("펀드 투자 정보")
                dpg.add_text(f"총 설정액: {latest['AUM'] / (10 ** 8):.0f}억원")
                dpg.add_text(f"순 자산액: {latest['NetAssets'] / (10 ** 8):.0f}억원")
                company_df = self.controller.load_fund_company_info(self.controller.price_db_adaptor, latest['CompanyCode'])
                dpg.add_text(f"운용사: {company_df['Name'][0]}")

    # 수익률 계산
    def cal_yield(self, df: pd.DataFrame, unit: str, period: int):
        # 가장 최근 데이터
        latest = df.iloc[-1]

        # TODO: 영업일이 아닐수 있음 -> 전으로 가는게 맞다 1. 데이터를 채워서 가져오거나, 전꺼 찾거나, 지금 방법은??
        try:
            if unit == "years":
                target_date = latest['AsOfDate'] - relativedelta(years=period)
                target_nav = df[df['AsOfDate'] <= target_date].iloc[-1]
            elif unit == "months":
                target_date = latest['AsOfDate'] - relativedelta(months=period)
                target_nav = df[df['AsOfDate'] <= target_date].iloc[-1]
        except IndexError:
            # 해당 날짜 데이터가 없어서 수익률을 구할 수 없을 때
            return "N/A"

        before = target_nav['AdjustedNAV']
        latest = latest['AdjustedNAV']
        return f"{float((latest - before) / before * 100):.2f}%"

    # 올해 수익률 계산
    def cal_this_year_rate(self, fund_df: pd.DataFrame) -> float:
        now = datetime.datetime.now()
        new_year = now.replace(month=1, day=1)

        # 현재 data
        now = fund_df.iloc[-1]
        now_nav = now['AdjustedNAV']

        # 작년 마지막날 data
        # TODO: 근데 1월 1일이 없으면 작년 12월걸로 들어감. 올해 첫 영업일 계산???
        new_year = fund_df[fund_df['AsOfDate'] <= new_year].iloc[-1]
        new_year_nav = new_year['AdjustedNAV']

        return float((now_nav - new_year_nav) / new_year_nav) * 100

    # 날짜 label 조절
    def set_custom_x_axis_ticks(self, xaxis: int, x_pair: list):
        # 날짜는 5개만 보여주게
        num = len(x_pair)
        show_list = []
        for i in range(num):
            if i % math.ceil(num / 5) == 0:
                show_list.append(x_pair[i])
            if len(show_list) == 5:
                break

        # 날짜 label
        dpg.set_axis_ticks(xaxis, tuple(show_list))

    # 그래프 크기 조절 버튼
    def fix_x_callback(self, sender, app_data, user_data):
        fund_x = user_data["fund_x"]
        date_label = user_data["date_label"]
        xaxis = user_data["xaxis"]

        # xaxis에서 최대, 최소 limit으로 fit
        dpg.set_axis_limits(xaxis, min(fund_x), max(fund_x))

        # 날짜 label 조절
        self.set_custom_x_axis_ticks(xaxis, date_label[min(fund_x):])

    # TODO: 흠..
    # 기간에 따라 그래프 fix callback
    def period_button_callback(self, sender, app_data, user_data):
        # 해당 기간 만큼의 인덱스 찾기(최소)
        bm_fund_df = user_data["bm_fund_df"]
        unit = user_data["unit"]
        period = user_data["period"]
        date_label = user_data["date_label"]
        xaxis = user_data["xaxis"]
        yaxis = user_data["yaxis"]
        asset_class_li = user_data["asset_class_li"]
        bm_line = user_data["bm_line"]

        # 가장 최근 데이터
        latest = bm_fund_df.iloc[-1]

        # TODO: 흠..
        if unit == "years":
            min_date = latest['AsOfDate'] - relativedelta(years=period)
        elif unit == "months":
            min_date = latest['AsOfDate'] - relativedelta(months=period)

        # 기간넘치면 가장 과거 데이터로 가져오기
        if min_date < bm_fund_df.iloc[0]['AsOfDate']:
            min_date = bm_fund_df.iloc[0]['AsOfDate']

        # 해당 범위에 맞게 df 준비
        range_bm_fund_df = bm_fund_df[bm_fund_df['AsOfDate'] >= min_date]

        # 날짜에 맞는 펀드 x,y 데이터
        period_x = range_bm_fund_df.index.values.astype('int').tolist()
        period_fund_y = range_bm_fund_df['AdjustedNAV'].values.astype('float').tolist()

        # 날짜에 맞는 bm y 데이터
        move_bm_y = self.change_bm_y(asset_class_li, range_bm_fund_df, period_fund_y)
        for idx, (_, elem) in enumerate(move_bm_y.items()):
            dpg.configure_item(item=bm_line[idx], x=period_x, y=elem)

        # for i in range(len(asset_class_li)):
        #     bm_y = bm_fund_df[asset_class_li[i]].values.astype('float').tolist()
        #
        #     offset = period_fund_y[0] - range_bm_fund_df[asset_class_li[i]].values.astype('float').tolist()[0]
        #     move_bm_y = [bm_y[i] + offset for i in range(len(bm_y))]
        #     dpg.configure_item(bm_line[i], y=move_bm_y)

        # x, y 범위 조절
        dpg.set_axis_limits(xaxis, min(period_x), max(period_x))
        dpg.set_axis_limits(yaxis, min(period_fund_y) - 300, max(period_fund_y) + 300)

        # 날짜 label 조절
        self.set_custom_x_axis_ticks(xaxis, date_label[min(period_x):])

    # bm 변동 가격을 펀드 가격 기준으로 계산
    def change_bm_y(self, asset_class_li: list, bm_df: pd.DataFrame, fund_y: list) -> dict:
        bm_y = {}
        for i in range(len(asset_class_li)):
            # 현재 bm 가격 = (bm 수익률 + 1) * 전일 bm 가격 (단, bm 가격의 가장 처음값은 펀드 가격 처음값과 동일)
            change_bm_df = bm_df[asset_class_li[i]].pct_change().fillna(0)
            change_bm_li = change_bm_df.values.astype('float').tolist()
            for j in range(len(fund_y)):
                change_bm_li[j] = (change_bm_li[j] + 1) * change_bm_li[j - 1] if j != 0 else fund_y[0]
            bm_y[asset_class_li[i]] = change_bm_li
        return bm_y

    # callback을 호출한 item을 삭제
    def close_callback(self, sender):
        dpg.delete_item(sender)
