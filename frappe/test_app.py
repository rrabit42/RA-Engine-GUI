from frappeController import FrappeController

if __name__ == '__main__':
    controller = FrappeController()
    target_date = '2021-08-09'

    # 전체 펀드 불러오기
    total_fund_df = controller.load_funds_info(controller.customer_db_adaptor, target_date)
    controller.fund_df["total_fund_df"] = total_fund_df

    # screen 단계
    selected_fund_df = controller.screening(controller.fund_df["total_fund_df"], target_date)
    controller.fund_df["selected_fund_df"] = selected_fund_df

    # preselect 단계
    preselected_fund_df = controller.preselecting(controller.fund_df["selected_fund_df"], target_date)
    controller.fund_df["preselected_fund_df"] = preselected_fund_df

    # postselect 단계
    asset_class_top_5_df = preselected_fund_df.sort_values(by="period_return", ascending=False).groupby(
        "asset_class_symbol").head(5)
    asset_class_top_5_df = asset_class_top_5_df.sort_values(by=["asset_class_symbol", "period_return"],
                                                            ascending=[False, False])
    controller.fund_df["postselected_fund_df"] = asset_class_top_5_df

    # weighting, allocation 단계
    postselected_fund_df = controller.fund_df["postselected_fund_df"]
    weight_by_risk = controller.weighting(target_date)
    portfolio_by_risk = controller.select_portfolio(postselected_fund_df, weight_by_risk)

    # correction 단계
    new_weight_by_risk, new_portfolio_by_risk = controller.correcting(postselected_fund_df, weight_by_risk)
    print("끝")
