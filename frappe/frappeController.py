import datetime
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
from sqlalchemy.exc import ArgumentError
import pandas as pd
import atexit
import re
import sqlite3
import os, json

from logger import CustomLogger

# config 파일 불러오기
import json
with open('config.json') as f:
    config = json.load(f)

# 로그 실행
logger = CustomLogger()


class FRAFetchResult:
    def __init__(self, columns, data):
        self.columns = columns
        self.data = data

    def df(self):
        return pd.DataFrame(columns=self.columns, data=self.data)

    def scalar(self):
        return self.data[0][0]

    def list(self):
        return list(*zip(*self.data))

    def native(self):
        return self.columns, self.data


class FRADBAdaptor:
    query_set = {}
    engine = None
    conn = None

    def __init__(self, info, pool_recycle=1200, echo=False):
        """
        :param info:
            type dict: db connect information
            type str: uri (uniform resource identifier)
        :param pool_recycle:
        """
        if isinstance(info, dict):
            info = self._generate_uri(**info)

        self.uri = info
        self.pool_recycle = pool_recycle
        self.display_uri = re.sub(r'[^:]*[@$]', '******@', self.uri)
        self.echo = echo

        try:
            if echo:
                logger.info(f"FRADBAdaptor create: {self.display_uri}")
            self.engine = create_engine(info, pool_pre_ping=True, pool_recycle=pool_recycle)
        except KeyError as e:
            raise AttributeError("Need to proper key %s" % str(e))
        except ArgumentError:
            raise ValueError("Incorrect arguments for SQL Manager")

        self.conn = self.engine.connect()
        atexit.register(self.close)

    @staticmethod
    def _generate_uri(**info):
        db_connect_url = '{db_type}://{user}:{password}@{host}:{port}/{database}'
        return db_connect_url.format(**info)

    def get(self, query, *args, **kwargs):
        result = self.engine.execute(query, *args, **kwargs)
        fetch_data = result.fetchall()
        keys = result.keys()
        result.close()
        return FRAFetchResult(keys, fetch_data)

    def save(self, query, *args, **kwargs):
        result = self.engine.execute(query, *args, **kwargs)
        rowcount = result.rowcount
        result.close()
        return rowcount

    def close(self):
        if not self.conn.closed:
            if self.echo:
                logger.info(f"FRADBAdaptor destroy: {self.display_uri}")

            self.conn.close()
            self.engine.dispose()
            self.engine = None

    def __enter__(self):
        return self

    def __exit__(self, t, v, traceback):
        self.close()


class DBAdaptor(FRADBAdaptor):
    def __init__(self, config, pool_recycle=1200, echo=False):
        super().__init__(config, pool_recycle, echo)

        self.max_packet_size = self.get_max_packet_size()

    def get_max_packet_size(self):
        scheme = self.uri.split(':')[0]
        if scheme.lower() == 'sqlite' or scheme.lower() == 'oracle':
            return 102400
        query = "SHOW VARIABLES LIKE 'max_allowed_packet'"
        return int(self.get(query).data[0][1])


class FrappeController:
    local_db_file = 'test.db'
    customer_db_adaptor: DBAdaptor = None
    price_db_adaptor: DBAdaptor = None
    bm_db_adaptor: DBAdaptor = None
    cache_bm_price_dict: dict = {}  # key: asset_class_symbol / value: bm_price_df
    cache_fund_dict: dict = {}  # key: asset_id / value: fund_trade_df

    def __init__(self):
        self.customer_db_adaptor = DBAdaptor(os.environ.get('OAK_DB', ''))
        self.price_db_adaptor = DBAdaptor(os.environ.get('BLUE_DB', ''))
        self.bm_db_adaptor = DBAdaptor(os.environ.get('BM_DB', ''))
        # TODO: 흠 위치...클래스 아니면 인스턴스?
        self.fund_df = {
            "total_fund_df": None,
            "selected_fund_df": None,
            "preselected_fund_df": None
        }

    # Main: 최신 펀드 정보 가져오기
    def load_funds_info(self, db_adaptor: DBAdaptor, target_date: str):
        query = f"""
            SELECT date, asset_id, asset_name, risk_type_name, asset_class_name, asset_class_symbol,
                    investment_area_name, fund_bm_name
                FROM asset_info
                WHERE date = (SELECT MAX(date) FROM asset_info WHERE date <= '{target_date}')
        """
        return db_adaptor.get(query).df()

    # Chart: 펀드 운용사 가져오기
    def load_fund_company_info(self, db_adaptor: DBAdaptor, company_code: str):
        return db_adaptor.get(f"SELECT * FROM Company WHERE Code='{company_code}'").df()

    # Screen: 펀드 출시 후 경과기간이 짧은 펀드 symbol 가져오기
    def load_funds_short_period(self, db_adaptor: DBAdaptor, least_date: datetime, fund_symbol_tuple: tuple):
        query = f"""
            SELECT DISTINCT Symbol, Name
                FROM Operation
                WHERE EndDate IS NULL AND InceptionDate > '{least_date}' AND Symbol in {fund_symbol_tuple}
        """
        return db_adaptor.get(query).df()

    # WEIGHT: macro_score 가져오기
    def load_macro_score(self, db_adaptor: DBAdaptor, target_date: str):
        # 현재의 이전달 데이터 가져오기
        now = datetime.datetime.strptime(target_date, "%Y-%m-%d")
        date = datetime.date(now.year, now.month, 1)
        query = f"""
            SELECT *
            FROM macro_score
            WHERE date = (SELECT MAX(date) FROM macro_score WHERE date < '{date}')
        """
        return db_adaptor.get(query).df()

    # Local db: 연결하기
    def create_conn_sqlite(self):
        conn = None
        try:
            conn = sqlite3.connect(self.local_db_file)  # disk에 db file 만들어짐
            # conn = sqlite3.connect(':memory:') # memory(RAM)에 db file 만들어짐
        except sqlite3.Error as e:
            logger.log_error(e)
        return conn

    # Local db: local과 remote db 데이터들의 max date 가져오기
    def get_local_remote_date(self, conn: sqlite3.Connection, db_adaptor: DBAdaptor, remote_sql: str, local_sql: str):
        remote_max_date = None
        local_max_date = None

        try:
            # remote db에 존재하는 최대 날짜 가져오기
            remote_df = db_adaptor.get(remote_sql).df()
            remote_max_date = remote_df.iloc[0]['max_date'].strftime("%Y-%m-%d")

            # local db에 존재하는 최대 날짜 가져오기
            local_df = pd.read_sql(sql=local_sql, con=conn)
            # 테이블이 비었을 경우 모든 날짜의 데이터를 가져온다
            local_max_date = local_df.iloc[0]['max_date'] if local_df.iloc[0]['max_date'] is not None else '0000-00-00'
        except pd.io.sql.DatabaseError as e:
            logger.log_warning("pandas sql 에러")
            print(e)
        except sqlite3.OperationalError as e:
            logger.log_warning("sqlite 에러")
            print(e)

        return remote_max_date, local_max_date

    # Local db: local db에 bm 가격 데이터 가져오기
    def dump_bm_price_data(self, conn: sqlite3.Connection, bm_symbol_tuple: tuple):
        # local db와 연결
        sqlite_table = 'BM_price'

        remote_sql = f"""
            SELECT MAX(AsOfDate) as max_date
                FROM FTSE
                WHERE Symbol IN {bm_symbol_tuple}
            UNION 
            SELECT MAX(AsOfDate) as max_date
                FROM MerrillLynch
                WHERE Symbol IN {bm_symbol_tuple}
            UNION
            SELECT MAX(AsOfDate) as max_date
                FROM GSCI
                WHERE Symbol IN {bm_symbol_tuple}
        """

        local_sql = f"""
            SELECT MAX(AsOfDate) as max_date
                FROM {sqlite_table}
                WHERE Symbol IN {bm_symbol_tuple}
        """

        # 각 db의 max date 가져오기
        remote_max_date, local_max_date = self.get_local_remote_date(conn, self.bm_db_adaptor, remote_sql, local_sql)

        # sqlite랑 mysql 날짜범위 비교해서 없는 날짜만 remote에서 가져오기
        if local_max_date != remote_max_date:
            # logger에 출력
            logger.log_warning(f"{local_max_date}~{remote_max_date} BM 데이터 불러오는 중")

            load_sql = f"""
            SELECT AsOfDate, Symbol, Price, IndexName
                FROM FTSE
                WHERE Symbol IN {bm_symbol_tuple} AND AsOfDate > '{local_max_date}' AND AsOfDate <= '{remote_max_date}'
            UNION 
            SELECT AsOfDate, Symbol, Price, IndexName
                FROM MerrillLynch
                WHERE Symbol IN {bm_symbol_tuple} AND AsOfDate > '{local_max_date}' AND AsOfDate <= '{remote_max_date}'
            UNION
            SELECT AsOfDate, Symbol, Price, IndexName
                FROM GSCI
                WHERE Symbol IN {bm_symbol_tuple} AND AsOfDate > '{local_max_date}' AND AsOfDate <= '{remote_max_date}'
            """

            # local db에 추가
            try:
                load_df = self.bm_db_adaptor.get(load_sql).df()
                if not load_df.empty:
                    load_df = load_df.astype('str')
                    load_df.to_sql(sqlite_table, conn, if_exists='append', index=False)
            except sqlite3.Error as e:
                logger.log_error("데이터를 로드하여 저장하는데 실패했습니다.")
                # TODO: 사실 무슨 에러였는지 기억이....
                print(e)

    # Local db: local db에 trading 데이터 가져오기
    def dump_fund_trading_data(self, conn: sqlite3.Connection, symbol_tuple: tuple):
        sqlite_table = 'Trading'

        for symbol in symbol_tuple:
            remote_sql = f"""
                SELECT MAX(AsOfDate) as max_date
                    FROM Trading
                    WHERE Symbol = '{symbol}'
            """
            # local db에 존재하는 최대 날짜 가져오기
            local_sql = f"""
                SELECT MAX(AsOfDate) as max_date
                    FROM Trading
                    WHERE Symbol = '{symbol}'
            """

            # 각 db의 max date 가져오기
            remote_max_date, local_max_date = self.get_local_remote_date(conn, self.price_db_adaptor, remote_sql, local_sql)

            # sqlite랑 mysql 날짜범위 비교해서 없는 날짜만 remote에서 가져오기
            if local_max_date != remote_max_date:
                # logger에 출력
                logger.log_warning(f"{symbol} {local_max_date}~{remote_max_date} 데이터 불러오는 중")

                load_sql = f"""
                    SELECT AsOfDate, Symbol, CompanyCode, NAV, AUM, NetAssets, AdjustedNAV, ShareClassAUM
                        FROM Trading
                        WHERE Symbol = '{symbol}' AND AsOfDate > '{local_max_date}' AND AsOfDate <= '{remote_max_date}'
                """

                # local db에 추가
                try:
                    load_df = self.price_db_adaptor.get(load_sql).df()
                    if not load_df.empty:
                        load_df = load_df.astype('str')
                        load_df.to_sql(sqlite_table, conn, if_exists='append', index=False)
                except sqlite3.IntegrityError as e:
                    logger.log_error("중복된 데이터가 있어 저장하는데 실채했습니다.")
                    print(e)
                    exit(1)
                except sqlite3.Error as e:
                    logger.log_error("데이터를 로드하여 저장하는데 실패했습니다.")
                    # TODO: 사실 무슨 에러였는지 기억이....
                    print(e)
                    exit(1)

    # Screen: 펀드 운용금액이 낮은 펀드 symbol 가져오기
    def get_funds_low_aum(self, symbol_tuple: tuple, target_date: str):
        query = f"""
            SELECT DISTINCT Symbol, AsOfDate
                FROM Trading
                WHERE ShareClassAUM < 5000000000 AND Symbol in {symbol_tuple}
                AND AsOfDate = (SELECT MAX(AsOfDate) FROM Trading WHERE AsOfDate <= '{target_date}')
        """

        # TODO: 진짜 empty인지 데이터가 없어서 empty인지 if로 그래도 검사?
        with sqlite3.connect(self.local_db_file) as conn:
            result_df = pd.read_sql(sql=query, con=conn)

        return result_df

    # Screen: Trading data의 최근 날짜가 타겟 날짜가 아닌 펀드 가져오기
    def get_funds_outdated(self, symbol_tuple: tuple, target_date: str):
        query = f"""
            SELECT Symbol, ShareClassAUM, Max(AsOfDate) as MaxDate
                FROM Trading
                WHERE Symbol in {symbol_tuple} AND AsOfDate <= '{target_date}'
                GROUP BY Symbol
                HAVING MaxDate != '{target_date}'
        """

        with sqlite3.connect(self.local_db_file) as conn:
            result_df = pd.read_sql(sql=query, con=conn)

        return result_df

    # Chart: 해당 펀드의 trade 데이터 가져오기
    def get_fund_trade_df(self, asset_id: str, target_date: str) -> pd.DataFrame:
        # 캐시에서 해당 펀드의 trade 데이터 있는지 확인
        fund_trade_df = self.cache_fund_dict.get(asset_id, None)
        if fund_trade_df is not None:
            return fund_trade_df
        else:
            # 없으면 DB에서 가져오기
            with sqlite3.connect(self.local_db_file) as conn:
                query = f"SELECT * FROM Trading WHERE Symbol='{asset_id}' AND AsOfDate <= '{target_date}'"
                fund_trade_df = pd.read_sql(sql=query, con=conn)
                # 데이터가 비었을 때 dump 해오기
                if fund_trade_df.empty:
                    symbol_tuple = (asset_id,)
                    self.dump_fund_trading_data(conn, symbol_tuple)
                    fund_trade_df = pd.read_sql(sql=query, con=conn)

            # fund_trade_df = self.load_fund_trade_info(self.price_db_adaptor, asset_id)
            # AsOfDate 컬럼을 index로
            fund_trade_df = fund_trade_df.set_index('AsOfDate')
            self.cache_fund_dict[asset_id] = fund_trade_df
            return fund_trade_df

    # Chart: bm 정보 가져오기
    def get_bm_price_df(self, asset_class: str, target_date: str) -> (pd.DataFrame, list):
        # 캐시에서 해당 펀드의 자산군에 맞는 bm 데이터 가져오기
        key = config["ASSET_CLASS_MAP"][asset_class]
        bm_price_df = self.cache_bm_price_dict.get(key, None)

        # bm dict가 None 이면 bm 데이터 전체 불러오기
        if bm_price_df is None:
            # bm_df = self.load_bm_price_info(self.bm_db_adaptor, tuple(ASSET_CLASS_MAP.values()))

            # local db와 연결
            with sqlite3.connect(self.local_db_file) as conn:
                bm_symbol_tuple = tuple(config["ASSET_CLASS_MAP"].values())
                query = f"""
                            SELECT AsOfDate, Symbol, Price, IndexName
                                FROM BM_price
                                WHERE Symbol IN {bm_symbol_tuple}
                        """

                bm_df = pd.read_sql(sql=query, con=conn)
                # 데이터가 비었을 때, dump해오기
                if bm_df.empty:
                    self.dump_bm_price_data(conn, bm_symbol_tuple)
                    bm_df = pd.read_sql(sql=query, con=conn)

            res = bm_df.groupby('Symbol')

            # timelag 계산(KR만 time lag 1)
            for symbol, group in res:
                if symbol in ("MLG0SK", "I04781"):
                    group['Price'] = group['Price'].shift(1)
                else:
                    group['Price'] = group['Price'].shift(2)
                # 캐시 데이터로 저장
                self.cache_bm_price_dict[symbol] = group

            # # ETC 자산군 캐시 만들기
            bm_1_df = self.cache_bm_price_dict["I04781"]  # KR_STOCK
            bm_2_df = self.cache_bm_price_dict["I00010"]  # DM_STOCK
            etc_bm_df = pd.concat([bm_1_df, bm_2_df], ignore_index=True)
            self.cache_bm_price_dict["ETC"] = etc_bm_df

        # 자산군에 맞는 bm 데이터 불러와서 남겨서 pivot
        bm_price_df = self.cache_bm_price_dict.get(key)

        # target date 넘지 않게 자르기
        bm_price_df = bm_price_df[bm_price_df['AsOfDate'] <= target_date]

        pivot_df = bm_price_df.pivot(index='AsOfDate', columns='Symbol', values='Price')
        pivot_df = pivot_df.astype('float')
        pivot_df.index = pd.to_datetime(pivot_df.index)  # day_name을 위해 필요

        # 빈 값들은 앞방향으로 채워나가기
        pivot_df = pivot_df.ffill()

        # 각 자산군의 IndexName 가져오기
        column_asset_id_li = pivot_df.columns.tolist()
        bm_name = []
        for i in range(len(column_asset_id_li)):
            asset_class_id = column_asset_id_li[i]
            # bm 이름 가져오기
            bm_name.append(bm_price_df[bm_price_df['Symbol'] == asset_class_id]['IndexName'].iloc[-1])

        # column 이름 바꾸기
        if asset_class == 'ETC':
            pivot_df = pivot_df.rename(columns={"I04781": "KR_STOCK", "I00010": "DM_STOCK"})
        else:
            pivot_df = pivot_df.rename(columns={key: asset_class})  # key: asset_class_id

        return pivot_df, bm_name

    # TODO: 시각화할 때 groupby가 문제임
    # # ALLOCATION: 데이터프레임 stock -> bond 자산군 순서로 정렬
    # def sort_asset_df(self, port_df: pd.DataFrame) -> pd.DataFrame:
    #     stand = port_df['asset_class_symbol'].str.contains('BOND')
    #     bond = port_df[stand].reset_index(drop=True)
    #     stock = port_df[~stand].reset_index(drop=True)
    #     port_df = pd.concat([stock, bond], ignore_index=True)
    #     return port_df

    # CORRECT: 총합을 100으로 맞추기
    def correct_total_weight(self, weight_dict: dict):
        # 총합이 100인지 확인
        total = sum(weight_dict.values())

        # 비중 dict를 주식/펀드 dict로 각각 나누기
        equity_weight_dict = {}
        fixed_income_weight_dict = {}
        for key in weight_dict.keys():
            if 'STOCK' in key or 'GOLD' in key:
                equity_weight_dict[key] = weight_dict[key]
            elif 'BOND' in key:
                fixed_income_weight_dict[key] = weight_dict[key]

        # 100% 이상일 경우
        if total > 100:
            over = total - 100

            choice = True  # 그냥 equity가 더 적으니까 equity 먼저 주게
            while over != 0:
                # TODO: 알고리즘 공부 해야겠다...하하핳
                max_equity_key = max(equity_weight_dict.keys(), key=lambda k: equity_weight_dict[k])
                max_fixed_key = max(fixed_income_weight_dict.keys(), key=lambda k: fixed_income_weight_dict[k])

                # equity의 최대 비중이 가장 크고, 그 비중이 MIN_WEIGHT 보다도 클 때 choice는 0
                choice = False if equity_weight_dict[max_equity_key] >= fixed_income_weight_dict[max_fixed_key] \
                                  and choice else True

                # choice 값에 따라 자산군 비중 번갈아가면서 조정
                if choice:
                    fixed_income_weight_dict[max_fixed_key] -= 1
                else:
                    equity_weight_dict[max_equity_key] -= 1
                over -= 1

        # 100% 미만일 경우
        elif total < 100:
            less = 100 - total

            # TODO: bool? equity는 False, fixed-income은 True
            choice = True  # 그냥 equity가 더 적으니까 fix-income 먼저 뺏게
            while less != 0:
                min_equity_key = min(equity_weight_dict.keys(), key=lambda k: equity_weight_dict[k])
                min_fixed_key = min(fixed_income_weight_dict.keys(), key=lambda k: fixed_income_weight_dict[k])

                # equity의 최소 비중이 가장 작을 때 choice는 0
                choice = False if equity_weight_dict[min_equity_key] <= fixed_income_weight_dict[min_fixed_key] \
                                  and choice else True

                # choice 값에 따라 자산군 비중 번갈아가면서 조정
                if choice:
                    fixed_income_weight_dict[min_fixed_key] += 1
                else:
                    equity_weight_dict[min_equity_key] += 1
                less -= 1

        weight_dict = equity_weight_dict | fixed_income_weight_dict
        return weight_dict

    # SCREEN: 특정 단어들을 포함하는 펀드 제외
    def screen_fund_name(self, selected_fund_df: pd.DataFrame):
        # 해당 단어들을 포함하는 펀드 찾기
        logger.log(f"펀드 명칭 필터링 시작")

        name_filter_list = ["사모", "모투자", "상장지수", "ELS", "지분증권", "연금", "퇴직", "변액", "장기주택마련",
                            "재형", "소득공제", "목표", "월지급", "법인", "레버리지", "BULL", "1.5배", "2배", "두배",
                            "불마켓", "인버스", "리버스", "BEAR", "경매", "프랭클린", "템플턴", "공모주",
                            "\(UH\)"]
        name_filter_fund = selected_fund_df['asset_name'].str.contains("|".join(name_filter_list), regex=True)

        # LOG: 펀드 명칭 필터 후 제외되는 펀드 출력
        filtered_fund_count = 0
        for idx, value in name_filter_fund.to_dict().items():
            if value is True:
                filtered_fund_count += 1
                logger.log_info(f"{selected_fund_df['asset_name'][idx]} 펀드 제외({selected_fund_df['asset_id'][idx]})")
        logger.log(f"총 {filtered_fund_count}개 펀드 제외")

        # 해당 단어를 가지고 있지 않은 펀드들 남겨놓기
        return selected_fund_df[~name_filter_fund]

    # SCREEN: C 클래스 이외 펀드 제외
    def screen_fund_class(self, selected_fund_df: pd.DataFrame):
        logger.log(f"C클래스 이외 펀드 필터링 시작")

        class_filter_fund = selected_fund_df['asset_name'].str.contains("(([Cc]([0-9]|-?[Ee])?.?))$", regex=True)

        # LOG: 클래스 필터링 후 제외되는 펀드 출력
        not_C_fund = selected_fund_df[~class_filter_fund]
        for idx in range(len(not_C_fund)):
            logger.log_info(f"{not_C_fund.iloc[idx]['asset_name']} 펀드 제외({not_C_fund.iloc[idx]['asset_id']})")
        logger.log(f"총 {len(not_C_fund)}개 펀드 제외")

        # C 클래스인 펀드들 남겨놓기
        return selected_fund_df[class_filter_fund]

    # SCREEN: 104주(=2년) + 5주(버퍼) 미만 펀드 제외
    def screen_fund_period(self, selected_fund_df: pd.DataFrame, target_date: str):
        # 타겟 날짜로부터 104주 전 날짜 찾기
        logger.log(f"펀드 출시일 기준 필터링 시작")

        today = datetime.datetime.strptime(target_date, "%Y-%m-%d")
        least_date = today - relativedelta(years=2) - relativedelta(weeks=5)

        # 운용 기간이 104주보다 짧은 펀드 찾기
        symbol_tuple = tuple(selected_fund_df['asset_id'])
        date_filter_fund = self.load_funds_short_period(self.price_db_adaptor, least_date, symbol_tuple)

        # LOG: 운용기간 필터 후 제외되는 펀드 출력
        for idx in range(len(date_filter_fund)):
            logger.log_info(f"{date_filter_fund.iloc[idx]['Name']} 펀드 제외({date_filter_fund.iloc[idx]['Symbol']})")
        logger.log(f"총 {len(date_filter_fund)}개 펀드 제외")

        # 운용기간이 104주 이상인 펀드들 남겨놓기
        return selected_fund_df[~selected_fund_df['asset_id'].isin(date_filter_fund['Symbol'])]

    # SCREEN: Trading ShareClassAUM 50억 미만 펀드 제외
    def screen_fund_shareAum(self, selected_fund_df: pd.DataFrame, target_date: str):
        # 50억 미만인 펀드 찾기
        logger.log(f"펀드 운용금액 기준 필터링 시작")
        symbol_tuple = tuple(selected_fund_df['asset_id'])
        aum_filter_fund = self.get_funds_low_aum(symbol_tuple, target_date)

        # LOG: AUM 필터 후 제외되는 펀드 출력
        aum_filter_fund_log = selected_fund_df[selected_fund_df['asset_id'].isin(aum_filter_fund['Symbol'])]

        for idx in range(len(aum_filter_fund)):
            logger.log_info(
                f"{aum_filter_fund_log.iloc[idx]['asset_name']} 펀드 제외({aum_filter_fund_log.iloc[idx]['asset_id']})")
        logger.log(f"총 {len(aum_filter_fund_log)}개 펀드 제외")

        # 운용금액이 50억 이상인 펀드들 남겨놓기
        return selected_fund_df[~selected_fund_df['asset_id'].isin(aum_filter_fund['Symbol'])]

    # SCREEN: Trading data가 최근에 쌓이지 않은 펀드 제외
    def screen_fund_last_update(self, selected_fund_df: pd.DataFrame, target_date: str):
        logger.log(f"Trading data 날짜 필터링 시작")
        symbol_tuple = tuple(selected_fund_df['asset_id'])
        trading_outdated_fund = self.get_funds_outdated(symbol_tuple, target_date)

        # LOG: Trade 기간 필터 후 제외되는 펀드 출력
        aum_filter_fund_log = selected_fund_df[selected_fund_df['asset_id'].isin(trading_outdated_fund['Symbol'])]
        for idx in range(len(aum_filter_fund_log)):
            logger.log_info(
                f"{aum_filter_fund_log.iloc[idx]['asset_name']} 펀드 제외({aum_filter_fund_log.iloc[idx]['asset_id']})")
        logger.log(f"총 {len(aum_filter_fund_log)}개 펀드 제외")

        # Trading 기록이 최신인 펀드들 남겨놓기
        return selected_fund_df[~selected_fund_df['asset_id'].isin(trading_outdated_fund['Symbol'])]

    # ---PROCESS: screening 단계
    def screening(self, total_fund_df: pd.DataFrame, target_date: str) -> pd.DataFrame:
        # Trading data가 최근에 쌓이지 않은 펀드 제외
        selected_fund_df = self.screen_fund_last_update(total_fund_df, target_date)
        if selected_fund_df.empty:
            with sqlite3.connect(self.local_db_file) as conn:
                logger.log_error("해당 일의 데이터가 로컬에 업데이트 되지 않았습니다. 업데이트를 진행하겠습니다.")
                self.dump_fund_trading_data(conn, tuple(total_fund_df['asset_id']))
                selected_fund_df = self.screen_fund_last_update(total_fund_df, target_date)
            logger.log_error("업데이트 끝")

        # Etc 자산군 펀드 제외
        logger.log(f"ETC 자산군 펀드 필터링 시작")
        asset_class_filter_fund = selected_fund_df[selected_fund_df['asset_class_symbol'] == 'ETC']

        # LOG: 자산군 필터링 후 제외되는 펀드 출력
        for idx in range(len(asset_class_filter_fund)):
            logger.log_info(
                f"{asset_class_filter_fund.iloc[idx]['asset_name']} 펀드 제외({asset_class_filter_fund.iloc[idx]['asset_id']})")
        logger.log(f"총 {len(asset_class_filter_fund)}개 펀드 제외")

        # ETC 자산군이 아닌 펀드들 남겨놓기
        selected_fund_df = selected_fund_df[~selected_fund_df['asset_id'].isin(asset_class_filter_fund['asset_id'])]

        # 특정 단어들을 포함하는 펀드 제외
        selected_fund_df = self.screen_fund_name(selected_fund_df)

        # C 클래스 이외 펀드 제외
        selected_fund_df = self.screen_fund_class(selected_fund_df)

        # 104주(=2년) + 5주(버퍼) 미만 펀드 제외
        selected_fund_df = self.screen_fund_period(selected_fund_df, target_date)

        # Trading ShareClassAUM 50억 미만 펀드 제외
        selected_fund_df = self.screen_fund_shareAum(selected_fund_df, target_date)

        return selected_fund_df

    # ---PROCESS: proselecting 단계(상관계수 구하기)
    def preselecting(self, fund_df: pd.DataFrame, target_date: str) -> pd.DataFrame:
        # pre-selection의 return 변수
        preselected_fund_df = pd.DataFrame(columns=['asset_id', 'asset_name', 'asset_class_symbol',
                                                    'correlation', 'period_return'])

        # TODO: 나중에 merge해서 리턴하기??
        # fund_corr_df = pd.DataFrame(columns=['asset_id', 'spearman_corr'])

        # 각 펀드의 spearman 상관계수 구하기
        res = fund_df.groupby('asset_class_symbol')

        # 자산군별로 corr 구하기
        filtered_fund_count = 0
        for key, group in res:
            # bm 데이터 df 불러오기
            bm_fund_df, bm_name = self.get_bm_price_df(key, target_date)
            fund_info_dict = {}
            for i in range(len(group)):
                asset_id = group.iloc[i]['asset_id']
                asset_name = group.iloc[i]['asset_name']

                # 펀드 가격 정보 가져오기
                trade_df = self.get_fund_trade_df(asset_id, target_date)
                trade_df = trade_df.reset_index()
                trade_df = trade_df.pivot(index='AsOfDate', columns='Symbol', values='AdjustedNAV')

                # 해당 펀드의 기간 수익률 계산
                # target date에서 40주까지, 최근 4주 제외
                trade_df.index = pd.to_datetime(trade_df.index)
                start_date = datetime.datetime.strptime(target_date, "%Y-%m-%d") - relativedelta(weeks=104)
                end_date = datetime.datetime.strptime(target_date, "%Y-%m-%d") - relativedelta(weeks=4)
                period_return_df = trade_df.query(
                    f"index >= '{start_date.date()}' and index <= '{end_date.date()}'").sort_index(axis=0, ascending=True)

                period_return = float(((period_return_df.iloc[-1] - period_return_df.iloc[0]) / period_return_df.iloc[0]) * 100)

                fund_info_dict[asset_id] = {
                    'asset_id': asset_id,
                    'asset_name': asset_name,
                    'asset_class_symbol': key,
                    'period_return': period_return
                }

                # 같은 자산군 df끼리 세로로 붙임 날짜가 index, column은 asset_id, value는 price
                bm_fund_df = self.concat_fund_bm_df(trade_df, bm_fund_df)  # 첫 bm_fund_df는 bm_df 값만 가지고 있음

            # 펀드들과 BM의 스피어만 상관계수 한꺼번에 구하기
            corr_df = self.cal_spearman_corr(bm_fund_df)
            corr_df = corr_df[key]  # ETC는 어차피 없으므로

            # TODO: 흠...
            logger.log(f"{key} 자산군 상관계수 필터링 시작")
            for i in range(len(group)):
                asset_id = group.iloc[i]['asset_id']
                asset_name = group.iloc[i]['asset_name']
                fund_bm_corr = corr_df[asset_id]

                # 스피어만 상관계수가 0.8 이상인 펀드들 추리기
                if (fund_bm_corr >= 0.8) or ("BOND" in key):
                    fund_info_dict[asset_id]['correlation'] = fund_bm_corr
                    preselected_fund_df = preselected_fund_df.append(fund_info_dict[asset_id], ignore_index=True)
                else:
                    # LOG: 기준 미달 펀드 정보 출력
                    filtered_fund_count += 1
                    logger.log_info(f"{asset_name} ({asset_id})펀드 제외. 상관계수: {fund_bm_corr}")

        # LOG: 기준 미달 펀드 총 개수 출력
        logger.log(f"총 {filtered_fund_count}개 펀드 제외")

        # 0.8 이상인 펀드 df 리턴
        return preselected_fund_df

    # ---PROCESS: weighting 단계
    def weighting(self, target_date: str, user_risk_type: dict) -> dict:
        # macro score 받아오기
        macro_score_df = self.load_macro_score(self.customer_db_adaptor, target_date)
        dm_stock_score = float(macro_score_df.query("score_id == 'DM_STOCK'")['score_value'])
        score_idx = int(dm_stock_score + 1)

        # 위험성향 별 자산군 비중 구하기
        weight_by_risk = {}
        for risk_type, equity in user_risk_type.items():
            # ---Equity(주식) 전체 비율 구하기 -> 일의 자리로 나눠 떨어져야함
            equity = round(equity * config['WEIGHTING_RULE']['TOTAL_EQUITY'][score_idx] * 100, 5)
            # equity 자산군 비중 계산
            equity_weight_dict = {}
            for asset_class, value in config['WEIGHTING_RULE']['EQUITY'].items():
                equity_weight_dict[asset_class] = round(value[score_idx] * equity, 5)
            # dm stock 따로 계산
            equity_weight_dict['DM_STOCK'] = round(equity - sum(equity_weight_dict.values()), 5)

            # ---Fixed Income(펀드) 전체 비율 구하기
            fixed_income = 100 - equity
            # fixed income 자산군 비중 계산
            fixed_income_weight_dict = {}
            for asset_class, value in config['WEIGHTING_RULE']['FIXED_INCOME'].items():
                fixed_income_weight_dict[asset_class] = round(value[score_idx] * fixed_income, 5)
            # kr_bond 따로 계산
            fixed_income_weight_dict['KR_BOND'] = round(fixed_income - sum(fixed_income_weight_dict.values()), 5)

            # 위험 유형별 최종 weight dict
            weight_by_risk[risk_type] = equity_weight_dict | fixed_income_weight_dict

        return weight_by_risk

    # ---PROCESS: 주어진 비중으로 포트폴리오 산출 단계
    def select_portfolio(self, postselected_fund_df: pd.DataFrame, weight_by_risk: dict, CORRECT: bool = False) -> dict:
        MAX_WEIGHT = 30
        MIN_WEIGHT = 5

        # 각 자산군의 top5 df를 자산군끼리 묶기
        res = postselected_fund_df.groupby('asset_class_symbol')

        # 최종 return dict
        portfolio_by_risk = {}
        for risk_type, weight_list in weight_by_risk.items():
            # risk type마다 선별된 포트폴리오 저장
            portfolio_df = pd.DataFrame(columns=['asset_id', 'asset_class_symbol', 'asset_name', 'weight'])  # 자산군, 펀드이름, 비중

            # 적절한 펀드 찾기
            # TODO: 3중 for구문
            for asset_class, fund_group in res:
                fund_group = fund_group.sort_values(by="period_return", ascending=False)
                # 비중이 없는 자산군은 표시 안함
                total_weight = weight_list[asset_class]
                if total_weight == 0:
                    continue

                # 각 자산군이 가질 펀드 개수
                recommend_fund_num = int(total_weight // MAX_WEIGHT + 1)

                # Correction 단계일 경우
                # MIN_WEIGHT 미만이라면(0포함), 선택된 펀드들의 비중이 MAX_WEIGHT을 초과해도 됨.(나눠가짐)
                if (total_weight % MAX_WEIGHT) < MIN_WEIGHT and CORRECT:
                    # 나머지가 최소 비중 미만이면 펀드 개수 -1
                    recommend_fund_num -= 1

                    for i in range(recommend_fund_num):
                        own_weight = total_weight if total_weight <= MAX_WEIGHT else max(MAX_WEIGHT, total_weight % MAX_WEIGHT)
                        own_weight += round((total_weight % MAX_WEIGHT) / recommend_fund_num)
                        total_weight -= own_weight

                        if own_weight != 0:
                            portfolio_df = portfolio_df.append({
                                'weight': own_weight,
                                'asset_name': fund_group.iloc[i]['asset_name'],
                                'asset_id': fund_group.iloc[i]['asset_id'],
                                'asset_class_symbol': asset_class
                            }, ignore_index=True)
                # Correction 단계가 아니거나, 나머지 비중이 최소 비중보다 클 경우
                else:
                    for i in range(recommend_fund_num):
                        own_weight = round(total_weight if total_weight <= MAX_WEIGHT else max(MAX_WEIGHT, total_weight % MAX_WEIGHT), 5)
                        total_weight -= own_weight

                        if own_weight != 0:
                            portfolio_df = portfolio_df.append({
                                'weight': own_weight,
                                'asset_name': fund_group.iloc[i]['asset_name'],
                                'asset_id': fund_group.iloc[i]['asset_id'],
                                'asset_class_symbol': asset_class
                            }, ignore_index=True)

            portfolio_by_risk[risk_type] = portfolio_df

        return portfolio_by_risk

    # TODO: portfolio_by_risk를 그냥 postselected_fund_df가 할수 있을 것 같은데...
    # ---PROCESS: correcting 단계
    def correcting(self, postselected_fund_df: pd.DataFrame, weight_by_risk: dict) -> (dict, dict):
        MAX_WEIGHT = 30
        MIN_WEIGHT = 5

        for risk_type, weight_dict in weight_by_risk.items():
            # # TODO: df로 하면 편한데 일단 dict로 해보기
            # weight_df = pd.DataFrame(list(weight_dict.items()), columns=['asset_class', 'weight'])

            # ---1. 일의 자리로 반올림
            for asset_class, weight in weight_dict.items():
                weight_dict[asset_class] = round(weight)

            # 비중 총합이 100인지 확인
            weight_dict = self.correct_total_weight(weight_dict)

            # ---2. 포트폴리오에 없는 자산군 유형의 비중은 N분할
            # TODO: 아어ㅏㅓ마ㅓ;ㅣㅏㅓ
            stock_left = 0
            bond_left = 0
            stock_num = 0
            bond_num = 0
            for asset_class, weight in list(weight_dict.items()):
                if asset_class not in postselected_fund_df['asset_class_symbol'].unique():
                    if 'BOND' in asset_class:
                        bond_left += weight
                        del weight_dict[asset_class]
                    elif 'STOCK' in asset_class or 'GOLD' in asset_class:
                        stock_left += weight
                        del weight_dict[asset_class]
                else:
                    if 'BOND' in asset_class:
                        bond_num += 1
                    elif 'STOCK' in asset_class or 'GOLD' in asset_class:
                        stock_num += 1

            # 정확히 분할이 안될 경우, 몫은 동등하게 가지고, 나머지는 defulat 자산에 넣기
            # TODO: 불필요한 순회?, 계산 에러처리...
            stock_quot = stock_left // stock_num
            bond_quot = bond_left // bond_num
            for asset_class in weight_dict.keys():
                if 'BOND' in asset_class:
                    weight_dict[asset_class] += bond_quot
                elif 'STOCK' in asset_class or 'GOLD' in asset_class:
                    weight_dict[asset_class] += stock_quot
            weight_dict['DM_STOCK'] += (stock_left % stock_num)
            weight_dict['KR_BOND'] += (bond_left % bond_num)

            # ---3. 비중이 최소비중 미만인 자산군은 default 자산에 포함시키기
            for asset_class, weight in weight_dict.items():
                # TODO: 자기 자신거 들어가서 문제. 한번 더 더하니까 -> 해결해쓴ㄴ데 이게 최선?
                if weight < MIN_WEIGHT:
                    if 'BOND' in asset_class and asset_class != 'KR_BOND':
                        weight_dict['KR_BOND'] += weight
                    elif ('STOCK' in asset_class or 'GOLD' in asset_class) and asset_class != 'DM_STOCK':
                        weight_dict['DM_STOCK'] += weight

            weight_by_risk[risk_type] = weight_dict

        # 수정된 비중에 따라 위험성향별 포트폴리오 선정
        portfolio_by_risk = self.select_portfolio(postselected_fund_df, weight_by_risk, True)

        return weight_by_risk, portfolio_by_risk

    # 같은 기간으로 자른 bm df와 trade df 가로로 합치기
    def concat_fund_bm_df(self, trade_df: pd.DataFrame, bm_df: pd.DataFrame) -> pd.DataFrame:
        # index 타입이 서로 같아야 함
        trade_df.index = pd.to_datetime(trade_df.index)

        # 펀드 가격 df와 bm 가격 df 합치기
        bm_fund_df = pd.concat([bm_df, trade_df], axis=1).ffill().dropna()

        # columns 명 변경
        bm_fund_df = bm_fund_df.rename(columns={'Symbol': 'asset_id'})

        # bm_fund_df.index = bm_fund_df.index.date

        return bm_fund_df

    # 스피어만 상관계수 계산
    def cal_spearman_corr(self, bm_fund_df: pd.DataFrame):
        bm_fund_df = bm_fund_df.astype('float')

        # 최근 날짜 기준 weekly 데이터로 전환
        date = bm_fund_df.index[-1]
        day_name = date.strftime("%a")

        corr_df = bm_fund_df.resample(f'W-{day_name}').ffill()

        # 104주 데이터로 자르기, 최근 4주 데이터 제외
        start_date = date - relativedelta(weeks=104)
        end_date = date - relativedelta(weeks=4)
        corr_df = corr_df.query(f"index >= '{start_date}' and index <= '{end_date}'")

        # 펀드들과 BM의 스피어만 상관계수 한꺼번에 구하기
        corr_df = corr_df.corr(method='spearman')

        return corr_df
