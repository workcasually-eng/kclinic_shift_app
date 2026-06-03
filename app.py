#インプット

import streamlit as st
import pandas as pd
from ortools.sat.python import cp_model
import calendar
import datetime
import os
import time
import gspread
from google.oauth2.service_account import Credentials
import random

# =========================================================
# ⚙️ 設定エリア
# =========================================================
# パスワードやURLはコードに書かず、StreamlitのSecrets機能から読み込みます
try:
    DEFAULT_ADMIN_PASSWORD = st.secrets["admin_password"]
    DEFAULT_SUPER_ADMIN_ID = "root"
    DEFAULT_SUPER_ADMIN_PASS = st.secrets["super_admin_pass"]
    URL_REQUEST_DB = st.secrets["sheet_url"]
except FileNotFoundError:
    st.error("⚠️ Secrets情報が見つかりません。Streamlit Cloud of Settingsで設定してください。")
    st.stop()
except KeyError as e:
    st.error(f"⚠️ 設定が不足しています: {e}")
    st.stop()

# 全てのデータ（マスタ、申請、ログ、仮シフト、完成シフト）をこのシートで管理します
# ※ご自身のスプレッドシートURLを設定してください
URL_REQUEST_DB = "https://docs.google.com/spreadsheets/d/1y7H-9c2EJhpCKoXY6Va_RRx3dfDZoarxlUmQLdXEP6o/edit"

# =========================================================
# 🚀 アプリ初期設定 & セッション初期化
# =========================================================
st.set_page_config(page_title="病院シフト管理アプリ", layout="wide")

# ▼▼▼ 追加コード：メニューとフッターを非表示にするCSS ▼▼▼
hide_streamlit_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            header {visibility: hidden;}
            </style>
            """
st.markdown(hide_streamlit_style, unsafe_allow_html=True)
# ▲▲▲ 追加コード終了 ▲▲▲

# セッション変数の枠作成
if 'user_role' not in st.session_state: st.session_state.user_role = None
if 'user_name' not in st.session_state: st.session_state.user_name = None
if 'schedule_df' not in st.session_state: st.session_state.schedule_df = None
if 'system_phase' not in st.session_state: st.session_state.system_phase = "0_通常"
if 'proc_year' not in st.session_state: st.session_state.proc_year = datetime.date.today().year
if 'proc_month' not in st.session_state: st.session_state.proc_month = datetime.date.today().month

# データキャッシュ
if 'master_staff' not in st.session_state: st.session_state.master_staff = None
if 'master_ph' not in st.session_state: st.session_state.master_ph = None
if 'master_log' not in st.session_state: st.session_state.master_log = None
if 'req_off_data' not in st.session_state: st.session_state.req_off_data = None
if 'req_chg_data' not in st.session_state: st.session_state.req_chg_data = None
if 'daily_reqs' not in st.session_state: st.session_state.daily_reqs = {}

# =========================================================
# 🛠️ ヘルパー関数 (GSheet操作一元化 + キャッシュ対応)
# =========================================================
def get_gspread_client():
    scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = None
    try:
        if "gcp_service_account" in st.secrets:
            key_dict = dict(st.secrets["gcp_service_account"])
            creds = Credentials.from_service_account_info(key_dict, scopes=scope)
    except: pass

    if creds is None and os.path.exists('secret_key.json'):
        creds = Credentials.from_service_account_file('secret_key.json', scopes=scope)
    
    if creds:
        return gspread.authorize(creds)
    return None

def connect_sheet(sheet_name, headers=None):
    """シートに接続、なければ作成する。リトライ処理付き"""
    client = get_gspread_client()
    if not client: return None, "認証エラー: secret_key.jsonまたはst.secretsの設定を確認してください"
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            spreadsheet = client.open_by_url(URL_REQUEST_DB)
            try:
                worksheet = spreadsheet.worksheet(sheet_name)
                if headers:
                    first_row = worksheet.row_values(1)
                    if not first_row: 
                        worksheet.append_row(headers)
            except gspread.exceptions.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=20)
                if headers:
                    worksheet.append_row(headers)
            return worksheet, None
        except gspread.exceptions.APIError as e:
            if "429" in str(e):
                time.sleep(2 ** attempt)
                continue
            else:
                return None, str(e)
        except Exception as e:
            return None, str(e)
    return None, "API制限により接続できませんでした。しばらく待って再試行してください。"

# --- データ読み書き用 ---

@st.cache_data(ttl=600, show_spinner=False)
def load_data(sheet_name, expected_headers=None):
    """スプレッドシートからデータを読み込みDataFrameで返す"""
    ws, err = connect_sheet(sheet_name, expected_headers)
    if err: return pd.DataFrame(columns=expected_headers or [])
    
    try:
        data = ws.get_all_records()
        if not data: return pd.DataFrame(columns=expected_headers or [])
        df = pd.DataFrame(data).astype(str)
        
        if expected_headers:
            for col in expected_headers:
                if col not in df.columns:
                    df[col] = ""
        return df
    except:
        return pd.DataFrame(columns=expected_headers or [])

def clear_data_cache():
    """保存後にキャッシュをクリアして最新データを読み込めるようにする"""
    load_data.clear()

def save_data(sheet_name, df):
    """DataFrameの内容でスプレッドシートを全上書きする"""
    ws, err = connect_sheet(sheet_name)
    if err: return False, err
    
    try:
        ws.clear()
        upload_df = df.fillna("")
        upload_data = [upload_df.columns.tolist()] + upload_df.values.tolist()
        try:
            ws.update(values=upload_data, range_name='A1')
        except TypeError:
            ws.update('A1', upload_data)
        
        clear_data_cache()
        return True, "保存完了"
    except Exception as e:
        return False, str(e)

def clear_sheet_data(sheet_name):
    """シートの中身を完全に消去する"""
    ws, err = connect_sheet(sheet_name)
    if err: return False
    try:
        ws.clear()
        clear_data_cache()
        return True
    except: return False

def append_row_data(sheet_name, row_list):
    """リストデータを1行追記する"""
    ws, err = connect_sheet(sheet_name)
    if err: return False, err
    try:
        ws.append_row(row_list)
        clear_data_cache()
        return True, "追加完了"
    except Exception as e:
        return False, str(e)

def update_cell_value(sheet_name, row_idx, col_idx, value):
    """特定セルの更新"""
    ws, err = connect_sheet(sheet_name)
    if err: return False
    try:
        ws.update_cell(row_idx, col_idx, value)
        clear_data_cache()
        return True
    except: return False

def update_log_sheet(new_df):
    """ログシート更新"""
    current_df = load_data("ログ", ['日付', '曜日'])
    
    if not current_df.empty:
        current_df['日付'] = pd.to_datetime(current_df['日付'], errors='coerce').dt.date
    if not new_df.empty:
        new_df['日付'] = pd.to_datetime(new_df['日付'], errors='coerce').dt.date
        
    current_df = current_df.dropna(subset=['日付'])
    new_df = new_df.dropna(subset=['日付'])
    
    if not new_df.empty and not current_df.empty:
        target_month = new_df.iloc[0]['日付'].month
        target_year = new_df.iloc[0]['日付'].year
        current_df = current_df[~current_df['日付'].apply(lambda x: x.year == target_year and x.month == target_month)]
    
    combined = pd.concat([current_df, new_df], ignore_index=True)
    combined.sort_values('日付', inplace=True)
    combined['日付'] = combined['日付'].astype(str)
    return save_data("ログ", combined)

def update_requirements_sheet(new_df):
    """必要人数シート（draft_requirements）更新"""
    current_df = load_data("draft_requirements", ['日付', '曜日', '必要人数'])
    
    if not current_df.empty:
        current_df['日付'] = pd.to_datetime(current_df['日付'], errors='coerce').dt.date
        current_df = current_df.dropna(subset=['日付'])
        
    if not new_df.empty:
        new_df['日付'] = pd.to_datetime(new_df['日付'], errors='coerce').dt.date
        new_df = new_df.dropna(subset=['日付'])
    
    if not new_df.empty and not current_df.empty:
        target_month = new_df.iloc[0]['日付'].month
        target_year = new_df.iloc[0]['日付'].year
        current_df = current_df[~current_df['日付'].apply(lambda x: x.year == target_year and x.month == target_month)]
    
    combined = pd.concat([current_df, new_df], ignore_index=True)
    combined.sort_values('日付', inplace=True)
    combined['日付'] = combined['日付'].astype(str)
    return save_data("draft_requirements", combined)

# --- システム設定（フェーズ・年月）管理関数 ---

def get_system_config():
    """DBからシステム設定を読み込み、辞書型で返す"""
    df = load_data("system_config", ["key", "value"])
    config = {}
    if not df.empty:
        for _, row in df.iterrows():
            config[row['key']] = row['value']
    return config

def update_single_config(key, value):
    """指定したキーの設定だけを更新し、他は維持する"""
    current_config = get_system_config()
    current_config[key] = str(value)
    
    new_df = pd.DataFrame(list(current_config.items()), columns=["key", "value"])
    save_data("system_config", new_df)
    
    if key == 'current_phase':
        st.session_state.system_phase = value
    elif key == 'proc_year':
        st.session_state.proc_year = int(value)
    elif key == 'proc_month':
        st.session_state.proc_month = int(value)

def init_session_from_db():
    """起動時にDBから設定を読み込んでセッションに反映する"""
    config = get_system_config()
    
    st.session_state.system_phase = config.get('current_phase', "0_通常")
    
    db_year = config.get('proc_year')
    db_month = config.get('proc_month')
    
    if db_year and db_month:
        st.session_state.proc_year = int(db_year)
        st.session_state.proc_month = int(db_month)
    else:
        # デフォルト計算
        today = datetime.date.today()
        if today.day <= 10: target_m = today.month + 2
        else: target_m = today.month + 3
        target_y = today.year
        while target_m > 12:
            target_m -= 12
            target_y += 1
        
        st.session_state.proc_year = target_y
        st.session_state.proc_month = target_m

# =========================================================
# 📦 データマネージャ & 共通ロジック
# =========================================================
def sync_all_data():
    """全データを最新化"""
    clear_data_cache()
    init_session_from_db()
    
    st.session_state.master_staff = load_data("スタッフマスタ", ['id', 'password', 'name', 'role', 'en', 'jp', 'vet', 'holiday_target'])
    if not st.session_state.master_staff.empty:
        for col in ['en','jp','vet']:
            if col in st.session_state.master_staff.columns:
                st.session_state.master_staff[col] = st.session_state.master_staff[col].apply(lambda x: str(x).upper()=='TRUE')

    st.session_state.master_ph = load_data("公休マスタ", ['date', 'name'])
    st.session_state.master_log = load_data("ログ", ['日付', '曜日'])
    st.session_state.req_off_data = load_data("希望休", ["タイムスタンプ", "名前", "日付", "備考", "ステータス"])
    st.session_state.req_chg_data = load_data("変更申請", ["タイムスタンプ", "名前", "日付", "種別", "備考", "ステータス"])

# アプリ起動時に一回だけ設定をロード
if st.session_state.master_staff is None:
    sync_all_data()

def get_staff_list():
    df = st.session_state.master_staff
    if df is None or df.empty: return []
    active_staff_df = df[df['role'] == 'staff'].copy()
    for col in ['en','jp','vet']:
        if col in active_staff_df.columns:
            active_staff_df[col] = active_staff_df[col].apply(lambda x: str(x).upper()=='TRUE')
    return active_staff_df.to_dict('records')

def check_daily_constraints(staffs_list, shift_column, required_count_map=None, current_day_idx=None):
    working_staffs = []
    for s in staffs_list:
        nm = s['name']
        val = str(shift_column.get(nm, '0'))
        if val == '1':
            working_staffs.append(s)
    
    required = 4
    if required_count_map and current_day_idx is not None:
        required = required_count_map.get(current_day_idx, 4)
        
    if len(working_staffs) < required:
        return False, f"人数不足(必要{required}人 -> 現在{len(working_staffs)}人)"
    if sum(1 for s in working_staffs if s['jp']) < 1: return False, "日本語話者不足"
    if sum(1 for s in working_staffs if s['en']) < 1: return False, "英語話者不足"
    if sum(1 for s in working_staffs if s['vet']) < 1: return False, "ベテラン不足"
    
    return True, "OK"

# =========================================================
# 🚪 ログイン画面
# =========================================================
def login_screen():
    st.title("🏥 シフト管理システム")
    st.markdown("IDとパスワードを入力してログインしてください。")

    with st.form("login_form"):
        user_id = st.text_input("ユーザーID")
        password = st.text_input("パスワード", type="password")
        submit = st.form_submit_button("ログイン")

        if submit:
            input_id = user_id.strip()
            input_pass = password.strip()

            if input_id == DEFAULT_SUPER_ADMIN_ID and input_pass == DEFAULT_SUPER_ADMIN_PASS:
                st.session_state.user_role = "admin"
                st.session_state.user_name = "Super Admin"
                with st.spinner("データ同期中..."):
                    sync_all_data()
                st.success("スーパー管理者としてログインしました")
                st.rerun()

            try:
                staff_master = load_data("スタッフマスタ", ['id', 'password', 'name', 'role'])
                user_row = staff_master[staff_master['id'] == input_id]
                
                if not user_row.empty:
                    stored_pass = str(user_row.iloc[0]['password'])
                    if stored_pass == input_pass:
                        role = str(user_row.iloc[0]['role']).lower()
                        name = str(user_row.iloc[0]['name'])
                        
                        st.session_state.user_name = name
                        if role == 'admin':
                            st.session_state.user_role = "admin"
                            with st.spinner("データ同期中..."):
                                sync_all_data()
                            st.success("管理者ログイン成功")
                        else:
                            st.session_state.user_role = "staff"
                            st.success("ログイン成功")
                        st.rerun()
                    else:
                        st.error("パスワードが違います")
                else:
                    st.error("IDが見つかりません")
            except Exception as e:
                st.error(f"ログインエラー: {e}")


# =========================================================
# 👤 スタッフ画面
# =========================================================
def staff_screen():
    user_name = st.session_state.user_name
    phase = st.session_state.system_phase
    
    target_y = st.session_state.proc_year
    target_m = st.session_state.proc_month
    default_date = datetime.date(target_y, target_m, 1)
    
    st.sidebar.title(f"👤 {user_name}")
    
    phase_colors = {
        "0_通常": "blue",
        "1_追加申請": "orange",
        "2_削減申請": "red"
    }
    p_color = phase_colors.get(phase, "gray")
    st.sidebar.markdown(f"現在のフェーズ:  \n:{p_color}[**{phase}**]")
    st.sidebar.info(f"対象年月: **{target_y}年{target_m}月**")

    if st.sidebar.button("ログアウト", type="primary"):
        st.session_state.user_role = None
        st.rerun()

    st.title("スタッフ用ダッシュボード")
    
    tabs = ["📝 希望休(初期)", "📜 確定シフト"]
    
    if "1_追加申請" in phase:
        tabs.insert(1, "➕ 出勤追加申請")
    elif "2_削減申請" in phase:
        tabs.insert(1, "➖ 休日追加申請")
        
    selected_tab = st.radio("メニュー選択", tabs, horizontal=True)
    st.divider()

    df_draft = load_data("draft_schedule")
    staffs = get_staff_list()
    
    # 変更申請データのロード（履歴表示と重複防止用）
    df_chg = load_data("変更申請", ["タイムスタンプ", "名前", "日付", "種別", "備考", "ステータス"])
    my_active_reqs = pd.DataFrame()
    if not df_chg.empty:
        df_chg['dt'] = pd.to_datetime(df_chg['日付'], errors='coerce')
        mask = (df_chg['名前'] == user_name) & \
               (df_chg['ステータス'] != '取り消し') & \
               (df_chg['dt'].dt.year == target_y) & \
               (df_chg['dt'].dt.month == target_m)
        my_active_reqs = df_chg[mask].copy()
        
        if not my_active_reqs.empty:
            my_active_reqs['original_idx'] = my_active_reqs.index + 2 

    req_map = {}
    req_df = load_data("draft_requirements")
    if not req_df.empty:
        for _, r in req_df.iterrows():
            try:
                d = pd.to_datetime(r['日付'])
                if d.year == target_y and d.month == target_m:
                    req_map[d.day - 1] = int(r['必要人数'])
            except: pass

    # ----------------------------------------------------------------
    # 📝 希望休(初期)
    # ----------------------------------------------------------------
    if selected_tab == "📝 希望休(初期)":
        st.subheader("希望休の申請")
        if "0_通常" not in phase:
            st.warning("⚠️ 現在は通常の希望休申請フェーズではありません。")
        else:
            st.info("希望休申請です。2か月前10日までに申請してください。それ以降に申請されたものはは反映されません。2ヶ月後以降先の予定も申請可能です。")
        
        with st.form("req_form"):
            d = st.date_input("日付", value=default_date)
            # 備考欄削除
            if st.form_submit_button("送信"):
                ts = datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S')
                res, msg = append_row_data("希望休", [ts, user_name, str(d), "", "申請"])
                if res: st.success("申請しました"); st.rerun()
                else: st.error(msg)

        st.subheader("▼ 申請済みリスト")
        df_req = load_data("希望休", ["タイムスタンプ", "名前", "日付", "備考", "ステータス"])
        if not df_req.empty:
            valid_recs = []
            for i, r in enumerate(df_req.to_dict('records')):
                if r['名前'] == user_name:
                    r['_row_idx'] = i + 2
                    valid_recs.append(r)
            valid_recs = [r for r in valid_recs if r.get('ステータス') != '取り消し']
            valid_recs = sorted(valid_recs, key=lambda x: x['日付'])

            if valid_recs:
                for i, r in enumerate(valid_recs):
                    with st.container():
                        ca, cb = st.columns([4, 2])
                        with ca: st.write(f"📅 **{r['日付']}**")
                        with cb:
                            if st.button("取り消し", key=f"can_req_{i}"):
                                update_cell_value("希望休", r['_row_idx'], 5, "取り消し")
                                st.success("取り消しました"); st.rerun()
                        st.markdown("---")
            else: st.info("有効な申請はありません")
        else: st.info("申請はありません")

    # ----------------------------------------------------------------
    # ➕ 出勤追加申請 (Phase 1)
    # ----------------------------------------------------------------
    elif selected_tab == "➕ 出勤追加申請":
        st.subheader("出勤追加申請 (仮シフト確認)")
        st.info("現在は「出勤を増やす」申請のみ受け付けています。仮シフトで「休み(-)」になっている箇所を申請できます。")
        
        if df_draft is None or df_draft.empty:
            st.error("仮シフトがまだ公開されていません")
        else:
            st.markdown("##### ▼ あなたの仮シフト")
            df_draft_idx = df_draft.set_index(df_draft.columns[0])
            
            if user_name in df_draft_idx.index:
                my_row = df_draft_idx.loc[user_name]
                st.dataframe(pd.DataFrame([my_row.replace({'1':'●','0':'-'})]), use_container_width=True)

                requested_add_dates = set()
                if not my_active_reqs.empty:
                    add_reqs = my_active_reqs[my_active_reqs['種別'] == '出勤希望']
                    for _, r in add_reqs.iterrows():
                        requested_add_dates.add(r['dt'].date())

                rest_days = []
                for col in df_draft_idx.columns:
                    if str(my_row[col]) == '0':
                        try:
                            d_obj = pd.to_datetime(f"{target_y}/{col}").date()
                            if d_obj not in requested_add_dates:
                                rest_days.append(col)
                        except: pass
                
                st.divider()
                st.markdown("##### 申請フォーム")
                if not rest_days:
                    st.success("追加申請可能な日（休み、かつ未申請の日）はありません。")
                else:
                    with st.form("add_work_form"):
                        target_day_str = st.selectbox("出勤に変更したい日", rest_days)
                        # 備考欄削除
                        if st.form_submit_button("出勤申請を送る"):
                            d_obj = pd.to_datetime(f"{target_y}/{target_day_str}").date()
                            ts = datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S')
                            res, msg = append_row_data("変更申請", [ts, user_name, str(d_obj), "出勤希望", "", "申請"])
                            if res: st.success("出勤申請を送りました"); st.rerun()
                            else: st.error(msg)
                
                # --- 履歴と取り消し ---
                st.markdown("##### ▼ 申請中の出勤希望")
                if not my_active_reqs.empty:
                    adds = my_active_reqs[my_active_reqs['種別'] == '出勤希望'].sort_values('dt')
                    if not adds.empty:
                        for i, row in adds.iterrows():
                            with st.container():
                                c1, c2 = st.columns([4, 2])
                                c1.write(f"📅 **{row['日付']}**")
                                if c2.button("取り消し", key=f"cnl_add_{i}"):
                                    update_cell_value("変更申請", row['original_idx'], 6, "取り消し")
                                    st.success("取り消しました"); st.rerun()
                                st.markdown("---")
                    else: st.info("申請中のものはありません")
                else: st.info("申請中のものはありません")

            else:
                st.error("シフト表にあなたの名前が見つかりません")

    # ----------------------------------------------------------------
    # ➖ 休日追加申請 (Phase 2)
    # ----------------------------------------------------------------
    elif selected_tab == "➖ 休日追加申請":
        st.subheader("休日追加申請 (仮シフト確認)")
        st.info("仮シフトを確認し、どうしても休みたい日があれば申請してください。")
        st.warning("※ チームの必要人数を満たしている日のみ申請可能です。申請が重複した場合は抽選となります。")
        
        if df_draft is None or df_draft.empty:
            st.error("仮シフトデータなし")
        else:
            df_draft_idx = df_draft.set_index(df_draft.columns[0])
            if user_name not in df_draft_idx.index:
                st.error("名簿にありません")
            else:
                my_row = df_draft_idx.loc[user_name]
                st.markdown("##### ▼ あなたの仮シフト")
                st.dataframe(pd.DataFrame([my_row.replace({'1':'●','0':'-'})]), use_container_width=True)

                requested_reduce_dates = set()
                if not my_active_reqs.empty:
                    red_reqs = my_active_reqs[my_active_reqs['種別'] == '休み希望']
                    for _, r in red_reqs.iterrows():
                        requested_reduce_dates.add(r['dt'].date())

                available_rest_options = []
                for col in df_draft_idx.columns:
                    if str(df_draft_idx.at[user_name, col]) == '0': continue
                    try:
                        d_obj = pd.to_datetime(f"{target_y}/{col}").date()
                        if d_obj in requested_reduce_dates: continue
                        day_idx = d_obj.day - 1
                        col_data = df_draft_idx[col].to_dict()
                        col_data[user_name] = '0'
                        is_ok, reason = check_daily_constraints(staffs, col_data, req_map, day_idx)
                        if is_ok:
                            available_rest_options.append(col)
                    except: pass
                
                st.divider()
                st.markdown("##### 申請フォーム")
                if not available_rest_options:
                    st.warning("現在、申請可能な日（出勤、かつ未申請、かつ人員余裕あり）はありません。")
                else:
                    with st.form("reduce_work_form"):
                        target_day_str = st.selectbox("休みに変更したい日", available_rest_options)
                        # 備考欄削除
                        if st.form_submit_button("休み申請を送る（抽選対象）"):
                            d_obj = pd.to_datetime(f"{target_y}/{target_day_str}").date()
                            ts = datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S')
                            res, msg = append_row_data("変更申請", [ts, user_name, str(d_obj), "休み希望", "", "申請"])
                            if res: st.success("休み申請を送りました（抽選待ち）"); st.rerun()

                # --- 履歴と取り消し ---
                st.markdown("##### ▼ 申請中の休み希望")
                if not my_active_reqs.empty:
                    reds = my_active_reqs[my_active_reqs['種別'] == '休み希望'].sort_values('dt')
                    if not reds.empty:
                        for i, row in reds.iterrows():
                            with st.container():
                                c1, c2 = st.columns([4, 2])
                                c1.write(f"📅 **{row['日付']}**")
                                if c2.button("取り消し", key=f"cnl_red_{i}"):
                                    update_cell_value("変更申請", row['original_idx'], 6, "取り消し")
                                    st.success("取り消しました"); st.rerun()
                                st.markdown("---")
                    else: st.info("申請中のものはありません")
                else: st.info("申請中のものはありません")

    # ----------------------------------------------------------------
    # 📜 確定シフト
    # ----------------------------------------------------------------
    elif selected_tab == "📜 確定シフト":
        st.subheader("確定シフト")
        
        # --- 追加: 休日消化状況の計算と表示 ---
        # 1. 本人の休日付与数を取得
        staff_master = st.session_state.master_staff
        target_holidays = 0
        if staff_master is not None and not staff_master.empty:
            my_info = staff_master[staff_master['name'] == user_name]
            if not my_info.empty:
                try: 
                    val = my_info.iloc[0]['holiday_target']
                    target_holidays = int(float(val)) if pd.notnull(val) and str(val).strip() != "" else 0
                except: 
                    target_holidays = 0
        
        # 2. ログから今年度の消化休日数を計算
        taken_holidays = 0
        df_log = load_data("ログ", ['日付', '曜日'])
        
        if not df_log.empty and user_name in df_log.columns:
            # 日付型に変換
            df_log['dt_obj'] = pd.to_datetime(df_log['日付'], errors='coerce')
            # 今年のデータのみ抽出
            current_year_logs = df_log[df_log['dt_obj'].dt.year == target_y]
            
            # '0' が休日なのでカウントする
            try:
                col_data = current_year_logs[user_name]
                if isinstance(col_data, pd.DataFrame):
                    # 重複する列名がある場合は最初の列を使用
                    col_data = col_data.iloc[:, 0]
                taken_holidays = int(col_data.apply(lambda x: 1 if str(x)=='0' else 0).sum())
            except:
                taken_holidays = 0
        
        # 安全のために再度キャスト
        try: target_holidays = int(target_holidays)
        except: target_holidays = 0
        try: taken_holidays = int(taken_holidays)
        except: taken_holidays = 0
        
        remaining_holidays = target_holidays - taken_holidays
        
        # 3. メトリクス表示
        st.markdown(f"**📊 {target_y}年度 休日状況**")
        m1, m2, m3 = st.columns(3)
        m1.metric("付与休日", f"{target_holidays}日")
        m2.metric("確定済み休日", f"{taken_holidays}日")
        m3.metric("残休日", f"{remaining_holidays}日", delta_color="normal")
        
        st.divider()
        # ---------------------------------------

        if not df_log.empty and user_name in df_log.columns:
            my_log = df_log[['日付', '曜日', user_name]].copy()
            my_log.columns = ['日付', '曜日', '勤務']
            my_log['勤務'] = my_log['勤務'].apply(lambda x: "✅ 出勤" if str(x)=='1' else "🛌 休み")
            
            # 最新の日付が上に来るようにソート
            my_log['dt_sort'] = pd.to_datetime(my_log['日付'], errors='coerce')
            my_log = my_log.sort_values('dt_sort', ascending=False).drop(columns=['dt_sort'])
            
            st.dataframe(my_log, use_container_width=True)
        else: st.info("履歴はありません")

# =========================================================
# 🔧 管理者画面
# =========================================================
def admin_screen():
    st.sidebar.header("管理者メニュー")
    if st.sidebar.button("ログアウト"):
        st.session_state.user_role = None
        st.rerun()
    
    if st.sidebar.button("🔄 全データ最新化"):
        with st.spinner("同期中..."):
            sync_all_data()
        st.success("完了")
        st.rerun()

    st.sidebar.divider()
    
    # -----------------------------------------------------
    # 📅 処理年月の設定
    # -----------------------------------------------------
    st.sidebar.subheader("📅 処理年月設定")
    with st.sidebar.form("date_selector"):
        current_y = st.session_state.proc_year
        current_m = st.session_state.proc_month
        
        sel_year = st.number_input("年", 2024, 2030, current_y)
        sel_month = st.number_input("月", 1, 12, current_m)
        
        if st.form_submit_button("処理年月を変更する"):
            update_single_config("proc_year", sel_year)
            update_single_config("proc_month", sel_month)
            st.success(f"{sel_year}年{sel_month}月に変更・保存しました")
            st.rerun()

    # -----------------------------------------------------
    # 🛠️ フェーズ管理
    # -----------------------------------------------------
    st.sidebar.divider()
    st.sidebar.subheader("🛠️ 現在のフェーズ")
    current_phase = st.session_state.system_phase
    phase_options = ["0_通常", "1_追加申請", "2_削減申請"]
    
    try: idx = phase_options.index(current_phase)
    except: idx = 0
    
    new_phase = st.sidebar.selectbox("フェーズ切替", phase_options, index=idx)
    
    if new_phase != current_phase:
        if st.sidebar.button("フェーズを変更して保存"):
            update_single_config("current_phase", new_phase)
            st.success(f"フェーズを {new_phase} に変更しました")
            st.rerun()

    year = st.session_state.proc_year
    month = st.session_state.proc_month

    st.title(f"🏥 病院シフト作成ツール (管理者)")
    st.caption(f"現在の処理対象: **{year}年 {month}月**")

    # -----------------------------------------------------
    # ヘルパー関数群 (Admin内)
    # -----------------------------------------------------
    def get_past_week_log_display(year, month, staff_order):
        df = st.session_state.master_log
        if df is None or df.empty: return None
        try:
            df['日付'] = pd.to_datetime(df['日付']).dt.date
            target_first = datetime.date(year, month, 1)
            target_start = target_first - datetime.timedelta(days=7)
            target_end = target_first - datetime.timedelta(days=1)
            mask = (df['日付'] >= target_start) & (df['日付'] <= target_end)
            past_df = df.loc[mask].copy()
            past_df.sort_values('日付', inplace=True)
            display_past = past_df.set_index('日付').transpose()
            ordered_index = [s['name'] for s in staff_order if s['name'] in display_past.index]
            display_past = display_past.reindex(ordered_index)
            display_past = display_past.applymap(lambda x: "●" if str(x)=='1' else ("-" if str(x)=='0' else x))
            return display_past
        except: return None

    def calculate_log_summary(staffs_list, target_year):
        df_log = st.session_state.master_log
        summary = []
        if df_log is None or df_log.empty:
            for s in staffs_list:
                tgt = int(s.get('holiday_target', 0))
                summary.append({"名前": s['name'], "付与休日": tgt, "消化休日": 0, "残休日": tgt})
            return pd.DataFrame(summary).set_index("名前")
        try:
            df_log['日付'] = pd.to_datetime(df_log['日付']).dt.date
            current_year_logs = df_log[df_log['日付'].apply(lambda x: x.year == target_year)]
            for s in staffs_list:
                nm = s['name']
                tgt = int(s.get('holiday_target', 0))
                used = 0
                if nm in current_year_logs.columns:
                    used = current_year_logs[nm].apply(lambda x: 1 if str(x)=='0' else 0).sum()
                summary.append({"名前": nm, "付与休日": tgt, "消化休日": used, "残休日": tgt - used})
        except: pass
        return pd.DataFrame(summary).set_index("名前")

    def calculate_detailed_stats(current_df, staffs_list, year, month):
        past_holidays = {s['name']: 0 for s in staffs_list}
        ldf = st.session_state.master_log
        if ldf is not None and not ldf.empty:
            try:
                ldf['日付'] = pd.to_datetime(ldf['日付']).dt.date
                start_of_target = datetime.date(year, month, 1)
                past_logs = ldf[(ldf['日付'] < start_of_target) & (ldf['日付'].apply(lambda x: x.year == year))]
                for s in staffs_list:
                    nm = s['name']
                    if nm in past_logs.columns:
                        past_holidays[nm] = past_logs[nm].apply(lambda x: 1 if str(x)=='0' else 0).sum()
            except: pass
        stats_data = []
        for s in staffs_list:
            nm = s['name']
            if nm not in current_df.index: stats_data.append({}); continue
            shifts = current_df.loc[nm].values
            month_off = sum(1 for v in shifts if str(v) == '0')
            target = int(s.get('holiday_target', 0))
            p_off = past_holidays.get(nm, 0)
            total_off = p_off + month_off
            remaining = target - total_off
            stats_data.append({"名前": nm, "付与休日": target, "消化休日": total_off, "残休日": remaining})
        return pd.DataFrame(stats_data).set_index("名前")

    def calculate_daily_stats(schedule_df, staff_list, year, month, required_map=None):
        staff_map = {s['name']: s for s in staff_list}
        daily_matrix = {col: [] for col in schedule_df.columns}
        wd_jp = ["月","火","水","木","金","土","日"]
        for col in schedule_df.columns:
            try:
                d_obj = pd.to_datetime(f"{year}/{col}").date()
                w_str = wd_jp[d_obj.weekday()]
                day_idx = d_obj.day - 1
            except:
                w_str = "-"
                day_idx = -1
            req_num = 4
            if required_map and day_idx in required_map:
                req_num = required_map[day_idx]
            working_people = schedule_df.index[schedule_df[col].astype(str) == '1'].tolist()
            c_total = len(working_people)
            c_en = sum(1 for name in working_people if name in staff_map and staff_map[name]['en'])
            c_jp = sum(1 for name in working_people if name in staff_map and staff_map[name]['jp'])
            c_vet = sum(1 for name in working_people if name in staff_map and staff_map[name]['vet'])
            daily_matrix[col] = [w_str, req_num, c_total, c_en, c_jp, c_vet]
        return pd.DataFrame(daily_matrix, index=["曜日", "必要人数", "勤務人数", "English", "Japanese", "Veterans"])
    
    # -----------------------------------------------------
    
    tab_input, tab_create, tab_phase1, tab_phase2, tab_log = st.tabs([
        "📥 ①準備・設定", 
        "📅 ②仮シフト作成", 
        "➕ ③追加申請(Phase1)", 
        "➖ ④削減抽選(Phase2)", 
        "📜 ⑤履歴・ログ"
    ])

    first_weekday, num_days = calendar.monthrange(year, month)
    all_days = range(num_days)

    staff_df = st.session_state.master_staff
    if staff_df is None: staff_df = pd.DataFrame(columns=['id','password','name','role','en','jp','vet','holiday_target'])
    
    active_staff_df = staff_df[staff_df['role'] == 'staff']
    staffs = active_staff_df.to_dict('records')
    staff_name_to_index = {s['name']: i for i, s in enumerate(staffs)}
    all_staff = range(len(staffs))

    ph_indices = set()
    ph_df = st.session_state.master_ph
    if ph_df is not None and not ph_df.empty:
        try:
            ph_df['date'] = pd.to_datetime(ph_df['date']).dt.date
            for _, r in ph_df.iterrows():
                if r['date'].year == year and r['date'].month == month:
                    ph_indices.add(r['date'].day - 1)
        except: pass

    # --- Tab1: 準備 ---
    with tab_input:
        st.markdown("### 1. 準備フェーズ")
        
        with st.expander("🔗 スプレッドシートを開く", expanded=True):
            st.markdown(f"- [データ管理シート (Google Sheets)]({URL_REQUEST_DB})")

        st.caption("※ id, password, role 列がスタッフマスタに必要です")

        c1, c2 = st.columns(2)
        with c1:
            st.subheader("👥 スタッフマスタ")
            edited_s = st.data_editor(staff_df, num_rows="dynamic", key="s_ed")
            if st.button("スタッフ情報をクラウドに保存"):
                save_data("スタッフマスタ", edited_s)
                st.session_state.master_staff = edited_s
                st.success("保存完了")

        with c2:
            st.subheader("㊗️ 公休マスタ")
            if ph_df is None: ph_df = pd.DataFrame(columns=['date','name'])
            edited_p = st.data_editor(ph_df, num_rows="dynamic", key="p_ed")
            if st.button("公休情報をクラウドに保存"):
                save_data("公休マスタ", edited_p)
                st.session_state.master_ph = edited_p
                st.success("保存完了")
        
        st.divider()
        st.subheader(f"📥 申請状況 ({year}年{month}月)")
        
        req_off_filtered = pd.DataFrame()
        req_chg_filtered = pd.DataFrame()

        if st.session_state.req_off_data is not None and not st.session_state.req_off_data.empty:
            temp = st.session_state.req_off_data.copy()
            temp['dt'] = pd.to_datetime(temp['日付'], errors='coerce')
            req_off_filtered = temp[temp['dt'].apply(lambda x: x.year == year and x.month == month if pd.notnull(x) else False)].drop(columns=['dt'])

        if st.session_state.req_chg_data is not None and not st.session_state.req_chg_data.empty:
            temp = st.session_state.req_chg_data.copy()
            temp['dt'] = pd.to_datetime(temp['日付'], errors='coerce')
            req_chg_filtered = temp[temp['dt'].apply(lambda x: x.year == year and x.month == month if pd.notnull(x) else False)].drop(columns=['dt'])

        c_r, c_c = st.columns(2)
        with c_r:
            st.markdown("##### 希望休リスト")
            if not req_off_filtered.empty: st.dataframe(req_off_filtered, use_container_width=True)
            else: st.info(f"{month}月の希望休はありません")
        with c_c:
            st.markdown("##### 変更申請リスト")
            if not req_chg_filtered.empty: st.dataframe(req_chg_filtered, use_container_width=True)
            else: st.info(f"{month}月の変更申請はありません")

    # --- Tab2: 作成 ---
    with tab_create:
        data_key = f"data_req_{year}_{month}"
        
        st.markdown(f"### 2. {year}年{month}月 仮シフト作成")
        st.markdown("#### ▼ 日別 必要人数の設定")
        
        # ① 初期ロード修正対応: 即座にデータを読み込む
        if data_key not in st.session_state:
            req_sheet_data = load_data("draft_requirements")
            init_data = []
            wd_jp = ["月","火","水","木","金","土","日"]
            
            saved_map = {}
            if not req_sheet_data.empty:
                for _, row in req_sheet_data.iterrows():
                    try:
                        # 日付文字列をパースして比較
                        d_obj = pd.to_datetime(row['日付']).date()
                        if d_obj.year == year and d_obj.month == month:
                            count = int(row['必要人数'])
                            saved_map[str(d_obj)] = count
                    except: pass

            for d in all_days:
                date_obj = datetime.date(year, month, d+1)
                w = wd_jp[date_obj.weekday()]
                val = saved_map.get(str(date_obj), 4)
                init_data.append({"日付": date_obj, "曜日": w, "必要人数": val})
            st.session_state[data_key] = pd.DataFrame(init_data, columns=["日付", "曜日", "必要人数"])

        edited_req_df = st.data_editor(
            st.session_state[data_key],
            num_rows="fixed",
            use_container_width=True,
            hide_index=True,
            column_config={
                "日付": st.column_config.DateColumn(format="YYYY-MM-DD", disabled=True),
                "曜日": st.column_config.TextColumn(disabled=True),
                "必要人数": st.column_config.NumberColumn(min_value=0, max_value=20, step=1, format="%d", required=True)
            }
        )

        if st.button("☁️ 必要人数をクラウド保存", type="secondary"):
            if edited_req_df is not None and not edited_req_df.empty:
                st.session_state[data_key] = edited_req_df
                save_df = edited_req_df.copy()
                save_df['日付'] = save_df['日付'].astype(str)
                res, msg = update_requirements_sheet(save_df)
                if res: st.success(msg)
                else: st.error(f"保存エラー: {msg}")

        current_req_map = {}
        if edited_req_df is not None and not edited_req_df.empty:
             for idx, row in edited_req_df.iterrows():
                 if '必要人数' in row:
                     current_req_map[row['日付'].day - 1] = int(row['必要人数'])
        
        st.divider()
        is_dec = (month == 12)
        req_holidays = 0 if is_dec else st.number_input("必要休日数", 8, 20, 11)

        prev_month_history = {}
        past_holidays_count = {s['name']: 0 for s in staffs}
        ldf = st.session_state.master_log
        if ldf is not None and not ldf.empty:
            try:
                ldf['日付'] = pd.to_datetime(ldf['日付']).dt.date
                first_date = datetime.date(year, month, 1)
                for i in reversed(range(1, 5)):
                    td = first_date - datetime.timedelta(days=i)
                    r = ldf[ldf['日付'] == td]
                    if not r.empty:
                        for idx, s in enumerate(staffs):
                            prev_month_history[(idx, -i)] = int(r.iloc[0][s['name']]) if s['name'] in r.columns else 0
                    else:
                        for idx, _ in enumerate(staffs): prev_month_history[(idx, -i)] = 0
                y_logs = ldf[ldf['日付'].apply(lambda x: x.year) == year]
                p_logs = y_logs[y_logs['日付'].apply(lambda x: x.month) != month]
                for s in staffs:
                    if s['name'] in p_logs.columns:
                        past_holidays_count[s['name']] = (p_logs[s['name']].astype(str) == '0').sum()
            except: pass

        if st.button("🚀 計算実行", type="primary"):
            st.session_state.daily_reqs = current_req_map
            
            if not staffs: st.error("スタッフがいません")
            else:
                with st.spinner("AI計算中..."):
                    model = cp_model.CpModel()
                    shifts = {}
                    obj_terms = []

                    for s in all_staff:
                        for d in all_days: shifts[(s, d)] = model.NewBoolVar(f's{s}d{d}')
                    
                    for d in ph_indices:
                        for s in all_staff: model.Add(shifts[(s, d)] == 0)

                    if req_off_filtered is not None and not req_off_filtered.empty:
                        for _, r in req_off_filtered.iterrows():
                            if r.get('ステータス') == '取り消し': continue
                            try:
                                do = pd.to_datetime(r['日付']).date()
                                if do.year==year and do.month==month and r['名前'] in staff_name_to_index:
                                    model.Add(shifts[(staff_name_to_index[r['名前']], do.day-1)] == 0)
                            except: continue

                    if month==1 and num_days>=4:
                        if 3 not in ph_indices:
                            for s in all_staff: model.Add(shifts[(s, 3)] == 1)

                    weekend_idx = [d for d in all_days if d not in ph_indices and (first_weekday+d)%7 >= 5]

                    for d in all_days:
                        if d in ph_indices: continue
                        if month==1 and d==3: continue
                        dw = sum(shifts[(s, d)] for s in all_staff)
                        min_req = current_req_map.get(d, 4)
                        model.Add(dw >= min_req)
                        model.Add(dw <= min_req + 2)
                        is_perfect = model.NewBoolVar(f'perf_{d}')
                        model.Add(dw == min_req).OnlyEnforceIf(is_perfect)
                        model.Add(dw != min_req).OnlyEnforceIf(is_perfect.Not())
                        obj_terms.append(is_perfect.Not() * 50)
                        model.Add(sum(shifts[(s,d)] for s in all_staff if staffs[s]['jp']) >= 1)
                        model.Add(sum(shifts[(s,d)] for s in all_staff if staffs[s]['en']) >= 1)
                        model.Add(sum(shifts[(s,d)] for s in all_staff if staffs[s]['vet']) >= 1)

                    for si, sv in enumerate(staffs):
                        off = sum(1 - shifts[(si, d)] for d in all_days)
                        # 12月ロジックの修正: 「ちょうど使い切る」ように制約強化
                        if is_dec:
                            tgt = int(sv.get('holiday_target', 139))
                            pst = past_holidays_count.get(sv['name'], 0)
                            # 必要な休日数を計算（マイナスにならないよう0以上、月日数を超えないよう上限設定）
                            ned = min(num_days, max(0, tgt - pst))
                            
                            # 制約: 必要数以上とる (実質、必要数に近づける)
                            model.Add(off >= ned)
                            # 目的関数: 超過分を最小化する（＝必要数ピッタリに近づける）
                            obj_terms.append((off - ned) * 200) 
                        else:
                            model.Add(off >= req_holidays)
                            model.Add(off <= req_holidays + 1)
                            obj_terms.append((off - req_holidays) * 100)
                        
                        def gsv(s_i, d_i):
                            if d_i < 0: return prev_month_history.get((s_i, d_i), 0)
                            elif d_i < num_days: return shifts[(s_i, d_i)]
                            return 0
                        
                        for start in range(-4, num_days - 4):
                            w_v = [gsv(si, start+i) for i in range(5)]
                            if any(isinstance(v, cp_model.IntVar) for v in w_v):
                                model.Add(sum(w_v) <= 4)
                        
                        if month != 1:
                            for d in range(num_days - 2):
                                is3off = model.NewBoolVar(f'o3_{si}_{d}')
                                model.Add(sum(shifts[(si, d+i)] for i in range(3))==0).OnlyEnforceIf(is3off)
                                model.Add(sum(shifts[(si, d+i)] for i in range(3))>0).OnlyEnforceIf(is3off.Not())
                                obj_terms.append(is3off * 50)
                        
                        for d in range(1, num_days-1):
                            if month==1 and d==3: continue
                            model.AddBoolOr([shifts[(si, d-1)], shifts[(si, d+1)]]).OnlyEnforceIf(shifts[(si, d)])

                        if weekend_idx:
                             wc = model.NewIntVar(0, len(weekend_idx), f'wc_{si}')
                             model.Add(wc == sum(shifts[(si, d)] for d in weekend_idx))
                             sq = model.NewIntVar(0, len(weekend_idx)**2, f'sq_{si}')
                             model.AddMultiplicationEquality(sq, [wc, wc])
                             obj_terms.append(sq * 200)

                    model.Minimize(sum(obj_terms))
                    solver = cp_model.CpSolver()
                    solver.parameters.max_time_in_seconds = 15.0
                    status = solver.Solve(model)

                    if status in [cp_model.OPTIMAL, cp_model.FEASIBLE]:
                        res = {}
                        for d in all_days:
                            res[f"{month}/{d+1}"] = [solver.Value(shifts[(s,d)]) for s in all_staff]
                        df_res = pd.DataFrame(res, index=[s['name'] for s in staffs])
                        st.session_state.schedule_df = df_res
                        st.success("計算完了。下のボタンで保存してください")
                    else:
                        st.error("作成失敗：条件を見直してください")

        display_df = None
        is_unsaved = False

        if st.session_state.schedule_df is not None:
             display_df = st.session_state.schedule_df
             is_unsaved = True
        else:
             loaded = load_data("draft_schedule")
             if not loaded.empty:
                 display_df = loaded.set_index(loaded.columns[0])
        
        if display_df is not None:
            st.markdown("##### ▼ 仮シフト表")
            
            if is_unsaved:
                st.warning("⚠️ このシフトはまだ保存されていません。")
                if st.button("💾 仮シフトを保存・公開し、Phase1へ移行", type="primary"):
                    upload_df = display_df.copy()
                    upload_df.insert(0, "名前", upload_df.index)
                    save_data("draft_schedule", upload_df)
                    
                    update_single_config("current_phase", "1_追加申請")
                    
                    st.success("仮シフトを保存し、フェーズを「1_追加申請」に変更しました！")
                    st.session_state.schedule_df = None
                    st.rerun()

            c_past, c_curr = st.columns([1, 3])
            with c_past:
                st.caption("直近7日間 (実績)")
                display_past = get_past_week_log_display(year, month, staffs)
                if display_past is not None and not display_past.empty:
                      st.dataframe(display_past, use_container_width=True)
                else:
                      st.info("データなし")

            with c_curr:
                st.caption(f"{month}月 仮シフト")
                st.dataframe(display_df.replace({1:"●",0:"-"}))
            
            st.markdown("##### ▼ 日別スタッフ配置数")
            st.dataframe(calculate_daily_stats(display_df, staffs, year, month, current_req_map))

            st.markdown("##### ▼ 休日取得状況 (予測)")
            stats_df = calculate_detailed_stats(display_df, staffs, year, month)
            st.dataframe(stats_df)
        else:
            st.info("仮シフトデータはありません")

    # --- Tab3: 追加申請処理 (Phase 1) ---
    with tab_phase1:
        st.markdown(f"### 3. 追加申請の反映 ({year}年{month}月)")
        st.info("「出勤希望」の申請を処理します。原則すべて受け入れます。")
        
        if current_phase == "1_追加申請":
            req_chg = load_data("変更申請")
            target_reqs = []
            if not req_chg.empty:
                req_chg['dt'] = pd.to_datetime(req_chg['日付'], errors='coerce')
                mask = (req_chg['dt'].dt.year == year) & (req_chg['dt'].dt.month == month) & \
                       (req_chg['種別'] == '出勤希望') & (req_chg['ステータス'] == '申請')
                target_reqs = req_chg[mask].to_dict('records')
            
            if not target_reqs:
                st.info("現在、処理待ちの出勤申請はありません。")
            else:
                st.write(f"未処理: {len(target_reqs)}件")
                disp_cols = ['名前','日付']
                if '備考' in pd.DataFrame(target_reqs).columns: disp_cols.append('備考')
                st.dataframe(pd.DataFrame(target_reqs)[disp_cols], use_container_width=True)
                
            st.markdown("---")
            if st.button("追加申請を反映（あれば）して、Phase2へ移行", type="primary"):
                df_draft = load_data("draft_schedule")
                
                if df_draft.empty:
                    st.error("仮シフトがありません")
                else:
                    df_draft = df_draft.set_index(df_draft.columns[0])
                    cnt = 0
                    if target_reqs:
                        for r in target_reqs:
                            nm = r['名前']
                            d_str = f"{r['dt'].month}/{r['dt'].day}"
                            if nm in df_draft.index and d_str in df_draft.columns:
                                df_draft.at[nm, d_str] = '1'
                                cnt += 1
                        
                        save_df = df_draft.copy()
                        save_df.insert(0, "名前", save_df.index)
                        save_data("draft_schedule", save_df)
                        
                        for idx in req_chg.index:
                            row = req_chg.loc[idx]
                            if row['dt'].year == year and row['dt'].month == month and \
                               row['種別'] == '出勤希望' and row['ステータス'] == '申請':
                                req_chg.at[idx, 'ステータス'] = '承認'
                        
                        if 'dt' in req_chg.columns: del req_chg['dt']
                        save_data("変更申請", req_chg)
                    
                    update_single_config("current_phase", "2_削減申請")
                    st.success(f"{cnt}件を反映し、フェーズを「2_削減申請」に変更しました！")
                    st.rerun()
        else:
            st.info(f"現在は「{current_phase}」のため、この機能は使用できません。")

    # --- Tab4: 削減申請処理 (Phase 2) ---
    with tab_phase2:
        st.markdown(f"### 4. 削減申請の処理 ({year}年{month}月)")
        st.info("「休み希望」の申請を処理します。重複や条件割れは抽選で却下されます。")
        
        if current_phase == "2_削減申請":
            req_chg = load_data("変更申請")
            reduce_reqs = []
            if not req_chg.empty:
                req_chg['dt'] = pd.to_datetime(req_chg['日付'], errors='coerce')
                mask = (req_chg['dt'].dt.year == year) & (req_chg['dt'].dt.month == month) & \
                       (req_chg['種別'] == '休み希望') & (req_chg['ステータス'] == '申請')
                reduce_reqs = req_chg[mask].to_dict('records')
                
            if not reduce_reqs:
                st.info("現在、処理待ちの削減申請はありません")
            else:
                st.write(f"申請件数: {len(reduce_reqs)}件")
                
            st.markdown("---")
            st.warning("⚠️ このボタンを押すと、抽選（あれば）を行い、仮シフトを確定ログに保存して、フェーズを「0_通常」に戻します。")
            
            if st.button("抽選・確定処理を実行し、Phase0へ完了移行", type="primary"):
                df_draft = load_data("draft_schedule")
                if df_draft.empty:
                    st.error("仮シフトなし"); st.stop()
                df_draft = df_draft.set_index(df_draft.columns[0])
                
                # --- 1. 抽選処理 ---
                logs = []
                approved_count = 0
                rejected_count = 0
                
                if reduce_reqs:
                    random.shuffle(reduce_reqs)
                    req_map = {}
                    req_df = load_data("draft_requirements")
                    if not req_df.empty:
                        for _, r in req_df.iterrows():
                            try:
                                d = pd.to_datetime(r['日付'])
                                if d.year == year and d.month == month:
                                    req_map[d.day - 1] = int(r['必要人数'])
                            except: pass
                    
                    for r in reduce_reqs:
                        nm = r['名前']
                        d_str = f"{r['dt'].month}/{r['dt'].day}"
                        day_idx = r['dt'].day - 1
                        ts_key = r['タイムスタンプ']
                        
                        if nm not in df_draft.index or d_str not in df_draft.columns: continue
                        if str(df_draft.at[nm, d_str]) == '0': continue
                            
                        current_col = df_draft[d_str].to_dict()
                        current_col[nm] = '0'
                        is_ok, reason = check_daily_constraints(staffs, current_col, req_map, day_idx)
                        row_idx_in_df = req_chg[req_chg['タイムスタンプ'] == ts_key].index
                        
                        if is_ok:
                            df_draft.at[nm, d_str] = '0'
                            approved_count += 1
                            logs.append(f"✅ 承認: {nm} {d_str}")
                            if not row_idx_in_df.empty: req_chg.at[row_idx_in_df[0], 'ステータス'] = '承認'
                        else:
                            rejected_count += 1
                            logs.append(f"❌ 却下: {nm} {d_str} ({reason})")
                            if not row_idx_in_df.empty: req_chg.at[row_idx_in_df[0], 'ステータス'] = '却下'
                    
                    if 'dt' in req_chg.columns: del req_chg['dt']
                    save_data("変更申請", req_chg)
                
                # --- 2. 確定ログ保存処理 ---
                new_logs = []
                for c in df_draft.columns:
                    try:
                        dt = pd.to_datetime(f"{year}/{c}").date()
                        wd = ["月","火","水","木","金","土","日"][dt.weekday()]
                        row_dict = {"日付": dt, "曜日": wd}
                        for nm in df_draft.index:
                            row_dict[nm] = df_draft.at[nm, c]
                        new_logs.append(row_dict)
                    except: pass
                
                if new_logs:
                    update_log_sheet(pd.DataFrame(new_logs))
                    clear_sheet_data("draft_schedule")
                    clear_sheet_data("draft_requirements")
                    
                    # --- 3. フェーズリセット ---
                    update_single_config("current_phase", "0_通常")
                    
                    st.success(f"処理完了！ (承認:{approved_count}件, 却下:{rejected_count}件)。確定ログを保存し、フェーズを「0_通常」に戻しました。")
                    st.balloons()
                    st.session_state.schedule_df = None
                    sync_all_data()
                    
                    with st.expander("詳細ログ", expanded=True):
                        for l in logs: st.write(l)
                    
                    time.sleep(3)
                    st.rerun()
        else:
            st.info(f"現在は「{current_phase}」のため、この機能は使用できません。")

    # --- Tab5: ログ・最終確定 (編集機能追加・全期間表示) ---
    with tab_log:
        st.subheader("📊 確定シフト (全期間)")
        
        df_log = st.session_state.master_log
        if df_log is not None and not df_log.empty:
            # 日付で降順ソート
            df_log['dt'] = pd.to_datetime(df_log['日付'], errors='coerce')
            df_sorted = df_log.sort_values('dt', ascending=False).drop(columns=['dt'])
            
            st.markdown("##### ▼ 編集モード")
            st.info("日付と曜日以外は編集可能です。修正後は必ず「修正内容を保存」ボタンを押してください。")
            
            # data_editorで全期間表示・編集可能に
            edited_log = st.data_editor(
                df_sorted,
                use_container_width=True,
                disabled=["日付", "曜日"],
                key="log_editor_full"
            )
            
            if st.button("修正内容を保存する"):
                # 編集されたデータ全体を保存（save_dataで全上書き）
                save_target = edited_log.copy()
                # 日付昇順に戻してから保存した方が綺麗
                save_target['dt'] = pd.to_datetime(save_target['日付'], errors='coerce')
                save_target = save_target.sort_values('dt', ascending=True).drop(columns=['dt'])
                
                res, msg = save_data("ログ", save_target)
                if res:
                    st.success("全期間の修正内容を保存しました！")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error(f"保存エラー: {msg}")
        else:
            st.info("ログはありません")

if st.session_state.user_role == "admin": admin_screen()
elif st.session_state.user_role == "staff": staff_screen()
else: login_screen()
