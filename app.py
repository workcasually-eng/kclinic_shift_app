import streamlit as st
import pandas as pd
from ortools.sat.python import cp_model
import calendar
import datetime
import os
import gspread
from google.oauth2.service_account import Credentials

# =========================================================
# ⚙️ 設定エリア
# =========================================================
DEFAULT_ADMIN_PASSWORD = "1234"
# スーパー管理者（緊急用）
DEFAULT_SUPER_ADMIN_ID = "root"
DEFAULT_SUPER_ADMIN_PASS = "1234"

# 全てのデータ（マスタ、申請、ログ、仮シフト、完成シフト）をこのシートで管理します
URL_REQUEST_DB = "https://docs.google.com/spreadsheets/d/1y7H-9c2EJhpCKoXY6Va_RRx3dfDZoarxlUmQLdXEP6o/edit"

# 参照用リンク（管理者画面Tab1用）
URL_REQ_SHEET = URL_REQUEST_DB

# =========================================================
# 🚀 アプリ初期設定
# =========================================================
st.set_page_config(page_title="病院シフト管理アプリ", layout="wide")

# セッション状態初期化
if 'user_role' not in st.session_state: st.session_state.user_role = None
if 'user_name' not in st.session_state: st.session_state.user_name = None
if 'schedule_df' not in st.session_state: st.session_state.schedule_df = None

# データキャッシュ
if 'master_staff' not in st.session_state: st.session_state.master_staff = None
if 'master_ph' not in st.session_state: st.session_state.master_ph = None
if 'master_log' not in st.session_state: st.session_state.master_log = None
if 'req_off_data' not in st.session_state: st.session_state.req_off_data = None
if 'req_chg_data' not in st.session_state: st.session_state.req_chg_data = None

# シミュレーション関連
if 'simulated_df' not in st.session_state: st.session_state.simulated_df = None
if 'sim_logs' not in st.session_state: st.session_state.sim_logs = []
if 'loaded_requests' not in st.session_state: st.session_state.loaded_requests = []

# 日別必要人数の保持用
if 'daily_reqs' not in st.session_state: st.session_state.daily_reqs = {}

# 仮シフト一時保存用
draft_csv_path = 'draft_schedule.csv'

# =========================================================
# 🛠️ ヘルパー関数 (GSheet操作一元化)
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
    """シートに接続、なければ作成する汎用関数"""
    client = get_gspread_client()
    if not client: return None, "認証エラー"
    
    try:
        spreadsheet = client.open_by_url(URL_REQUEST_DB)
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except:
            # シートがない場合は作成
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=20)
            if headers:
                worksheet.append_row(headers)
        return worksheet, None
    except Exception as e:
        return None, str(e)

# --- データ読み書き用 ---

def load_data(sheet_name, expected_headers=None):
    """スプレッドシートからデータを読み込みDataFrameで返す"""
    ws, err = connect_sheet(sheet_name, expected_headers)
    if err: return pd.DataFrame(columns=expected_headers or [])
    
    try:
        data = ws.get_all_records()
        if not data: return pd.DataFrame(columns=expected_headers or [])
        # 全てのデータを文字列として読み込む
        return pd.DataFrame(data).astype(str)
    except:
        return pd.DataFrame(columns=expected_headers or [])

def save_data(sheet_name, df):
    """DataFrameの内容でスプレッドシートを全上書きする"""
    ws, err = connect_sheet(sheet_name)
    if err: return False, err
    
    try:
        ws.clear()
        upload_df = df.fillna("")
        # カラム名を1行目として追加
        upload_data = [upload_df.columns.tolist()] + upload_df.values.tolist()
        ws.update(upload_data)
        return True, "保存完了"
    except Exception as e:
        return False, str(e)

def clear_sheet_data(sheet_name):
    """シートの中身を完全に消去する（仮シフト削除用）"""
    ws, err = connect_sheet(sheet_name)
    if err: return False
    try:
        ws.clear()
        return True
    except: return False

def append_row_data(sheet_name, row_list):
    """リストデータを1行追記する"""
    ws, err = connect_sheet(sheet_name)
    if err: return False, err
    try:
        ws.append_row(row_list)
        return True, "追加完了"
    except Exception as e:
        return False, str(e)

def update_cell_value(sheet_name, row_idx, col_idx, value):
    """特定セルの更新"""
    ws, err = connect_sheet(sheet_name)
    if err: return False
    try:
        ws.update_cell(row_idx, col_idx, value)
        return True
    except: return False

def update_log_sheet(new_df):
    """ログシートに新しい確定シフトデータをマージして保存する"""
    current_df = load_data("ログ", ['日付', '曜日'])
    
    if not current_df.empty:
        current_df['日付'] = pd.to_datetime(current_df['日付']).dt.date
    if not new_df.empty:
        new_df['日付'] = pd.to_datetime(new_df['日付']).dt.date
    
    if not new_df.empty and not current_df.empty:
        target_month = new_df.iloc[0]['日付'].month
        target_year = new_df.iloc[0]['日付'].year
        current_df = current_df[~current_df['日付'].apply(lambda x: x.year == target_year and x.month == target_month)]
    
    combined = pd.concat([current_df, new_df], ignore_index=True)
    combined.sort_values('日付', inplace=True)
    
    combined['日付'] = combined['日付'].astype(str)
    return save_data("ログ", combined)

def update_requirements_sheet(new_df):
    """必要人数シート（draft_requirements）に新しい月データをマージして保存する"""
    current_df = load_data("draft_requirements", ['日付', '曜日', '必要人数'])
    
    # 型変換
    if not current_df.empty:
        current_df['日付'] = pd.to_datetime(current_df['日付']).dt.date
    if not new_df.empty:
        new_df['日付'] = pd.to_datetime(new_df['日付']).dt.date
    
    # 既存データから、今回更新する月のデータを削除（上書き）
    if not new_df.empty and not current_df.empty:
        target_month = new_df.iloc[0]['日付'].month
        target_year = new_df.iloc[0]['日付'].year
        current_df = current_df[~current_df['日付'].apply(lambda x: x.year == target_year and x.month == target_month)]
    
    combined = pd.concat([current_df, new_df], ignore_index=True)
    combined.sort_values('日付', inplace=True)
    
    combined['日付'] = combined['日付'].astype(str)
    return save_data("draft_requirements", combined)

# =========================================================
# 📦 データマネージャ
# =========================================================
def sync_all_data():
    st.session_state.master_staff = load_data("スタッフマスタ", ['id', 'password', 'name', 'role', 'en', 'jp', 'vet', 'holiday_target'])
    if not st.session_state.master_staff.empty:
        for col in ['en','jp','vet']:
            if col in st.session_state.master_staff.columns:
                st.session_state.master_staff[col] = st.session_state.master_staff[col].apply(lambda x: str(x).upper()=='TRUE')

    st.session_state.master_ph = load_data("公休マスタ", ['date', 'name'])
    st.session_state.master_log = load_data("ログ", ['日付', '曜日'])
    st.session_state.req_off_data = load_data("希望休", ["タイムスタンプ", "名前", "日付", "備考", "ステータス"])
    st.session_state.req_chg_data = load_data("変更申請", ["タイムスタンプ", "名前", "日付", "種別", "備考", "ステータス"])

# =========================================================
# 🚪 ログイン画面 (ID/PASS方式)
# =========================================================
def login_screen():
    st.title("🏥 シフト管理システム (Cloud完全版)")
    st.markdown("IDとパスワードを入力してログインしてください。")

    with st.form("login_form"):
        user_id = st.text_input("ユーザーID")
        password = st.text_input("パスワード", type="password")
        submit = st.form_submit_button("ログイン")

        if submit:
            input_id = user_id.strip()
            input_pass = password.strip()

            # スーパー管理者
            if input_id == DEFAULT_SUPER_ADMIN_ID and input_pass == DEFAULT_SUPER_ADMIN_PASS:
                st.session_state.user_role = "admin"
                st.session_state.user_name = "Super Admin"
                with st.spinner("データ同期中..."):
                    sync_all_data()
                st.success("スーパー管理者としてログインしました")
                st.rerun()

            # スタッフマスタ認証
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
    st.sidebar.title(f"👤 {user_name}")
    if st.sidebar.button("ログアウト", type="primary"):
        st.session_state.user_role = None
        st.rerun()

    today = datetime.date.today()
    if today.day <= 10: target_m = today.month + 2
    else: target_m = today.month + 3
    target_y = today.year
    while target_m > 12:
        target_m -= 12
        target_y += 1
    default_date = datetime.date(target_y, target_m, 1)

    st.title("スタッフ用ダッシュボード")
    tab1, tab2, tab3 = st.tabs(["📝 希望休申請", "📅 仮シフト・変更申請", "📜 確定シフト履歴"])

    with tab1:
        st.subheader("希望休の申請")
        with st.form("req_form"):
            c1, c2 = st.columns(2)
            d = c1.date_input("日付", value=default_date)
            n = c2.text_input("備考")
            if st.form_submit_button("送信"):
                ts = datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S')
                res, msg = append_row_data("希望休", [ts, user_name, str(d), n, "申請"])
                if res: st.success("申請しました"); st.rerun()
                else: st.error(msg)

        st.divider()
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
                        with ca: st.write(f"📅 **{r['日付']}**　{r.get('備考', '')}")
                        with cb:
                            if st.button("取り消し", key=f"can_req_{i}"):
                                update_cell_value("希望休", r['_row_idx'], 5, "取り消し")
                                st.success("取り消しました"); st.rerun()
                        st.markdown("---")
            else: st.info("有効な申請はありません")
        else: st.info("申請はありません")

    with tab2:
        st.subheader("仮シフト確認")
        
        df_draft = load_data("draft_schedule")
        draft_exists = False
        
        if not df_draft.empty:
            df_draft = df_draft.set_index(df_draft.columns[0])
            if user_name in df_draft.index:
                draft_exists = True
                my_row = df_draft.loc[user_name]
                data = [{"日付": c, "予定": "✅ 出勤" if str(v)=='1' else "🛌 休み"} for c, v in my_row.items()]
                st.dataframe(pd.DataFrame(data), use_container_width=True)
            else: st.warning("仮シフトに名前がありません")
        else: st.info("現在、仮シフトはありません")

        if draft_exists:
            st.divider()
            st.subheader("🔄 変更申請")
            with st.form("chg_form"):
                cc1, cc2 = st.columns(2)
                cd = cc1.date_input("変更したい日付")
                ct = cc2.selectbox("希望する状態", ["休み希望", "出勤希望"])
                cn = st.text_input("理由")
                if st.form_submit_button("変更申請を送る"):
                    ts = datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S')
                    res, msg = append_row_data("変更申請", [ts, user_name, str(cd), ct, cn, "申請"])
                    if res: st.success("変更申請しました"); st.rerun()
                    else: st.error(msg)
            
            st.markdown("##### ▼ 変更申請履歴")
            df_chg = load_data("変更申請", ["タイムスタンプ", "名前", "日付", "種別", "備考", "ステータス"])
            if not df_chg.empty:
                valid_recs = []
                for i, r in enumerate(df_chg.to_dict('records')):
                    if r['名前'] == user_name:
                        r['_row_idx'] = i + 2
                        valid_recs.append(r)
                valid_recs = [r for r in valid_recs if r.get('ステータス') != '取り消し']
                valid_recs = sorted(valid_recs, key=lambda x: x['日付'])

                if valid_recs:
                    for i, r in enumerate(valid_recs):
                        with st.container():
                            c1, c2, c3 = st.columns([3, 3, 2])
                            c1.write(f"📅 **{r['日付']}**")
                            c2.write(f"**{r['種別']}** {r.get('備考','')}")
                            if c3.button("取り消し", key=f"can_chg_{i}"):
                                update_cell_value("変更申請", r['_row_idx'], 6, "取り消し")
                                st.success("取り消しました"); st.rerun()
                            st.markdown("---")
                else: st.info("変更申請はありません")
            else: st.info("変更申請はありません")

    with tab3:
        st.subheader("確定シフト履歴")
        df_log = load_data("ログ", ['日付', '曜日'])
        if not df_log.empty and user_name in df_log.columns:
            my_log = df_log[['日付', '曜日', user_name]].copy()
            my_log.columns = ['日付', '曜日', '勤務']
            my_log['勤務'] = my_log['勤務'].apply(lambda x: "✅ 出勤" if str(x)=='1' else "🛌 休み")
            st.dataframe(my_log.sort_values('日付', ascending=False), use_container_width=True)
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

    st.title("🏥 病院シフト作成ツール (管理者・Cloud版)")

    # -----------------------------------------------------
    # ヘルパー関数群
    # -----------------------------------------------------
    def get_past_week_log(year, month):
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
            return past_df
        except: return None

    def calculate_log_summary(staffs_list, target_year):
        df_log = st.session_state.master_log
        summary = []
        if df_log is None or df_log.empty:
            for s in staffs_list:
                tgt = int(s.get('holiday_target', 0))
                summary.append({"名前": s['name'], "付与公休": tgt, "消化公休": 0, "残公休": tgt})
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
                summary.append({"名前": nm, "付与公休": tgt, "消化公休": used, "残公休": tgt - used})
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
            
            # ★修正点: 文字列の '0' も数値の 0 も休みとしてカウント
            month_off = sum(1 for v in shifts if str(v) == '0')
            target = int(s.get('holiday_target', 0))
            p_off = past_holidays.get(nm, 0)
            total_off = p_off + month_off
            remaining = target - total_off
            stats_data.append({"名前": nm, "付与公休": target, "消化公休": total_off, "残公休": remaining})
        return pd.DataFrame(stats_data).set_index("名前")

    def calculate_daily_stats(schedule_df, staff_list, year, month, required_map=None):
        """日別配置数を計算 (曜日・必要人数を追加)"""
        staff_map = {s['name']: s for s in staff_list}
        daily_matrix = {col: [] for col in schedule_df.columns}
        wd_jp = ["月","火","水","木","金","土","日"]

        for col in schedule_df.columns:
            # 曜日取得
            try:
                d_obj = pd.to_datetime(f"{year}/{col}").date()
                w_str = wd_jp[d_obj.weekday()]
                day_idx = d_obj.day - 1
            except:
                w_str = "-"
                day_idx = -1

            # 必要人数取得
            req_num = 4 # default
            if required_map and day_idx in required_map:
                req_num = required_map[day_idx]

            # ★修正点: 文字列の '1' を出勤としてカウント
            working_people = schedule_df.index[schedule_df[col].astype(str) == '1'].tolist()
            
            c_total = len(working_people)
            c_en = sum(1 for name in working_people if name in staff_map and staff_map[name]['en'])
            c_jp = sum(1 for name in working_people if name in staff_map and staff_map[name]['jp'])
            c_vet = sum(1 for name in working_people if name in staff_map and staff_map[name]['vet'])
            
            # リストに「曜日」「必要人数」を追加
            daily_matrix[col] = [w_str, req_num, c_total, c_en, c_jp, c_vet]

        # インデックスにも追加
        return pd.DataFrame(daily_matrix, index=["曜日", "必要人数", "勤務人数", "English", "Japanese", "Veterans"])

    def check_daily_constraints(staffs_list, shift_column, required_count_map=None, current_day=None):
        # ★修正点: 文字列の '1' を出勤としてカウント
        working_staffs = [s for i, s in enumerate(staffs_list) if str(shift_column.iloc[i]) == '1']
        
        # 最低人数チェック
        required = 4 # Default
        if required_count_map and current_day is not None:
            required = required_count_map.get(current_day, 4)
            
        if len(working_staffs) < required:
            return False, f"人数不足(必要{required}人 -> 現在{len(working_staffs)}人)"
            
        if sum(1 for s in working_staffs if s['jp']) < 1: return False, "日本語話者不足"
        if sum(1 for s in working_staffs if s['en']) < 1: return False, "英語話者不足"
        if sum(1 for s in working_staffs if s['vet']) < 1: return False, "ベテラン不足"
        return True, "OK"

    # -----------------------------------------------------
    
    tab_input, tab_create, tab_finalize, tab_log = st.tabs(["📥 ①準備・設定", "📅 ②仮シフト作成", "🔄 ③変更申請・確定", "📜 履歴・ログ"])

    st.sidebar.divider()
    today = datetime.date.today()
    if today.day <= 10: target_m = today.month + 2
    else: target_m = today.month + 3
    def_y = today.year
    while target_m > 12:
        target_m -= 12
        def_y += 1
    
    col_y, col_m = st.sidebar.columns(2)
    with col_y: year = st.number_input("年", 2024, 2030, def_y)
    with col_m: month = st.number_input("月", 1, 12, target_m)

    first_weekday, num_days = calendar.monthrange(year, month)
    all_days = range(num_days)

    # セッションからマスタ取得
    staff_df = st.session_state.master_staff
    if staff_df is None: staff_df = pd.DataFrame(columns=['id','password','name','role','en','jp','vet','holiday_target'])
    
    # role='staff' のみ抽出 (admin除外)
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
        with st.expander("🔗 リンク集"):
            st.write(f"スプレッドシート: {URL_REQ_SHEET}")

        c1, c2 = st.columns(2)
        with c1:
            st.subheader("👥 スタッフマスタ (編集可)")
            st.caption("※ id, password, role 列を追加してください")
            edited_s = st.data_editor(staff_df, num_rows="dynamic", key="s_ed")
            if st.button("スタッフ情報をクラウドに保存"):
                res, msg = save_data("スタッフマスタ", edited_s)
                if res: st.success(msg); st.session_state.master_staff = edited_s
                else: st.error(msg)

        with c2:
            st.subheader("㊗️ 公休マスタ (編集可)")
            if ph_df is None: ph_df = pd.DataFrame(columns=['date','name'])
            edited_p = st.data_editor(ph_df, num_rows="dynamic", key="p_ed")
            if st.button("公休情報をクラウドに保存"):
                res, msg = save_data("公休マスタ", edited_p)
                if res: st.success(msg); st.session_state.master_ph = edited_p
                else: st.error(msg)
        
        st.divider()
        st.subheader(f"📥 申請状況 ({year}年{month}月)")
        st.caption("※ ログイン時にスプレッドシートから自動同期済")
        
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
        st.markdown(f"### 2. {year}年{month}月 仮シフト作成")

        st.markdown("#### ▼ 直近1週間の確定シフト (連勤確認用)")
        past_log_df = get_past_week_log(year, month)
        if past_log_df is not None and not past_log_df.empty:
            display_past = past_log_df.set_index('日付').transpose()
            display_past = display_past.applymap(lambda x: "●" if str(x)=='1' else ("-" if str(x)=='0' else x))
            st.dataframe(display_past, use_container_width=True)
        else: st.info("直近の履歴はありません")

        st.divider()
        
        # --- 日別必要人数の設定 (自動保存) ---
        st.markdown("#### ▼ 日別 必要人数の設定")
        st.caption("※「必要人数」を変更すると自動保存されます")
        
        data_key = f"data_req_{year}_{month}"
        editor_key = f"editor_req_{year}_{month}"
        
        # 1. 保存データの読み込み
        if data_key not in st.session_state:
            req_sheet_data = load_data("draft_requirements")
            init_data = []
            wd_jp = ["月","火","水","木","金","土","日"]
            
            saved_map = {}
            if not req_sheet_data.empty:
                for _, row in req_sheet_data.iterrows():
                    try:
                        d_str = str(row['日付'])
                        count = int(row['必要人数'])
                        saved_map[d_str] = count
                    except: pass

            for d in all_days:
                date_obj = datetime.date(year, month, d+1)
                w = wd_jp[date_obj.weekday()]
                val = saved_map.get(str(date_obj), 4)
                init_data.append({"日付": date_obj, "曜日": w, "必要人数": val})
            
            st.session_state[data_key] = pd.DataFrame(init_data, columns=["日付", "曜日", "必要人数"])

        # 2. 自動保存用コールバック
        def auto_save_reqs():
            edited_df = st.session_state[editor_key]
            if "日付" in edited_df.columns and "必要人数" in edited_df.columns:
                st.session_state[data_key] = edited_df
                save_df = edited_df.copy()
                save_df['日付'] = save_df['日付'].astype(str)
                update_requirements_sheet(save_df)
                st.toast("必要人数を保存しました", icon="💾")

        # 3. エディタ表示
        st.data_editor(
            st.session_state[data_key],
            key=editor_key, 
            on_change=auto_save_reqs,
            num_rows="fixed",
            use_container_width=True,
            hide_index=True,
            column_config={
                "日付": st.column_config.DateColumn(format="YYYY-MM-DD", disabled=True),
                "曜日": st.column_config.TextColumn(disabled=True),
                "必要人数": st.column_config.NumberColumn(
                    min_value=0, max_value=20, step=1, format="%d", required=True
                )
            }
        )
        
        current_req_map = {}
        if data_key in st.session_state and not st.session_state[data_key].empty:
             for idx, row in st.session_state[data_key].iterrows():
                 if '必要人数' in row:
                     current_req_map[idx] = int(row['必要人数'])
        
        st.divider()
        is_dec = (month == 12)
        req_holidays = 0 if is_dec else st.number_input("必要休日数", 8, 20, 11)

        # 過去ログ集計
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
                        if is_dec:
                            tgt = int(sv.get('holiday_target', 139))
                            pst = past_holidays_count.get(sv['name'], 0)
                            ned = max(0, tgt - pst)
                            model.Add(off >= ned)
                            obj_terms.append((off - ned) * 100)
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
                        
                        upload_df = df_res.copy()
                        upload_df.insert(0, "名前", upload_df.index)
                        save_data("draft_schedule", upload_df)
                        
                        # ★必要人数設定も保存 (draft_requirements)
                        req_save_df = st.session_state[data_key].copy()
                        req_save_df['日付'] = req_save_df['日付'].astype(str)
                        update_requirements_sheet(req_save_df)

                        st.session_state.schedule_df = df_res
                        st.success("計算完了 (仮シフトを保存しました)")
                    else:
                        st.error("作成失敗：条件を見直してください")

        # 仮シフト表示
        df_draft = load_data("draft_schedule")
        if not df_draft.empty:
            df_d = df_draft.set_index(df_draft.columns[0])
            st.markdown("##### ▼ 仮シフト表")
            st.dataframe(df_d.replace({1:"●",0:"-"}))
            
            st.markdown("##### ▼ 日別スタッフ配置数")
            # 引数に year, month, current_req_map を渡す
            st.dataframe(calculate_daily_stats(df_d, staffs, year, month, current_req_map))

            st.markdown("##### ▼ 休日取得状況 (予測)")
            stats_df = calculate_detailed_stats(df_d, staffs, year, month)
            st.dataframe(stats_df)

    # --- Tab3: 確定 ---
    with tab_finalize:
        st.markdown("### 3. 確定 & Web公開")
        
        st.markdown("#### ▼ 直近1週間の確定シフト (Context)")
        past_log_df = get_past_week_log(year, month)
        if past_log_df is not None and not past_log_df.empty:
            display_past = past_log_df.set_index('日付').transpose()
            display_past = display_past.applymap(lambda x: "●" if str(x)=='1' else ("-" if str(x)=='0' else x))
            st.dataframe(display_past, use_container_width=True)

        st.divider()
        
        # 仮シフト読み込み
        df_draft = load_data("draft_schedule")
        
        if not df_draft.empty:
            base_df = df_draft.set_index(df_draft.columns[0])
            base_df = base_df.apply(pd.to_numeric, errors='coerce').fillna(0).astype(int)
            
            st.markdown("#### 変更申請の処理")
            
            if st.button("読み込みデータをリセット"):
                st.session_state.loaded_requests = []
                st.session_state.simulated_df = None
                st.session_state.sim_logs = []
                st.rerun()

            valid_c = []
            if req_chg_filtered is not None and not req_chg_filtered.empty:
                for r in req_chg_filtered.to_dict('records'):
                    s = r.get('ステータス') or '申請'
                    if s != '取り消し':
                        valid_c.append({
                            'name': r['名前'], 'date': r['日付'], 'type': r['種別'], 'source': 'Cloud'
                        })
                st.session_state.loaded_requests = valid_c
            else:
                st.session_state.loaded_requests = []

            if st.session_state.loaded_requests:
                st.markdown("##### ▼ 有効な変更申請リスト")
                req_disp = pd.DataFrame(st.session_state.loaded_requests)
                if not req_disp.empty:
                    st.dataframe(req_disp, use_container_width=True)

                if st.button("シミュレーション実行", type="primary"):
                    sim_df = base_df.copy()
                    logs = []
                    
                    # Tab2で設定した必要人数マップを取得 (GSheet: draft_requirementsから復元)
                    req_sheet_data = load_data("draft_requirements")
                    req_map_for_check = {}
                    if not req_sheet_data.empty:
                        for _, row in req_sheet_data.iterrows():
                            try:
                                d_obj = pd.to_datetime(row['日付'])
                                if d_obj.year == year and d_obj.month == month:
                                    req_map_for_check[d_obj.day - 1] = int(row['必要人数'])
                            except: pass
                    
                    if not req_map_for_check:
                        req_map_for_check = {i: 4 for i in all_days}
                    
                    # session_stateにも保存しておく（統計表示用）
                    st.session_state.daily_reqs = req_map_for_check

                    for req in st.session_state.loaded_requests:
                        nm = req.get('name')
                        if nm not in sim_df.index:
                            logs.append(f"❌ {nm}: スタッフ名が見つかりません")
                            continue
                        try:
                            dobj = pd.to_datetime(req['date'])
                            if dobj.month != month:
                                logs.append(f"⚠️ {nm} {req['date']}: 対象月外")
                                continue
                            col = f"{month}/{dobj.day}"
                            if col not in sim_df.columns:
                                logs.append(f"⚠️ {nm} {req['date']}: 列なし")
                                continue
                            
                            is_work = ("出" in str(req.get('type')))
                            target_val = 1 if is_work else 0
                            current_val = sim_df.at[nm, col]
                            
                            if current_val == target_val:
                                logs.append(f"ℹ️ {nm} {col}: 変更不要")
                                continue
                            
                            sim_df.at[nm, col] = target_val
                            
                            day_idx = dobj.day - 1
                            ok, rsn = check_daily_constraints(staffs, sim_df[col], req_map_for_check, day_idx)
                            
                            if ok: logs.append(f"✅ {nm} {col}: **{req['type']}** 適用OK")
                            else:
                                sim_df.at[nm, col] = current_val # 戻す
                                logs.append(f"🚫 {nm} {col}: **{req['type']}** 却下 ({rsn})")
                        except Exception as e:
                            logs.append(f"❌ {nm}: エラー {e}")
                    
                    st.session_state.simulated_df = sim_df
                    st.session_state.sim_logs = logs

            # --- 結果表示と確定 ---
            if st.session_state.simulated_df is not None:
                st.divider()
                st.markdown("#### シミュレーション結果")
                with st.expander("詳細ログ", expanded=True):
                    for l in st.session_state.sim_logs: st.markdown(l)
                
                c_before, c_after = st.columns(2)
                with c_before:
                    st.caption("変更前")
                    st.dataframe(base_df.replace({1:"●", 0:"-"}), use_container_width=True)
                with c_after:
                    st.caption("変更後")
                    st.dataframe(st.session_state.simulated_df.replace({1:"●", 0:"-"}), use_container_width=True)

                st.write("▼ 人数集計の比較 (変更後)")
                # 復元したマップを使用
                req_map_stats = st.session_state.get('daily_reqs', {})
                st.dataframe(calculate_daily_stats(st.session_state.simulated_df, staffs, year, month, req_map_stats))

                st.write("▼ 休日取得状況 (予測)")
                st.dataframe(calculate_detailed_stats(st.session_state.simulated_df, staffs, year, month))

                st.warning("⚠️ 確定すると、仮シフトは削除され、ログに確定データとして保存されます。")
                if st.button("この内容で確定・保存する", type="primary"):
                    final = st.session_state.simulated_df
                    
                    with st.spinner("保存処理中..."):
                        # 1. ログに追記 (クラウド)
                        new_logs = []
                        for c in final.columns:
                            dt = pd.to_datetime(f"{year}/{c}").date()
                            wd = ["月","火","水","木","金","土","日"][dt.weekday()]
                            row_dict = {"日付": dt, "曜日": wd}
                            for s in staffs:
                                nm = s['name']
                                row_dict[nm] = int(final.at[nm, c]) if nm in final.index else 0
                            new_logs.append(row_dict)
                        
                        new_log_df = pd.DataFrame(new_logs)
                        update_log_sheet(new_log_df)

                        # 2. 仮シフトシートと必要人数シートをクリア (削除)
                        clear_sheet_data("draft_schedule")
                        clear_sheet_data("draft_requirements")
                    
                    st.success("✅ 保存完了！ 仮シフトをクリアし、ログを更新しました。")
                    st.balloons()
                    st.session_state.simulated_df = None
                    st.session_state.loaded_requests = []
                    st.session_state.sim_logs = []
                    sync_all_data()
                    st.rerun()

    # --- Tab4: ログ ---
    with tab_log:
        st.subheader(f"📊 確定シフトログ & 統計 ({year}年)")
        
        st.markdown(f"##### ▼ {year}年の休日取得状況 (確定済みデータ)")
        log_summary = calculate_log_summary(staffs, year)
        st.dataframe(log_summary, use_container_width=True)
        st.divider()

        st.markdown("##### ▼ 確定シフト履歴 (生データ)")
        df_log = st.session_state.master_log
        if df_log is not None and not df_log.empty:
            st.dataframe(df_log, use_container_width=True)
        else:
            st.info("ログはありません")

if st.session_state.user_role == "admin": admin_screen()
elif st.session_state.user_role == "staff": staff_screen()
else: login_screen()
