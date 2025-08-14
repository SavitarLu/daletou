#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
按页面爬取浙江体彩网大乐透历史（固定 URL 模板）
起始 URL 示例:
https://www.zjlottery.com/win/SResult.asp?flag=L&Sissue=10001&Eissue=95092&Sdate=&page=1

行为:
- 从 page=1 开始逐页抓取，每页 50 条（站点设定）
- 解析期号(period)、开奖日期(draw_date)、前区5个(red1..red5)、后区2个(blue1,blue2)
- 写入 MySQL 表 dlt_draws（幂等：ON DUPLICATE KEY UPDATE）
- 会话内去重 seen_periods 防止重复写入
- 当发现 period == 10001（整数比较）时，写入并立即停止抓取
- 解析失败或不确定的行会记录到 parse_warns.log
依赖: requests, beautifulsoup4, pymysql, python-dateutil
"""
import time
import re
import sys
from datetime import datetime, timedelta
from dateutil import parser as dtparser
from dateutil.relativedelta import relativedelta
import requests
from bs4 import BeautifulSoup
import pymysql
from pymysql.constants import CLIENT
from urllib.parse import urljoin

# ------------------ 配置区 ------------------
DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "password",
    "db": "my_project",
    "charset": "utf8mb4",
    "client_flag": CLIENT.MULTI_STATEMENTS
}

# 基础 URL 模板（只替换 page 参数）
URL_TEMPLATE = "https://www.zjlottery.com/win/SResult.asp?flag=L&Sissue=10001&Eissue=95092&Sdate=&page={page}"

# 每次请求之间的间隔（秒）
REQUEST_INTERVAL = 0.8

# 最大页数保护（防止无限循环）
MAX_PAGES = 2000

# 解析失败日志
PARSE_WARN_LOG = "parse_warns.log"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

# 停止条件的期号（整数）
STOP_PERIOD_INT = 10001

# ------------------ 数据库操作 ------------------
def connect_db():
    try:
        conn = pymysql.connect(
            host=DB_CONFIG["host"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            db=DB_CONFIG["db"],
            port=DB_CONFIG.get("port", 3306),
            charset=DB_CONFIG.get("charset", "utf8mb4"),
            client_flag=DB_CONFIG.get("client_flag")
        )
        return conn
    except Exception as e:
        print(f"[FATAL] 无法连接数据库: {e}")
        sys.exit(1)

def create_table_if_not_exists(conn):
    create_sql = """
    CREATE TABLE IF NOT EXISTS dlt_draws (
      period VARCHAR(20) NOT NULL,
      draw_date DATE NOT NULL,
      red1 TINYINT UNSIGNED NOT NULL,
      red2 TINYINT UNSIGNED NOT NULL,
      red3 TINYINT UNSIGNED NOT NULL,
      red4 TINYINT UNSIGNED NOT NULL,
      red5 TINYINT UNSIGNED NOT NULL,
      blue1 TINYINT UNSIGNED NOT NULL,
      blue2 TINYINT UNSIGNED NOT NULL,
      sales BIGINT DEFAULT NULL,
      jackpot BIGINT DEFAULT NULL,
      first_prize_count INT DEFAULT NULL,
      first_prize_amount BIGINT DEFAULT NULL,
      source VARCHAR(255) DEFAULT NULL,
      fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (period),
      INDEX (draw_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    with conn.cursor() as cur:
        cur.execute(create_sql)
    conn.commit()

def insert_or_update(conn, record):
    sql = """
    INSERT INTO dlt_draws
      (period, draw_date, red1, red2, red3, red4, red5, blue1, blue2, sales, jackpot, first_prize_count, first_prize_amount, source)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      draw_date=VALUES(draw_date),
      red1=VALUES(red1), red2=VALUES(red2), red3=VALUES(red3), red4=VALUES(red4), red5=VALUES(red5),
      blue1=VALUES(blue1), blue2=VALUES(blue2),
      sales=VALUES(sales), jackpot=VALUES(jackpot),
      first_prize_count=VALUES(first_prize_count), first_prize_amount=VALUES(first_prize_amount),
      source=VALUES(source),
      fetched_at=CURRENT_TIMESTAMP();
    """
    vals = (
        record["period"], record["draw_date"],
        record["reds"][0], record["reds"][1], record["reds"][2], record["reds"][3], record["reds"][4],
        record["blues"][0], record["blues"][1],
        record.get("sales"), record.get("jackpot"),
        record.get("first_prize_count"), record.get("first_prize_amount"),
        record.get("source")
    )
    with conn.cursor() as cur:
        cur.execute(sql, vals)
    conn.commit()

def log_parse_warn(msg):
    try:
        with open(PARSE_WARN_LOG, "a", encoding="utf-8") as f:
            f.write(f"{datetime.now().isoformat()} {msg}\n")
    except Exception:
        pass

# ------------------ 解析工具（针对浙江站常见样式） ------------------
def extract_numbers_from_tags(node, tag_names=("i", "em", "span")):
    for tag_name in tag_names:
        elems = node.find_all(tag_name) if hasattr(node, "find_all") else []
        if not elems:
            continue
        tmp = [e.get_text(strip=True) for e in elems if re.match(r'^\d{1,2}$', e.get_text(strip=True))]
        if tmp:
            return tmp
    return []

def split_14digit_string(s):
    s = re.sub(r'\D', '', s)
    if len(s) == 14:
        return [s[i:i+2] for i in range(0, 14, 2)]
    return None

def split_two_digit_groups(s):
    parts = re.findall(r'\d{1,2}', s)
    if len(parts) >= 7:
        return parts
    parts14 = split_14digit_string(s)
    if parts14:
        return parts14
    return parts

def parse_row_zj(tr, source_url):
    """
    根据 tr 解析期号/日期/号码，返回 record dict 或 None
    """
    tds = tr.find_all("td")
    if not tds:
        return None

    texts = [td.get_text(" ", strip=True) for td in tds]

    # 尝试找期号：优先匹配纯数字 4~6 位
    period = None
    period_idx = None
    for idx, txt in enumerate(texts):
        if re.fullmatch(r'\d{4,6}', txt):
            period = txt
            period_idx = idx
            break
    if period is None and len(texts) >= 2:
        # 有时期号在第2列/第3列，宽松取第二列为候补
        maybe = texts[1].strip()
        if re.search(r'\d', maybe):
            period = maybe
            period_idx = 1

    # 日期优先在期号左侧或首列
    draw_date = None
    if period_idx is not None and period_idx >= 1:
        try:
            draw_date = dtparser.parse(texts[period_idx - 1]).date()
        except Exception:
            draw_date = None
    if draw_date is None:
        try:
            draw_date = dtparser.parse(texts[0]).date()
        except Exception:
            draw_date = None

    # 号码提取：优先标签内的单个号码，再找数字最多的 td，再整行拼接
    nums = []
    if period_idx is not None:
        for j in (period_idx+1, period_idx+2, period_idx-1, period_idx+3):
            if 0 <= j < len(tds):
                nums = extract_numbers_from_tags(tds[j])
                if nums and len(nums) >= 7:
                    break

    if not nums:
        # 找数字最多的 td
        best_txt = None
        max_digits = 0
        for idx, txt in enumerate(texts):
            cnt = len(re.findall(r'\d', txt))
            if cnt > max_digits:
                max_digits = cnt
                best_txt = txt
        if best_txt:
            sp14 = split_14digit_string(best_txt)
            if sp14:
                nums = sp14
            else:
                nums = split_two_digit_groups(best_txt)

    if not nums or len(nums) < 7:
        alltxt = " ".join(texts)
        sp14 = split_14digit_string(alltxt)
        if sp14:
            nums = sp14
        else:
            nums = re.findall(r'\d{1,2}', alltxt)

    if not nums or len(nums) < 7:
        log_parse_warn(f"[解析失败] period={period} date='{draw_date}' nums_found={nums} source={source_url}")
        return None

    nums_int = [int(x) for x in nums if 0 <= int(x) <= 99]

    # 在 nums_int 中搜索连续 7 个符合范围（前5:1-35 后2:1-12）
    reds = blues = None
    for i in range(0, max(1, len(nums_int) - 6)):
        cand = nums_int[i:i+7]
        if len(cand) < 7:
            continue
        if all(1 <= v <= 35 for v in cand[:5]) and all(1 <= v <= 12 for v in cand[5:7]):
            reds = cand[:5]
            blues = cand[5:7]
            break

    # 宽松策略：前7直接取
    if reds is None or blues is None:
        cand = nums_int[:7]
        if len(cand) >= 7 and all(1 <= v <= 35 for v in cand[:5]) and all(1 <= v <= 12 for v in cand[5:7]):
            reds = cand[:5]
            blues = cand[5:7]

    if reds is None or blues is None:
        log_parse_warn(f"[号码校验失败] period={period} date='{draw_date}' nums_int={nums_int} source={source_url}")
        return None

    return {
        "period": str(period).strip(),
        "draw_date": draw_date,
        "reds": reds,
        "blues": blues,
        "sales": None,
        "jackpot": None,
        "first_prize_count": None,
        "first_prize_amount": None,
        "source": source_url
    }

# ------------------ 翻页抓取主逻辑 ------------------
def fetch_pages_until_stop(conn, stop_period_int=STOP_PERIOD_INT):
    """
    从 page=1 循环抓取 URL_TEMPLATE 的各页，直到遇到 stop_period_int
    返回写入记录数
    """
    session = requests.Session()
    session.headers.update(HEADERS)
    page = 1
    total_written = 0
    seen_periods = set()
    while page <= MAX_PAGES:
        url = URL_TEMPLATE.format(page=page)
        try:
            r = session.get(url, timeout=20)
            r.encoding = r.apparent_encoding
            if r.status_code != 200:
                print(f"[WARN] {url} 返回 {r.status_code}，停止。")
                break
            soup = BeautifulSoup(r.text, "html.parser")
            # 定位表格：查找包含“开奖号码”或“开奖日期”的 table
            table = None
            for t in soup.find_all("table"):
                if "开奖号码" in t.get_text() or "开奖日期" in t.get_text():
                    table = t
                    break
            if table is None:
                tbody = soup.find("tbody", id="tdata")
                rows = tbody.find_all("tr") if tbody else []
            else:
                tbody = table.find("tbody") or table
                rows = tbody.find_all("tr")

            if not rows:
                print(f"[INFO] page={page} 未找到数据行，停止。")
                break

            any_new = False
            for tr in rows:
                rec = parse_row_zj(tr, url)
                if not rec:
                    continue
                # 必要字段校验
                if rec["draw_date"] is None or not rec["period"]:
                    continue
                # 去重
                if rec["period"] in seen_periods:
                    continue

                # 写库
                try:
                    insert_or_update(conn, rec)
                    total_written += 1
                    seen_periods.add(rec["period"])
                    any_new = True
                    print(f"[OK ] 写入 {rec['period']} {rec['draw_date']} {rec['reds']}+{rec['blues']}")
                except Exception as e:
                    print(f"[ERROR] 插入 DB 失败 {rec['period']}: {e}")

                # 停止条件：如果该期号等于 stop_period_int（整数比较）
                try:
                    if int(re.sub(r'\D', '', rec["period"])) == stop_period_int:
                        print(f"[INFO] 发现 stop period {stop_period_int}，结束抓取（已写入本期）。")
                        return total_written
                except Exception:
                    # 如果解析为整数失败则忽略
                    pass

            # 如果本页没有任何新记录，考虑可能到尾或页重复，继续下一页但记录情况
            page += 1
            time.sleep(REQUEST_INTERVAL)
        except Exception as e:
            print(f"[ERROR] 请求 {url} 失败: {e}")
            # 继续尝试下一页或根据需要中断
            page += 1
            time.sleep(REQUEST_INTERVAL)
            continue

    print("[INFO] 达到最大页数或队列末尾，抓取结束。")
    return total_written

# ------------------ 主程序 ------------------
def main():
    local_today = datetime.now().date()
    yesterday = local_today - timedelta(days=1)
    cutoff_date = yesterday - relativedelta(years=2)  # 未直接用，但保留作为信息
    print(f"[INFO] 开始抓取 (页面翻页)，目标 stop_period={STOP_PERIOD_INT}。")

    conn = connect_db()
    create_table_if_not_exists(conn)

    try:
        n = fetch_pages_until_stop(conn, STOP_PERIOD_INT)
        print(f"[INFO] 总共写入 {n} 条记录。")
    except Exception as e:
        print(f"[FATAL] 抓取异常: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
