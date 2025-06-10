import requests
import json
import pandas as pd
import os
import time
import logging
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('ted_crawler.log', encoding='utf-8')
    ]
)
logger = logging.getLogger('ted_crawler')

OUTPUT_DIR = 'data'
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'ted_api_tenders_full13.csv')
CACHE_DIR = os.path.join(OUTPUT_DIR, 'cache')
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

API_URL = 'https://tedweb.api.ted.europa.eu/private-search/api/v1/notices/search'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'Origin': 'https://ted.europa.eu',
    'Referer': 'https://ted.europa.eu/'
}


def create_payload(page_number=1, page_size=50):
    return {
        "query": "(classification-cpv IN (44000000 45000000))  SORT BY publication-number DESC",
        "page": page_number,
        "limit": page_size,
        "fields": [
            "publication-number",
            "notice-type",
            "buyer-name",
            "buyer-country",
            "buyer-legal-type",
            "contract-nature",
            "publication-date",
            "notice-title",
            "links",
            "business-opportunity",
            "cpv",
            "place-of-performance",
            "estimated-value",
            "awards",
            "lots",
            "procedure-type",
            "deadline-receipt-request",
            "change-notice-version-identifier"
        ],
        "validation": False,
        "scope": "ALL",
        "language": "EN",
        "onlyLatestVersions": True,
        "facets": {
            "business-opportunity": [],
            "cpv": [],
            "contract-nature": [],
            "place-of-performance": [],
            "procedure-type": [],
            "publication-date": [],
            "buyer-country": []
        }
    }

# 定义文件缓存路径
def get_cache_file_path(page_number):
    return Path(CACHE_DIR) / f'ted_api_raw_page{page_number}.json'

# 从缓存中加载数据
def load_from_cache(page_number):
    cache_file = get_cache_file_path(page_number)
    if cache_file.exists():
        try:
            logger.info(f"从缓存中加载第 {page_number} 页的数据")
            with open(cache_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"加载第 {page_number} 页缓存时出错: {str(e)}")
    return None

# 将数据保存到缓存中
def save_to_cache(data, page_number):
    if not data:
        return

    cache_file = get_cache_file_path(page_number)
    try:
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"已将数据保存到缓存: {cache_file}")
    except Exception as e:
        logger.error(f"保存第 {page_number} 页缓存时出错: {str(e)}")

# 从API获取招标信息
def fetch_tenders(session, page_number=1, page_size=50, use_cache=True):
    if use_cache:
        cached_data = load_from_cache(page_number)
        if cached_data:
            return cached_data

    payload = create_payload(page_number, page_size)

    try:
        logger.info(f"正在从API请求第 {page_number} 页的数据...")
        response = session.post(API_URL, json=payload)

        if response.status_code == 200:
            data = response.json()
            logger.info(f"成功获取第 {page_number} 页的数据")

            save_to_cache(data, page_number)

            return data
        else:
            logger.error(f"请求失败，状态码: {response.status_code}")
            logger.error(f"响应内容: {response.text}")
            return None
    except Exception as e:
        logger.error(f"请求异常: {str(e)}")
        return None


def extract_notice_info(notice):
    """提取公告级信息"""
    return {
        'business_opportunity': notice.get('notice-type', ''),
        'publication_date': notice.get('publication-date', ''),
        # 添加其他公告级字段
    }


def extract_buyer_info(notice):
    """提取买方信息"""
    buyer = notice.get('buyer', {})
    if not buyer:
        return {
            'buyer_name': '',
            'buyer_legal_type': ''
        }

    # 提取买方名称
    buyer_name = ''
    name_data = buyer.get('official-name', {})
    if name_data:
        for names in name_data.values():
            if names and len(names) > 0:
                buyer_name = names[0]
                break

    return {
        'buyer_name': buyer_name,
        'buyer_legal_type': buyer.get('legal-type', '')
    }


def extract_value(data):
    """提取value字段"""
    if not data:
        return "", ""

    # 处理多种数据结构
    value_data = data[0] if isinstance(data, list) else data

    # 尝试不同字段名
    amount = value_data.get('amount', value_data.get('value', ''))

    # 货币处理
    currency_data = value_data.get('currency', {})
    currency = currency_data.get('label', '') if isinstance(currency_data, dict) else str(currency_data)

    return amount, currency


def extract_award_info(award):
    """提取中标信息"""
    winner_info = {
        'winner_name': "",
        'winner_value': "",
        'winner_currency': "",
        'contract_date': award.get('contract-date', '')
    }

    # 提取中标者名称
    winner = award.get('winner', [])
    if winner:
        first_winner = winner[0] if isinstance(winner, list) else winner

        # 尝试多个可能的名称字段
        for field in ['officialName', 'official-name', 'name', 'legalName']:
            name_data = first_winner.get(field, {})
            if name_data:
                # 动态遍历所有语言
                for lang, names in name_data.items():
                    if names and len(names) > 0:
                        winner_info['winner_name'] = names[0]
                        break
                if winner_info['winner_name']:
                    break

    # 提取中标value
    value = award.get('value', [])
    if value:
        winner_info['winner_value'], winner_info['winner_currency'] = extract_value(value)

    return winner_info


def extract_lot_info(lot):
    """提取单个批次信息"""
    lot_info = {
        'lot_identifier': lot.get('lotIdentifier', lot.get('lot-identifier', '')),
        'lot_title': '',
        'purpose_cpv': "",
        'place_of_performance': "",
        'estimated_value': "",
        'estimated_currency': "",
        'estimated_duration': "",
        'winner_selection_status': "",
        'reason_no_winner': "",
    }

    # 提取标题
    title_data = lot.get('title', {})
    if title_data:
        for names in title_data.values():
            if names and len(names) > 0:
                lot_info['lot_title'] = names[0]
                break

    # 提取CPV分类
    purpose = lot.get('purpose', [])
    if purpose:
        first_purpose = purpose[0] if isinstance(purpose, list) else purpose
        cpv_list = first_purpose.get('cpv', [])
        if cpv_list:
            first_cpv = cpv_list[0] if isinstance(cpv_list, list) else cpv_list
            lot_info['purpose_cpv'] = first_cpv.get('code', '')

    # 提取履行地国家

    place_of_performance = lot.get('place-of-performance', [])
    if place_of_performance and len(place_of_performance) > 0:
        places = [place.get('label', '') for place in place_of_performance if place.get('label')]
        lot_info['place_of_performance'] = ', '.join(places)
    else:
        lot_info['place_of_performance'] = ''

    # 提取估计价值和持续时间
    estimated_value = lot.get('estimated-value', [])
    if estimated_value:
        lot_info['estimated_value'], lot_info['estimated_currency'] = extract_value(estimated_value)

    duration = lot.get('estimatedDuration', lot.get('estimated-duration', []))
    if duration:
        duration_data = duration[0] if isinstance(duration, list) else duration
        lot_info['estimated_duration'] = duration_data.get('duration', '')

    # 提取中标信息
    awards = lot.get('awards', [])
    if awards:
        award = awards[0] if isinstance(awards, list) else awards
        lot_info['winner_selection_status'] = award.get('winnerSelectionStatus',
                                                        award.get('winner-selection-status', ''))
        lot_info['reason_no_winner'] = award.get('reasonNoWinner', award.get('reason-no-winner', ''))

        # 提取中标详细信息
        winner_info = extract_award_info(award)
        lot_info.update(winner_info)

    return lot_info

def extract_tender_info(notice):
    """提取招标信息，处理多批次情况"""
    tenders = []

    # 提取公告级别信息
    common_info = {
        'notice_number': notice.get('publication-number', ''),
        'notice_type': notice.get('notice-type', {}).get('label', ''),
        'business_opportunity': notice.get('business-opportunity', ''),#商机
        'publication_date': notice.get('publication-date', ''),
        'procedure_type': notice.get('procedure-type', {}).get('label', ''),
        'contract_nature': '',  # 初始化合同性质为空字符串
        'deadline': notice.get('deadline-receipt-request', [''])[0],
        'change_version': notice.get('change-notice-version-identifier', '')
    }

    """"√"""
    # 处理 contract-nature 合同主要性质
    contract_nature = notice.get('contract-nature', {})
    if isinstance(contract_nature, dict):
        common_info['contract_nature'] = contract_nature.get('label', '')
    elif isinstance(contract_nature, list):
        # 如果是列表，你可以根据具体需求处理，这里简单取第一个元素的 label
        if contract_nature and isinstance(contract_nature[0], dict):
            common_info['contract_nature'] = contract_nature[0].get('label', '')
    """"√"""
    # 提取买方信息
    buyer_name = notice.get('buyer-name', {})
    if buyer_name:
        for names in buyer_name.values():  # 不需要语言键，只需第一个非空值
            if names and len(names) > 0:  # 确保列表非空
                common_info['buyer_name'] = names[0]
                break
        else:
            common_info['buyer_name'] = ''

    """"√"""
    buyer_country = notice.get('buyer-country', [])
    common_info['buyer_country'] = buyer_country[0].get('label', '') if buyer_country else ''

    buyer_legal_type = notice.get('buyer-legal-type', {})
    common_info['buyer_legal_type'] = buyer_legal_type.get('label', '') if buyer_legal_type else ''

    """"√"""
    # 提取公告标题
    notice_title = notice.get('notice-title', {})
    common_info['title'] = notice_title.get('eng', '')
    if not common_info['title']:
        for lang in notice_title:
            if notice_title[lang]:
                common_info['title'] = notice_title[lang]
                break
    """"√"""
    # 提取公告链接
    links = notice.get('links', {})
    common_info['link'] = ''
    if links:
        html_links = links.get('html', {})
        if html_links and 'ENG' in html_links:
            common_info['link'] = html_links['ENG']
        elif html_links:
            for lang in html_links:
                if html_links[lang]:
                    common_info['link'] = html_links[lang]
                    break

    # 提取主CPV分类
    cpv_list = notice.get('cpv', [])
    common_info['main_cpv'] = cpv_list[0].get('code', '') if cpv_list else ''

    # 提取履行地国家（公告级别）
    place_of_performance = notice.get('place-of-performance', [])
    if place_of_performance and len(place_of_performance) > 0:
        places = [place.get('label','') for place in place_of_performance if place.get('label')]
        common_info['place_of_performance'] = ', '.join(places)
    else:
        common_info['place_of_performance'] = ''



    # 提取估计价值（公告级别）
    estimated_value = notice.get('estimated-value', [])
    if estimated_value:
        common_info['estimated_value'], common_info['estimated_currency'] = extract_value(estimated_value[0])
    else:
        common_info['estimated_value'] = ''
        common_info['estimated_currency'] = ''

    # 处理批次信息
    lots = notice.get('lots', [])
    if not lots:
        # 如果没有批次，创建单个虚拟批次
        lot_info = extract_lot_info({})
        tender = {**common_info, **lot_info}
        tenders.append(tender)
    else:
        # 处理每个批次
        for lot in lots:
            lot_info = extract_lot_info(lot)
            tender = {**common_info, **lot_info}
            tenders.append(tender)

    return tenders



def save_data(data, filename, append=False):
    if not data:
        return

    # 确保所有记录都有相同的字段
    all_keys = set()
    for record in data:
        all_keys.update(record.keys())

    # 创建包含所有字段的DataFrame
    df = pd.DataFrame(data)

    # 定义CSV列顺序
    column_order = [
        'notice_number', 'notice_type', 'business_opportunity',
        'publication_date', 'procedure_type', 'contract_nature',
        'deadline', 'change_version',
        'buyer_name', 'buyer_legal_type', 'buyer_country',
        'title', 'link', 'main_cpv',
        'place_of_performance_country', 'estimated_value', 'estimated_currency',
        'lot_identifier', 'lot_title', 'purpose_cpv',
        'place_of_performance_country', 'estimated_value', 'estimated_currency',
        'estimated_duration',
        'winner_selection_status', 'reason_no_winner',
        'winner_name', 'winner_value', 'winner_currency', 'contract_date'
    ]

    # 添加缺失的列
    for col in column_order:
        if col not in df.columns:
            df[col] = None

    # 按指定顺序排列列
    df = df[column_order]

    mode = 'a' if append else 'w'
    header = not (append and os.path.exists(filename))

    df.to_csv(filename, mode=mode, header=header, index=False, encoding='utf-8-sig')
    logger.info(f"已将 {len(df)} 条记录保存到 {filename}")


def scrape_ted_api(max_pages=3, use_cache=True):
    all_tenders = []
    total_count = 0

    logger.info(f"开始TED API数据抓取，计划抓取 {max_pages} 页...")

    session = requests.Session()
    session.headers.update(HEADERS)

    for page_number in range(1, max_pages + 1):
        logger.info(f"\n正在抓取第 {page_number} 页...")

        data = fetch_tenders(session, page_number, use_cache=use_cache)

        if not data:
            logger.error(f"获取第 {page_number} 页数据失败，停止抓取")
            break

        notices = data.get('notices', [])

        if not notices:
            logger.warning(f"第 {page_number} 页没有公告数据，停止抓取")
            break

        if page_number == 1 and 'totalNoticeCount' in data:
            total_count = data.get('totalNoticeCount', 0)
            logger.info(f"共找到 {total_count} 条招标公告")

        page_tenders = []
        for notice in notices:
            tenders = extract_tender_info(notice)
            page_tenders.extend(tenders)

        logger.info(f"从第 {page_number} 页提取了 {len(page_tenders)} 条记录")

        all_tenders.extend(page_tenders)

        save_data(page_tenders, OUTPUT_FILE, append=(page_number > 1))

        if page_number < max_pages:
            delay = 2.0
            logger.info(f"等待 {delay} 秒后再抓取下一页")
            time.sleep(delay)

    logger.info(f"\n抓取完成，共抓取了 {len(all_tenders)} 条记录")
    return all_tenders


if __name__ == "__main__":
    MAX_PAGES = 10
    USE_CACHE = True

    start_time = time.time()
    tenders = scrape_ted_api(MAX_PAGES, USE_CACHE)
    end_time = time.time()

    logger.info(f"数据已保存到: {OUTPUT_FILE}")
    logger.info(f"总执行时间: {end_time - start_time:.2f} 秒")
