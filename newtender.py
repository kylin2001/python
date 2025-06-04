import requests
import json
import pandas as pd
import os
import time
import logging
from pathlib import Path
from tqdm import tqdm

# 配置日志系统
logging.basicConfig(
    level=logging.DEBUG,  # 更改为DEBUG级别以获取更多信息
    format='%(asctime)s - %(levelname)s - %(funcName)s - %(message)s',
    handlers=[
        logging.FileHandler("ted_scraper_debug.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("TEDScraper")

# 配置文件和缓存目录
OUTPUT_DIR = 'data'
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'ted_tenders_with_lots.csv')
CACHE_DIR = os.path.join(OUTPUT_DIR, 'cache')
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

# API 配置
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
            "BT-5141-Procedure",
            "BT-5141-Part",
            "BT-5141-Lot",
            "BT-5071-Procedure",
            "BT-5071-Part",
            "BT-5071-Lot",
            "BT-727-Procedure",
            "BT-727-Part",
            "BT-727-Lot",
            "place-of-performance",
            "procedure-type",
            "contract-nature",
            "buyer-name",
            "buyer-country",
            "publication-date",
            "deadline-receipt-request",
            "notice-title",
            "official-language",
            "notice-type",
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


def get_cache_file_path(page_number):
    """获取缓存文件路径"""
    return Path(CACHE_DIR) / f'ted_api_page_{page_number}.json'


def load_from_cache(page_number):
    """从缓存加载数据"""
    cache_file = get_cache_file_path(page_number)
    if cache_file.exists():
        try:
            logger.info(f"从缓存加载第 {page_number} 页数据")
            with open(cache_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"加载缓存失败: {str(e)}")
    return None


def save_to_cache(data, page_number):
    """保存数据到缓存"""
    if not data:
        return

    cache_file = get_cache_file_path(page_number)
    try:
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"数据已缓存: {cache_file}")
    except Exception as e:
        logger.error(f"缓存保存失败: {str(e)}")


def fetch_tenders(session, page_number=1, use_cache=True):
    """从API获取招标数据"""
    if use_cache:
        cached_data = load_from_cache(page_number)
        if cached_data:
            return cached_data

    payload = create_payload(page_number)

    try:
        logger.info(f"请求第 {page_number} 页数据...")
        response = session.post(API_URL, json=payload, headers=HEADERS)

        # 详细记录错误信息
        if response.status_code != 200:
            error_msg = f"请求失败，状态码: {response.status_code}"
            try:
                error_details = response.json().get('message', '无错误详情')
                error_msg += f", 错误信息: {error_details}"
            except:
                error_msg += f", 响应内容: {response.text[:500]}"
            logger.error(error_msg)
            return None

        data = response.json()
        logger.info(f"成功获取第 {page_number} 页数据，包含 {len(data.get('notices', []))} 条记录")
        save_to_cache(data, page_number)
        return data
    except Exception as e:
        logger.error(f"请求异常: {str(e)}")
        return None


def extract_lot_info(lot_data):
    """提取标段信息"""
    lot_info = {
        'lot_id': lot_data.get('id', ''),
        'lot_number': lot_data.get('number', ''),
        'lot_title': '',
        'lot_purpose_cpv': '',
        'lot_place_country': '',
        'lot_estimated_duration': '',
        'lot_value': '',
        'winner_status': '',
        'winner_name': '',
        'contract_value': '',
        'contract_date': ''
    }

    # 提取标段标题
    title = lot_data.get('title', {})
    if title:
        lot_info['lot_title'] = title.get('eng', '')
        if not lot_info['lot_title']:
            for text in title.values():
                if text:
                    lot_info['lot_title'] = text
                    break

    # 提取CPV分类
    cpv_list = lot_data.get('cpv', [])
    if cpv_list:
        cpv_codes = [cpv.get('code', '') for cpv in cpv_list]
        lot_info['lot_purpose_cpv'] = ', '.join(cpv_codes)

    # 提取执行地点
    places = lot_data.get('place', [])
    if places:
        countries = []
        for place in places:
            country = place.get('country', {})
            if country:
                countries.append(country.get('label', ''))
        lot_info['lot_place_country'] = ', '.join(countries)

    # 提取持续时间
    duration = lot_data.get('duration', {})
    if duration:
        lot_info['lot_estimated_duration'] = duration.get('description', '')

    # 提取标段价值
    value = lot_data.get('value', {})
    if value:
        lot_info['lot_value'] = value.get('amount', '')

    # 提取中标信息
    contractors = lot_data.get('contractors', [])
    if contractors:
        winner = contractors[0]
        lot_info['winner_name'] = winner.get('name', '')
        lot_info['winner_status'] = 'Awarded' if winner.get('awarded') else 'Pending'

        # 合同价值
        award_value = winner.get('value', {})
        if award_value:
            lot_info['contract_value'] = award_value.get('amount', '')

        # 合同日期
        award_date = winner.get('awardDate', '')
        if award_date:
            lot_info['contract_date'] = award_date

    return lot_info


def extract_tender_info(notice):
    """提取招标公告基本信息"""
    tender = {
        'notice_id': notice.get('publication-number', ''),
        'business_opportunity': notice.get('notice-type', {}).get('label', ''),
        'publication_date': notice.get('publication-date', ''),
        'buyer_official_name': '',
        'buyer_country': '',
        'purpose_cpv': '',
        'place_country': '',
        'total_value': ''
    }

    # 提取采购方名称
    buyer_name = notice.get('buyer-name', {})
    if buyer_name:
        for names in buyer_name.values():
            if names and names:
                tender['buyer_official_name'] = names[0]
                break

    # 提取采购方国家
    buyer_country = notice.get('buyer-country', [])
    if buyer_country:
        tender['buyer_country'] = buyer_country[0].get('label', '') if buyer_country else ''

    # 提取CPV分类
    cpv_list = notice.get('cpv', [])
    if cpv_list:
        cpv_codes = [cpv.get('code', '') for cpv in cpv_list]
        tender['purpose_cpv'] = ', '.join(cpv_codes)

    # 提取执行地点
    places = notice.get('place-of-performance', [])
    if places:
        countries = []
        for place in places:
            country = place.get('country', {})
            if country:
                countries.append(country.get('label', ''))
        tender['place_country'] = ', '.join(countries)

    # 提取总价值
    value = notice.get('estimated-value', {})
    if value:
        tender['total_value'] = value.get('amount', '')

    return tender


def process_notice(notice):
    """处理单条公告，生成标段数据行"""
    base_info = extract_tender_info(notice)
    all_lots = []

    # 提取标段信息
    lots = notice.get('lots', [])

    if lots:
        for lot in lots:
            lot_info = extract_lot_info(lot)
            combined = {**base_info, **lot_info}
            all_lots.append(combined)
    else:
        # 没有标段时，只添加基础信息
        base_info.update({
            'lot_id': '',
            'lot_number': '',
            'lot_title': '',
            'lot_purpose_cpv': '',
            'lot_place_country': '',
            'lot_estimated_duration': '',
            'lot_value': '',
            'winner_status': '',
            'winner_name': '',
            'contract_value': '',
            'contract_date': ''
        })
        all_lots.append(base_info)

    return all_lots


def save_data(data, filename):
    """保存数据到CSV文件"""
    if not data:
        logger.warning("没有数据可保存")
        return None

    # 确保目录存在
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    # 转换为DataFrame
    df = pd.DataFrame(data)

    # 保存到CSV
    df.to_csv(filename, index=False, encoding='utf-8-sig')
    logger.info(f"已保存 {len(df)} 条记录到 {filename}")
    return df


def scrape_ted_api(max_pages=5, use_cache=True, delay=2):
    """主爬取函数"""
    all_tenders = []
    session = requests.Session()
    session.headers.update(HEADERS)

    logger.info(f"开始爬取TED数据，计划获取 {max_pages} 页...")

    for page in tqdm(range(1, max_pages + 1), desc="处理页面"):
        # 获取API数据
        data = fetch_tenders(session, page, use_cache=use_cache)

        if not data:
            logger.error(f"第 {page} 页数据获取失败，跳过")
            continue

        notices = data.get('notices', [])

        if not notices:
            logger.warning(f"第 {page} 页没有公告数据")
            continue

        # 处理本页所有公告
        page_tenders = []
        for notice in notices:
            try:
                tender_rows = process_notice(notice)
                page_tenders.extend(tender_rows)
            except Exception as e:
                logger.error(f"处理公告失败: {str(e)}")

        logger.info(f"第 {page} 页提取了 {len(page_tenders)} 条记录")
        all_tenders.extend(page_tenders)

        # 页面间延迟
        if page < max_pages:
            time.sleep(delay)

    # 保存最终结果
    if all_tenders:
        df = save_data(all_tenders, OUTPUT_FILE)
        logger.info(f"爬取完成! 共获取 {len(all_tenders)} 条记录")
        return df
    else:
        logger.warning("没有获取到任何数据")
        return pd.DataFrame()


if __name__ == "__main__":
    # 配置参数
    MAX_PAGES = 3  # 爬取页数
    USE_CACHE = False  # 首次运行禁用缓存
    DELAY = 1.5  # 页面间延迟（秒）

    logger.info("=" * 50)
    logger.info("TED招标数据爬取程序启动")
    logger.info(f"输出文件: {OUTPUT_FILE}")
    logger.info(f"缓存目录: {CACHE_DIR}")
    logger.info("=" * 50)

    start_time = time.time()
    result_df = scrape_ted_api(MAX_PAGES, USE_CACHE, DELAY)
    end_time = time.time()

    logger.info(f"总执行时间: {end_time - start_time:.2f} 秒")

    # 打印结果摘要
    if result_df is not None and not result_df.empty:
        logger.info("\n数据摘要:")
        logger.info(f"总记录数: {len(result_df)}")
        logger.info(f"公告数量: {result_df['notice_id'].nunique()}")
        logger.info(f"包含标段的公告: {result_df[result_df['lot_id'] != '']['notice_id'].nunique()}")
        logger.info(f"中标标段: {result_df[result_df['winner_status'] == 'Awarded'].shape[0]}")
    else:
        logger.warning("没有获取到数据")
