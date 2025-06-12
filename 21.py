# -*- coding: utf-8 -*-  # 指定文件编码为UTF-8，支持中文等特殊字符
import csv  # 用于CSV文件读写
import time  # 用于时间控制（如延时）
import requests  # 用于发送HTTP请求
import json  # 用于处理JSON数据
import re  # 用于正则表达式匹配
import os  # 用于目录操作
import logging  # 用于日志记录
from lxml import etree  # 用于HTML/XML解析和XPath查询
import pandas as pd  # 用于数据处理和CSV输出

# 配置日志系统
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("ted_scraper.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("TEDScraper")

# 配置文件和缓存目录
OUTPUT_DIR = 'data'
OUTPUT_FILE = os.path.join(OUTPUT_DIR, '21.csv')
CACHE_DIR = os.path.join(OUTPUT_DIR, 'cache')
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)


# 通过API获取单个公告的HTML内容
def raw_data(param):
    """获取子页面原始数据"""
    # 请求头设置，模拟浏览器行为
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "zh-CN,zh;q=0.9",
        "cache-control": "no-cache",
        "origin": "https://ted.europa.eu",
        "pragma": "no-cache",
        "priority": "u=1, i",
        "referer": "https://ted.europa.eu/",
        "sec-ch-ua": "\"Google Chrome\";v=\"137\", \"Chromium\";v=\"137\", \"Not/A)Brand\";v=\"24\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"
    }
    # 网站Cookie（可能需要定期更新）
    """cookies = {
        "route": "1749617454.221.31.707444|726825d00aba56cccab96f4e82375684",
        "cck1": "%7B%22cm%22%3Atrue%2C%22all1st%22%3Afalse%7D"
    }"""
    # 构建API请求URL
    url = f"https://tedweb.api.ted.europa.eu/viewer/api/v1/render/{param}/html"
    # 查询参数
    params = {
        "fields": "notice-type",
        "language": "EN",
        "termsToHighlight": ""
    }
    try:
        # 发送GET请求获取数据
        response = requests.get(url, headers=headers,
                                #cookies=cookies,
                                params=params, timeout=30)
        response.raise_for_status()  # 检查HTTP错误

        # 从JSON响应中提取HTML格式的公告内容
        raw = response.json()["noticeAsHtml"]
        return raw
    except Exception as e:
        logger.error(f"获取公告 {param} 原始数据失败: {str(e)}")
        return None


# 使用XPath解析HTML，提取结构化数据
def handle_raw(data, notice_number):
    """解析HTML数据并提取关键字段"""
    # 定义需要提取的字段名称（CSV表头）
    head = [
        # 'notice_number'
        'Official name', 'Legal type of the buyer', 'Country', 'Legal basis', 'Estimated value excluding VAT',
        'Main classification', 'Duration', 'The procurement is covered by the Government Procurement Agreement (GPA)',
        'Winner selection status', 'winners_official_name', 'Value of subcontracting',
        'Date of the conclusion of the contract', 'Publication date'
    ]

    res_dic = {
        'notice_id': notice_number,
        'ted_url': f"https://ted.europa.eu/en/notice/-/detail/{notice_number}"
    }  # 存储解析结果的字典

    tree = etree.HTML(data)  # 将HTML字符串转换为可查询的XPath树

    for i in head:
        # 查找包含字段名的span标签，并定位到其父div
        div = tree.xpath(f"//*[text() = '{i}']/ancestor::div[1]")
        if div:
            if div:
                div = div[0]
                # 获取div内所有文本
                aa = div.xpath('.//text()')
                # 分割文本获取字段值（格式：字段名: 值）
                b = ''.join(aa).split(': ')[0].strip()
                # 处理特殊字符并提取值部分
                c = ''.join(aa).split(': ')[1].strip().replace('\xa0', ' ').replace('\\n', '')

                # 特殊处理：当值不存在时，查找相邻div
                if b and not c:
                    c = div.xpath('./following-sibling::div[1]/span[1]/text()')[0]
                res_dic[i] = c
            else:
                # 字段未找到时留空
                res_dic[i] = ''

        #print(res_dic)  # 打印解析结果
        #return res_dic

    logger.info(f"解析公告 {notice_number} 完成")
    return res_dic


# 保存数据到CSV文件
def save_to_csv(data, filename):
    """保存数据到CSV"""
    if not data:
        logger.warning("没有数据可保存")
        return

    # 转换为DataFrame
    df = pd.DataFrame(data)

    # 保存到CSV（使用UTF-8-sig编码解决Excel中文乱码）
    df.to_csv(filename, index=False, encoding='utf-8-sig')
    logger.info(f"已保存 {len(df)} 条记录到 {filename}")


# 主爬取函数：获取公告列表并处理详情页
def get_target_url(targetpage=1):
    # 请求头设置
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "zh-CN,zh;q=0.9",
        "content-type": "application/json",
        "origin": "https://ted.europa.eu",
        "priority": "u=1, i",
        "referer": "https://ted.europa.eu/",
        "sec-ch-ua": "\"Google Chrome\";v=\"137\", \"Chromium\";v=\"137\", \"Not/A)Brand\";v=\"24\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"
    }
    # Cookie设置
    """cookies = {
        #"route": "1749618267.028.31.181417|726825d00aba56cccab96f4e82375684",
        "cck1": "%7B%22cm%22%3Afalse%2C%22all1st%22%3Afalse%7D"
    }"""
    # 公告搜索API
    url = "https://tedweb.api.ted.europa.eu/private-search/api/v1/notices/search"

    all_tenders = []  # 存储所有公告数据

    # 遍历指定页数
    for i in range(targetpage):
        # 构造POST请求的JSON数据
        data = {
            "query": "(classification-cpv IN (44000000 45000000))  SORT BY publication-number DESC",
            "page": i + 1,
            "limit": 50,
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
        data_json = json.dumps(data, separators=(',', ':'))  # 序列化为JSON

        try:
            # 发送POST请求
            logger.info(f"获取第 {i + 1} 页数据...")
            response = requests.post(url, headers=headers, #cookies=cookies,
                                      data=data_json, timeout=30)
            response.raise_for_status()  # 检查HTTP错误
            text = response.text

            # 使用正则提取公告编号（格式：数字-数字）
            pat = '"publication-number":.*?"(\d+-\d+)"'
            res = re.findall(pat, text)
            logger.info(f"第 {i + 1} 页找到 {len(res)} 个公告")

            # 遍历当前页所有公告编号
            for j in res:
                # 获取公告详情页HTML
                raw = raw_data(j)
                if raw:
                    # 解析并处理数据
                    tender_data = handle_raw(raw, j)
                    all_tenders.append(tender_data)
                else:
                    # 失败时记录日志
                    logger.error(f"公告 {j} 获取失败")
                time.sleep(1)  # 请求间隔防止被封
        except Exception as e:
            logger.error(f"获取第 {i + 1} 页数据失败: {str(e)}")

        # 每处理完一页就保存一次数据
        if all_tenders:
            save_to_csv(all_tenders, OUTPUT_FILE)
        time.sleep(2)  # 页间延迟

    logger.info(f"爬取完成! 共获取 {len(all_tenders)} 条记录")


# 主程序入口
if __name__ == "__main__":
    logger.info("=" * 50)
    logger.info("TED招标数据爬取程序启动")
    logger.info(f"输出文件: {OUTPUT_FILE}")
    logger.info(f"缓存目录: {CACHE_DIR}")
    logger.info("=" * 50)

    start_time = time.time()
    target_pages = 1  # 设置爬取页数
    get_target_url(target_pages)
    end_time = time.time()

    logger.info(f"总执行时间: {end_time - start_time:.2f} 秒")