from selenium import webdriver
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, StaleElementReferenceException
)
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
from tqdm import tqdm
import re
import os
import sys
import datetime


# ----------------------
# 1. 配置Edge选项和驱动（优化驱动路径检查和反爬设置）
# ----------------------
def setup_driver():
    edge_options = EdgeOptions()
    # 增强反爬配置
    edge_options.add_argument("--disable-blink-features=AutomationControlled")
    edge_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    edge_options.add_experimental_option('useAutomationExtension', False)
    edge_options.add_argument("--disable-gpu")
    edge_options.add_argument("--no-sandbox")
    edge_options.add_argument("--disable-dev-shm-usage")
    edge_options.add_argument("--window-size=1920,1080")

    # 随机化User-Agent（增加更多UA类型）
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13.5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/115.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/126.0.1823.67"
    ]
    edge_options.add_argument(f"user-agent={random.choice(user_agents)}")
    edge_options.add_argument("accept-language=en-US,en;q=0.9")

    # 动态获取驱动路径（优化路径检查）
    driver_dir = r'E:\code\project\drivers'
    driver_path = os.path.join(driver_dir, 'msedgedriver.exe')

    if not os.path.exists(driver_path):
        raise FileNotFoundError(
            f"错误: WebDriver未找到于 {driver_path}\n"
            f"请下载匹配的Edge WebDriver（Edge版本需≥107）并放置在此目录\n"
            f"下载地址: https://developer.microsoft.com/en-us/microsoft-edge/tools/webdriver/"
        )

    # 增加驱动启动重试机制（最多3次）
    for attempt in range(3):
        try:
            service = EdgeService(executable_path=driver_path)
            driver = webdriver.Edge(service=service, options=edge_options)
            driver.set_page_load_timeout(60)
            print("WebDriver 成功启动")
            return driver
        except Exception as e:
            print(f"WebDriver启动失败（尝试 {attempt + 1}/3）: {str(e)}")
            time.sleep(5)
    raise RuntimeError("无法启动WebDriver，请检查路径和版本")


# ----------------------
# 2. 创建数据存储结构
# ----------------------
def init_dataframe():
    return pd.DataFrame(columns=[
        'notice_id', 'business_opportunity', 'title', 'publication_date', 'deadline_date',
        'buyer_name', 'buyer_legal_type', 'buyer_activity',
        'purpose_main_cpv', 'place_of_performance_country',
        'estimated_value_total', 'lot_number', 'lot_title',
        'lot_main_cpv', 'lot_place_of_performance_country',
        'lot_estimated_duration', 'lot_estimated_value',
        'lot_winner_selection_status', 'lot_winner_selection_reason',
        'winner_name', 'winner_value', 'contract_conclusion_date',
        'procurement_type', 'general_info', 'financed_by_eu',
        'covered_by_gpa', 'notice_url'
    ])


# ----------------------
# 3. 解析详情页（优化字段提取逻辑）
# ----------------------
def parse_detail_page(html_content, notice_url):
    soup = BeautifulSoup(html_content, 'html.parser')
    data = {'notice_url': notice_url}

    # 提取基本信息
    try:
        header = soup.find('div', class_='ted-detail-header')
        data['notice_id'] = header.find('div', class_='ted-detail-header__id').get_text(strip=True)
        data['business_opportunity'] = header.find('div', class_='ted-detail-header__type').get_text(strip=True)
        data['title'] = header.find('h1', class_='ted-detail-header__title').get_text(strip=True)
    except AttributeError:
        pass  # 处理部分字段缺失的情况

    # 提取日期信息
    dates = soup.find_all('div', class_='ted-detail-header__date')
    data['publication_date'] = dates[0].get_text(strip=True).replace('Publication date:', '').strip() if len(
        dates) >= 1 else ""
    data['deadline_date'] = dates[1].get_text(strip=True).replace('Deadline:', '').strip() if len(dates) >= 2 else ""

    # 提取采购方信息
    try:
        buyer_info = soup.find('div', class_='ted-detail-buyer')
        data['buyer_name'] = buyer_info.find('div', class_='ted-detail-buyer__name').get_text(strip=True)
        legal_type = buyer_info.find('span', string=re.compile(r'Legal type of the buyer', re.IGNORECASE))
        data['buyer_legal_type'] = legal_type.find_next_sibling('span').get_text(strip=True) if legal_type else ""
        activity = buyer_info.find('span', string=re.compile(r'Activity of the contracting authority', re.IGNORECASE))
        data['buyer_activity'] = activity.find_next_sibling('span').get_text(strip=True) if activity else ""
    except AttributeError:
        data.update({k: "" for k in ['buyer_name', 'buyer_legal_type', 'buyer_activity']})

    # 提取合同信息
    try:
        contract_info = soup.find('div', class_='ted-detail-contract')
        data['procurement_type'] = contract_info.find('span', string=re.compile(r'Type of contract',
                                                                                re.IGNORECASE)).find_next_sibling(
            'span').get_text(strip=True) if contract_info else ""
        cpv_element = contract_info.find('span',
                                         string=re.compile(r'Main CPV code', re.IGNORECASE)) if contract_info else None
        data['purpose_main_cpv'] = cpv_element.find_next_sibling('span').get_text(strip=True) if cpv_element else ""
        country_element = contract_info.find('span',
                                             string=re.compile(r'Country', re.IGNORECASE)) if contract_info else None
        data['place_of_performance_country'] = country_element.find_next_sibling('span').get_text(
            strip=True) if country_element else ""
        value_element = contract_info.find('span', string=re.compile(r'Estimated value',
                                                                     re.IGNORECASE)) if contract_info else None
        data['estimated_value_total'] = value_element.find_next_sibling('span').get_text(
            strip=True) if value_element else ""
        gen_info = contract_info.find('span', string=re.compile(r'General information',
                                                                re.IGNORECASE)) if contract_info else None
        data['general_info'] = gen_info.find_next_sibling('span').get_text(strip=True) if gen_info else ""
    except AttributeError:
        data.update({k: "" for k in
                     ['procurement_type', 'purpose_main_cpv', 'place_of_performance_country', 'estimated_value_total',
                      'general_info']})

    # 提取标段信息（优化多标段循环逻辑）
    lots = []
    lot_sections = soup.find_all('div', class_='ted-detail-lot') + soup.find_all('div',
                                                                                 class_='ted-detail-result')  # 兼容不同页面结构
    for idx, lot_section in enumerate(lot_sections, 1):
        lot_data = {
            'lot_number': idx,
            'lot_title': lot_section.find('h2', class_='ted-detail-lot__title').get_text(
                strip=True) if lot_section.find('h2') else "",
            'financed_by_eu': "No",
            'covered_by_gpa': "No"
        }

        # 提取标段基础信息
        purpose = lot_section.find('span', string=re.compile(r'Main CPV code', re.IGNORECASE))
        lot_data['lot_main_cpv'] = purpose.find_next_sibling('span').get_text(strip=True) if purpose else ""

        country = lot_section.find('span', string=re.compile(r'Country', re.IGNORECASE))
        lot_data['lot_place_of_performance_country'] = country.find_next_sibling('span').get_text(
            strip=True) if country else ""

        duration = lot_section.find('span', string=re.compile(r'Estimated duration', re.IGNORECASE))
        lot_data['lot_estimated_duration'] = duration.find_next_sibling('span').get_text(strip=True) if duration else ""

        value = lot_section.find('span', string=re.compile(r'Estimated value', re.IGNORECASE))
        lot_data['lot_estimated_value'] = value.find_next_sibling('span').get_text(strip=True) if value else ""

        # 提取欧盟资金和GPA信息
        gen_info_text = lot_section.find('span', string=re.compile(r'General information', re.IGNORECASE))
        if gen_info_text:
            info_text = gen_info_text.find_next_sibling('span').get_text(strip=True)
            lot_data['financed_by_eu'] = "Yes" if "financed with EU Funds" in info_text else "No"
            lot_data[
                'covered_by_gpa'] = "Yes" if "covered by the Government Procurement Agreement" in info_text else "No"

        # 提取中标信息
        result_section = lot_section.find_next_sibling('div', class_='ted-detail-result') or lot_section  # 兼容结果页结构
        try:
            status = result_section.find('span', string=re.compile(r'Winner selection status', re.IGNORECASE))
            lot_data['lot_winner_selection_status'] = status.find_next_sibling('span').get_text(
                strip=True) if status else ""

            reason = result_section.find('span',
                                         string=re.compile(r'reason why a winner was not chosen', re.IGNORECASE))
            lot_data['lot_winner_selection_reason'] = reason.find_next_sibling('span').get_text(
                strip=True) if reason else ""

            winner_name = result_section.find('span', string=re.compile(r'Official name', re.IGNORECASE))
            lot_data['winner_name'] = winner_name.find_next_sibling('span').get_text(strip=True) if winner_name else ""

            winner_value = result_section.find('span', string=re.compile(r'Value of the result', re.IGNORECASE))
            lot_data['winner_value'] = winner_value.find_next_sibling('span').get_text(
                strip=True) if winner_value else ""

            contract_date = result_section.find('span', string=re.compile(r'Date of the conclusion of the contract',
                                                                          re.IGNORECASE))
            lot_data['contract_conclusion_date'] = contract_date.find_next_sibling('span').get_text(
                strip=True) if contract_date else ""
        except AttributeError:
            pass  # 处理无结果的标段

        lots.append(lot_data)

    # 处理无标段的情况
    if not lots:
        lots = [{
            'lot_number': 1,
            'lot_title': data.get('title', ''),
            'lot_main_cpv': data.get('purpose_main_cpv', ''),
            'lot_place_of_performance_country': data.get('place_of_performance_country', ''),
            'lot_estimated_value': data.get('estimated_value_total', ''),
            'financed_by_eu': "No",
            'covered_by_gpa': "No"
        }]

    return data, lots


# ----------------------
# 4. 爬取主逻辑（优化页面导航和错误处理）
# ----------------------
def scrape_ted_tenders_selenium(max_pages=5, delay=10):
    print("开始使用Selenium抓取TED招标信息...")
    driver = None
    try:
        driver = setup_driver()
        df = init_dataframe()
        initial_url = "https://ted.europa.eu/en/search/result?classificationcpv=44000000%2C45000000&search-scope=ALL&only-latest-versions=true"  # PDF提供的正确URL

        # 导航到页面并等待加载
        driver.get(initial_url)
        driver.maximize_window()
        WebDriverWait(driver, 40).until(EC.presence_of_element_located((By.CSS_SELECTOR, "form#ted-search-form")))
        time.sleep(random.uniform(3, 8))

        # 处理Cookie通知（优化定位逻辑）
        try:
            WebDriverWait(driver, 15).until(
                EC.visibility_of_element_located((By.XPATH, "//div[contains(text(), 'cookies policy')]")))
            accept_buttons = driver.find_elements(By.XPATH,
                                                  "//button[contains(@class, 'accept') or contains(@class, 'btn-accept')]")
            if accept_buttons:
                driver.execute_script("arguments[0].click();", accept_buttons[0])
            else:
                close_buttons = driver.find_elements(By.XPATH, "//button[contains(@class, 'close')]")
                if close_buttons:
                    driver.execute_script("arguments[0].click();", close_buttons[0])
        except TimeoutException:
            print("Cookie通知未出现，跳过处理")

        # 提交搜索表单（使用显式等待确保按钮可点击）
        search_button = WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button[type='submit']"))
        )
        driver.execute_script("arguments[0].click();", search_button)
        print("已提交搜索表单")
        time.sleep(random.uniform(5, 10))

        # 检查429错误的通用函数
        def handle_429_error():
            if "429 Too Many Requests" in driver.page_source:
                print("检测到429错误，等待60秒后重试...")
                time.sleep(60)
                driver.refresh()
                WebDriverWait(driver, 40).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.ted-search-result")))
                return True
            return False

        # 遍历页面
        for page in tqdm(range(1, max_pages + 1), desc="处理页面"):
            if handle_429_error():
                continue

            try:
                # 等待搜索结果加载
                WebDriverWait(driver, 40).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.ted-link--primary")))
                notice_links = [elem.get_attribute('href') for elem in
                                driver.find_elements(By.CSS_SELECTOR, "a.ted-link--primary")]
                print(f"第 {page} 页: 找到 {len(notice_links)} 个公告")

                if not notice_links:
                    print(f"第 {page} 页无公告链接，跳过")
                    continue

                # 遍历每个公告
                for notice_url in tqdm(notice_links, desc=f"处理公告"):
                    try:
                        # 打开新标签页
                        driver.execute_script(f"window.open('{notice_url}', '_blank');")
                        driver.switch_to.window(driver.window_handles[-1])
                        WebDriverWait(driver, 40).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "div.ted-detail-header")))

                        # 解析详情页
                        detail_html = driver.page_source
                        base_data, lots_data = parse_detail_page(detail_html, notice_url)

                        # 合并数据
                        for lot in lots_data:
                            row = {**base_data, **lot}
                            df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)

                        # 关闭标签页并返回主窗口
                        driver.close()
                        driver.switch_to.window(driver.window_handles[0])
                        time.sleep(random.uniform(delay - 5, delay + 5))  # 动态延迟

                    except TimeoutException:
                        print(f"公告页加载超时: {notice_url}")
                        driver.close()
                        driver.switch_to.window(driver.window_handles[0])
                    except Exception as e:
                        print(f"处理公告时出错: {str(e)}")
                        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                        driver.save_screenshot(f"error_notice_{timestamp}.png")

            except (StaleElementReferenceException, NoSuchElementException):
                print(f"第 {page} 页元素定位失败，重试...")
                driver.refresh()
                WebDriverWait(driver, 40).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.ted-link--primary")))
                continue

            # 翻页逻辑
            if page < max_pages:
                try:
                    next_button = WebDriverWait(driver, 20).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, "a.ted-pagination__link[title='Go to next page']"))
                    )
                    driver.execute_script("arguments[0].scrollIntoView(true);")
                    time.sleep(2)
                    driver.execute_script("arguments[0].click();")
                    WebDriverWait(driver, 40).until(EC.staleness_of(next_button))
                    time.sleep(random.uniform(5, 10))
                except TimeoutException:
                    print("下一页按钮未找到，结束翻页")
                    break

    except Exception as e:
        print(f"爬取过程中发生严重错误: {str(e)}")
        if driver:
            driver.save_screenshot("critical_error.png")
    finally:
        if driver:
            driver.quit()
            print("浏览器已关闭")
    return df


# ----------------------
# 5. 运行爬虫（优化参数设置）
# ----------------------
if __name__ == "__main__":
    print("Starting TED tender scraping with Selenium...")
    max_pages = 2  # 测试时建议设为1-2页
    delay = 15  # 增加延迟避免反爬（单位：秒）

    df = scrape_ted_tenders_selenium(max_pages=max_pages, delay=delay)

    if not df.empty:
        print(f"成功抓取 {len(df)} 条记录")
        save_dir = r'E:\code\project'
        os.makedirs(save_dir, exist_ok=True)
        df.to_csv(os.path.join(save_dir, 'ted_tenders.csv'), index=False)
        print(f"结果已保存至 {save_dir}/ted_tenders.csv")
    else:
        print("未抓取到数据，请检查网络或页面结构是否变化")
