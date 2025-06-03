from selenium import webdriver
from selenium.common.exceptions import TimeoutException, NoSuchElementException
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
import json
import datetime


# 1. 配置 Edge 选项和驱动
def setup_driver():
    edge_options = EdgeOptions()
    # 暂时禁用无头模式以便调试
    edge_options.add_argument("--disable-gpu")
    edge_options.add_argument("--no-sandbox")
    edge_options.add_argument("--disable-dev-shm-usage")
    edge_options.add_argument("--window-size=1920,1080")
    edge_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")
    edge_options.add_argument("accept-language=en-US,en;q=0.9")

    # 手动指定 Edge WebDriver 路径
    driver_path = r"E:\code\project\drivers\msedgedriver.exe"  # 确保路径正确

    # 创建 Edge WebDriver 实例（带重试机制）
    for attempt in range(3):
        try:
            driver = webdriver.Edge(
                service=EdgeService(executable_path=driver_path),
                options=edge_options
            )
            driver.set_page_load_timeout(45)
            print("WebDriver 成功启动")
            return driver
        except Exception as e:
            print(f"WebDriver 启动失败 (尝试 {attempt + 1}/3): {str(e)}")
            time.sleep(2)

    raise RuntimeError("无法启动WebDriver，请检查路径和版本")


# 2. 创建数据存储结构
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


# 3. 解析函数，提取所有字段
def parse_detail_page(html_content, notice_url):
    soup = BeautifulSoup(html_content, 'html.parser')
    data = {}

    # 基本公告信息
    data['notice_url'] = notice_url

    # 公告ID和业务机会类型
    header = soup.find('div', class_='ted-detail-header')
    if header:
        data['notice_id'] = header.find('div', class_='ted-detail-header__id').get_text(strip=True) if header.find(
            'div', class_='ted-detail-header__id') else ""

        # 业务机会类型 (Result, Competition, Contract Modification等)
        opp_elem = header.find('div', class_='ted-detail-header__type')
        data['business_opportunity'] = opp_elem.get_text(strip=True) if opp_elem else ""

        # 标题
        title = header.find('h1', class_='ted-detail-header__title')
        data['title'] = title.get_text(strip=True) if title else ""

    # 日期信息
    dates = soup.find_all('div', class_='ted-detail-header__date')
    data['publication_date'] = dates[0].get_text(strip=True).replace('Publication date:', '').strip() if len(
        dates) > 0 else ""
    data['deadline_date'] = dates[1].get_text(strip=True).replace('Deadline:', '').strip() if len(dates) > 1 else ""

    # 采购方信息 (1.1 Buyer)
    buyer_info = soup.find('div', class_='ted-detail-buyer')
    if buyer_info:
        data['buyer_name'] = buyer_info.find('div', class_='ted-detail-buyer__name').get_text(
            strip=True) if buyer_info.find('div', class_='ted-detail-buyer__name') else ""

        # 采购方法律类型
        legal_type = buyer_info.find('span', string=re.compile(r'Legal type of the buyer', re.IGNORECASE))
        data['buyer_legal_type'] = legal_type.find_next_sibling('span').get_text(strip=True) if legal_type else ""

        # 采购方活动
        activity = buyer_info.find('span', string=re.compile(r'Activity of the contracting authority', re.IGNORECASE))
        data['buyer_activity'] = activity.find_next_sibling('span').get_text(strip=True) if activity else ""
    else:
        data['buyer_name'] = ""
        data['buyer_legal_type'] = ""
        data['buyer_activity'] = ""

    # 合同信息 (2.1.1 Purpose, 2.1.2 Place of Performance, 2.1.3 Value)
    contract_info = soup.find('div', class_='ted-detail-contract')
    if contract_info:
        # 采购类型
        data['procurement_type'] = contract_info.find('span', string=re.compile(r'Type of contract', re.IGNORECASE))
        if data['procurement_type']:
            data['procurement_type'] = data['procurement_type'].find_next_sibling('span').get_text(strip=True)

        # 主CPV代码 (2.1.1)
        cpv_element = contract_info.find('span', string=re.compile(r'Main CPV code', re.IGNORECASE))
        data['purpose_main_cpv'] = cpv_element.find_next_sibling('span').get_text(strip=True) if cpv_element else ""

        # 执行地点国家 (2.1.2)
        country_element = contract_info.find('span', string=re.compile(r'Country', re.IGNORECASE))
        data['place_of_performance_country'] = country_element.find_next_sibling('span').get_text(
            strip=True) if country_element else ""

        # 估计总值 (2.1.3)
        value_element = contract_info.find('span', string=re.compile(r'Estimated value', re.IGNORECASE))
        data['estimated_value_total'] = value_element.find_next_sibling('span').get_text(
            strip=True) if value_element else ""

        # 一般信息 (2.1.4)
        gen_info = contract_info.find('span', string=re.compile(r'General information', re.IGNORECASE))
        data['general_info'] = gen_info.find_next_sibling('span').get_text(strip=True) if gen_info else ""
    else:
        data['procurement_type'] = ""
        data['purpose_main_cpv'] = ""
        data['place_of_performance_country'] = ""
        data['estimated_value_total'] = ""
        data['general_info'] = ""

    # 提取标段信息 (5.x.x)
    lots = []
    lot_sections = soup.find_all('div', class_='ted-detail-lot')

    for i, lot_section in enumerate(lot_sections):
        lot_data = {
            'lot_number': i + 1,
            'lot_title': lot_section.find('h2').get_text(strip=True) if lot_section.find('h2') else "",
            'lot_main_cpv': "",
            'lot_place_of_performance_country': "",
            'lot_estimated_duration': "",
            'lot_estimated_value': "",
            'lot_winner_selection_status': "",
            'lot_winner_selection_reason': "",
            'winner_name': "",
            'winner_value': "",
            'contract_conclusion_date': "",
            'financed_by_eu': "No",
            'covered_by_gpa': "No"
        }

        # 标段目的 (5.1.1)
        purpose = lot_section.find('span', string=re.compile(r'Main CPV code', re.IGNORECASE))
        if purpose:
            lot_data['lot_main_cpv'] = purpose.find_next_sibling('span').get_text(strip=True)

        # 标段执行地点国家 (5.1.2)
        lot_country = lot_section.find('span', string=re.compile(r'Country', re.IGNORECASE))
        if lot_country:
            lot_data['lot_place_of_performance_country'] = lot_country.find_next_sibling('span').get_text(strip=True)

        # 标段估计持续时间 (5.1.3)
        duration = lot_section.find('span', string=re.compile(r'Estimated duration', re.IGNORECASE))
        if duration:
            lot_data['lot_estimated_duration'] = duration.find_next_sibling('span').get_text(strip=True)

        # 标段估计价值 (5.1.5)
        lot_value = lot_section.find('span', string=re.compile(r'Estimated value', re.IGNORECASE))
        if lot_value:
            lot_data['lot_estimated_value'] = lot_value.find_next_sibling('span').get_text(strip=True)

        # 标段一般信息 (5.1.6) - 提取欧盟资金和GPA信息
        gen_info = lot_section.find('span', string=re.compile(r'General information', re.IGNORECASE))
        if gen_info:
            info_text = gen_info.find_next_sibling('span').get_text(strip=True) if gen_info else ""
            if "financed with EU Funds" in info_text:
                lot_data['financed_by_eu'] = "Yes"
            if "covered by the Government Procurement Agreement" in info_text:
                lot_data['covered_by_gpa'] = "Yes"

        # 标段结果信息 (6.1)
        result_section = lot_section.find_next_sibling('div', class_='ted-detail-result')
        if result_section:
            # 中标者选择状态 (6.1.1)
            status = result_section.find('span', string=re.compile(r'Winner selection status', re.IGNORECASE))
            if status:
                lot_data['lot_winner_selection_status'] = status.find_next_sibling('span').get_text(strip=True)

            # 未选择中标者的原因 (6.1.1)
            reason = result_section.find('span',
                                         string=re.compile(r'reason why a winner was not chosen', re.IGNORECASE))
            if reason:
                lot_data['lot_winner_selection_reason'] = reason.find_next_sibling('span').get_text(strip=True)

            # 中标者信息 (6.1.2)
            winner_name = result_section.find('span', string=re.compile(r'Official name', re.IGNORECASE))
            if winner_name:
                lot_data['winner_name'] = winner_name.find_next_sibling('span').get_text(strip=True)

            # 中标价值 (6.1.2)
            winner_value = result_section.find('span', string=re.compile(r'Value of the result', re.IGNORECASE))
            if winner_value:
                lot_data['winner_value'] = winner_value.find_next_sibling('span').get_text(strip=True)

            # 合同签订日期 (6.1.2)
            contract_date = result_section.find('span', string=re.compile(r'Date of the conclusion of the contract',
                                                                          re.IGNORECASE))
            if contract_date:
                lot_data['contract_conclusion_date'] = contract_date.find_next_sibling('span').get_text(strip=True)

        lots.append(lot_data)

    # 如果没有标段，创建默认标段
    if not lots:
        lots = [{
            'lot_number': 1,
            'lot_title': data.get('title', ''),
            'lot_main_cpv': data.get('purpose_main_cpv', ''),
            'lot_place_of_performance_country': data.get('place_of_performance_country', ''),
            'lot_estimated_duration': "",
            'lot_estimated_value': data.get('estimated_value_total', ''),
            'lot_winner_selection_status': "",
            'lot_winner_selection_reason': "",
            'winner_name': "",
            'winner_value': "",
            'contract_conclusion_date': "",
            'financed_by_eu': "No",
            'covered_by_gpa': "No"
        }]

    return data, lots


# 4. 又爬取函数，解决爬不到数据
def scrape_ted_tenders_selenium(max_pages=5, delay=3):
    print("开始使用Selenium抓取TED招标信息...")
    driver = None
    try:
        driver = setup_driver()
        df = init_dataframe()

        # 使用PDF中提供的搜索URL
        initial_url = "https://ted.europa.eu/en/search/result?classification-cpv=44000000%2C45000000&search-scope=ALL&only-latest-versions=true"

        # 导航到初始页面
        print(f"正在访问初始页面: {initial_url}")
        driver.get(initial_url)

        # 自动关闭cookie弹窗
        try:
            cookie_btn = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button#cookie-consent-agree")))
            cookie_btn.click()
            print("已关闭cookie通知")
            time.sleep(1)
        except Exception:
            print("未找到cookie通知或无法关闭")
            pass

        # 等待搜索表单加载完成
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "form#ted-search-form"))
        )

        # 提交搜索表单
        search_button = driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
        search_button.click()

        # 等待结果加载
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.ted-search-result"))
        )

        for page in tqdm(range(1, max_pages + 1), desc="处理页面"):
            try:
                # 等待结果加载
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a.ted-link--primary"))
                )

                # 获取当前页面所有公告链接
                notice_elements = driver.find_elements(By.CSS_SELECTOR, "a.ted-link--primary")
                notice_links = [elem.get_attribute('href') for elem in notice_elements]

                print(f"第 {page} 页: 找到 {len(notice_links)} 个公告")

                # 处理每个公告
                for notice_url in tqdm(notice_links, desc=f"第 {page} 页公告"):
                    try:
                        # 在新标签页打开公告
                        driver.execute_script(f"window.open('{notice_url}', '_blank');")

                        # 切换到新标签页
                        driver.switch_to.window(driver.window_handles[1])

                        # 等待详情页加载
                        try:
                            WebDriverWait(driver, 20).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, "div.ted-detail-header"))
                            )
                        except TimeoutException:
                            print(f"超时: 无法加载公告页面 {notice_url}")
                            # 保存页面截图以便调试
                            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                            driver.save_screenshot(f"error_detail_{timestamp}.png")
                            driver.close()
                            driver.switch_to.window(driver.window_handles[0])
                            continue

                        # 获取详情页源码
                        detail_html = driver.page_source

                        # 解析详情页
                        try:
                            base_data, lots_data = parse_detail_page(detail_html, notice_url)
                        except Exception as e:
                            print(f"解析公告页面时出错: {str(e)}")
                            base_data, lots_data = {}, []

                        # 为每个标段创建单独行
                        for lot in lots_data:
                            if base_data:  # 确保有基础数据
                                # 合并基础数据和标段数据
                                row = {**base_data, **lot}
                                # 添加到DataFrame
                                df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)

                        # 关闭当前标签页并切换回主标签页
                        driver.close()
                        driver.switch_to.window(driver.window_handles[0])

                        # 随机延迟
                        sleep_time = delay * random.uniform(0.8, 1.2)
                        time.sleep(sleep_time)

                    except Exception as e:
                        print(f"处理公告时出错: {str(e)}")
                        # 保存页面截图以便调试
                        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                        driver.save_screenshot(f"error_notice_{timestamp}.png")

                        # 确保回到主标签页
                        if driver:
                            if len(driver.window_handles) > 1:
                                driver.close()
                            driver.switch_to.window(driver.window_handles[0])
                        time.sleep(2)

                # 如果不是最后一页，点击下一页按钮
                if page < max_pages:
                    try:
                        # 使用显式等待确保按钮可点击
                        next_button = WebDriverWait(driver, 15).until(
                            EC.element_to_be_clickable(
                                (By.CSS_SELECTOR, "a.ted-pagination__link[title='Go to next page']"))
                        )

                        # 滚动到元素位置
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_button)
                        time.sleep(0.5)

                        # 检查元素是否可见和可点击
                        if next_button.is_displayed() and next_button.is_enabled():
                            print("导航到下一页...")
                            # 使用JavaScript点击避免交互问题
                            driver.execute_script("arguments[0].click();", next_button)

                            # 等待页面刷新 - 等待旧按钮消失
                            WebDriverWait(driver, 20).until(
                                EC.staleness_of(next_button)
                            )

                            # 等待新页面加载
                            WebDriverWait(driver, 20).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, "div.ted-search-result"))
                            )
                            time.sleep(2)
                        else:
                            print("下一页按钮不可用")
                            # 保存页面状态
                            driver.save_screenshot("next_button_unavailable.png")
                            break
                    except TimeoutException:
                        print("等待下一页按钮超时")
                        break
                    except NoSuchElementException:
                        print("下一页按钮未找到")
                        break
                    except Exception as e:
                        print(f"导航到下一页时出错: {str(e)}")
                        # 保存页面截图以便调试
                        driver.save_screenshot("next_page_error.png")
                        break

            except Exception as e:
                print(f"处理页面 {page} 时出错: {str(e)}")
                # 保存页面截图以便调试
                driver.save_screenshot(f"page_{page}_error.png")

                # 如果出现严重错误，尝试重新加载页面
                driver.refresh()
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.ted-search-result")))
                time.sleep(5)
                continue

        return df

    except Exception as e:
        print(f"爬取过程中发生严重错误: {str(e)}")
        # 保存错误截图
        if driver:
            driver.save_screenshot("critical_error.png")
        # 保存当前数据
        if 'df' in locals() and not df.empty:
            return df
        return pd.DataFrame()
    finally:
        # 确保关闭浏览器
        if driver:
            try:
                driver.quit()
                print("浏览器已关闭")
            except:
                pass


# 5. 运行爬虫
if __name__ == "__main__":
    print("Starting TED tender scraping with Selenium...")

    # 创建驱动目录
    driver_dir = r'E:\code\project\drivers'
    os.makedirs(driver_dir, exist_ok=True)

    # 检查WebDriver是否存在
    driver_path = os.path.join(driver_dir, 'msedgedriver.exe')
    if not os.path.exists(driver_path):
        print(f"错误: WebDriver 未找到于 {driver_path}")
        print("请下载匹配的 Edge WebDriver 并放置在此目录")
        print(f"Edge版本: 137.0.3296.52")
        print(f"下载地址: https://msedgedriver.azureedge.net/137.0.3296.52/edgedriver_win64.zip")
        sys.exit(1)

    # 运行爬虫
    df = scrape_ted_tenders_selenium(max_pages=1, delay=3)  # 测试1页

    if not df.empty:
        print(f"成功抓取 {len(df)} 条记录")
        print(df.head())

        # 创建保存目录
        save_dir = r'E:\code\project'
        os.makedirs(save_dir, exist_ok=True)

        # 保存结果
        save_path = os.path.join(save_dir, 'ted_tenders.csv')
        df.to_csv(save_path, index=False)
        print(f"结果已保存至 {save_path}")

        # 同时保存为Excel格式
        excel_path = os.path.join(save_dir, 'ted_tenders.xlsx')
        df.to_excel(excel_path, index=False)
        print(f"Excel文件已保存至 {excel_path}")
    else:
        print("未抓取到数据。请检查脚本和网站结构。")
