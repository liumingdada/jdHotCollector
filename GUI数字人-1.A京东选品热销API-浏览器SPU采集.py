import os
import threading
import time
import re
import json
import hashlib
import requests
from urllib.parse import urlparse, parse_qs, unquote
from datetime import datetime, timedelta
import pandas as pd
from playwright.sync_api import sync_playwright
import PySimpleGUI as sg

# 设置主题
sg.theme('DarkAmber')

# 替换为你的API访问凭证
API_KEY = "85c77421c5fec5717bb1998bc03a920e"
ACCESS_TOKEN = "4ad6269154624f6f93a7d2938226abf9"

# 获取当前日期和时间
now = datetime.now()
today = now.strftime("%Y-%m-%d")

br_Port=12345 #浏览器端口
# 热销榜单ID
rankIdList = [200001, 200002, 200003, 200005, 200006, 200007, 200008]
pages = 10  # 默认页数
commission_min=3 #最低佣金起点，最少佣金数额 int类型
pubNum_max=100

# sortTypeChoices=["选项1", "选项2", "选项3", "选项4"] #1：2小时，2：高佣，3：24小时
sortTypeOptions = {
    "1：2小时": 1,
    "2：高佣": 2,
    "3：24小时": 3
}

sortType=1

# 定义可选择的类别列表
rank_id_list = [
    "200001 - 食品酒水",
    "200002 - 家庭清洁",
    "200003 - 个护美妆",
    "200005 - 生鲜食物",
    "200006 - 数码家电",
    "200007 - 家居日用",
    "200008 - 时尚生活"
]

pathDir=r"C:/选品数据"
if not os.path.exists(pathDir):
    os.mkdir(pathDir)

# 定义布局   commission
            
layout = [
    [sg.Text('选择类别:', size=(10, 1)), sg.Listbox(rank_id_list, size=(40, 7), key='-RANK_IDS-', select_mode=sg.LISTBOX_SELECT_MODE_MULTIPLE)],
    [sg.Text('端口:', size=(4, 1)), sg.Input(key='-BR_PORT-', default_text=br_Port, size=(8, 1)),
     sg.Text('页数:', size=(4, 1)), sg.Input(key='-PAGES-', default_text='10', size=(4, 1)),
     sg.Text('最大渠数:', size=(8, 1)), sg.Input(key='-PUBNUM_MAX-', default_text='100', size=(4, 1)),
     sg.Text('最低佣金:', size=(8, 1)), sg.Input(key='-COMMISSION_MIN-', default_text='3', size=(4, 1)),
     sg.Text('优选类型:', size=(8, 1)), sg.Combo( list(sortTypeOptions.keys()), size=(10, 1), key="-COMBO-", default_value="1：2小时")
     ],
    [sg.Input(f'{pathDir}',key='-saveDir-', size=(47, 1)), sg.FolderBrowse('选择保存目录')], 

    [sg.Button('全选'), sg.Button('开始API采集'), sg.Button('关闭'),sg.Text('*需要打开浏览器第1个TAB页 进入到京东发布 已上传视频界面',font=("微软雅黑", 10))],
    [sg.Multiline(size=(90, 20), key='-OUTPUT-', autoscroll=True, reroute_stdout=True, write_only=True)]
]

# 创建窗口
window = sg.Window('京东选品A.热销榜API采集工具', layout, resizable=True)

# HTTP 请求头部信息
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}

# 主函数
def main():
    while True:
        event, values = window.read()
        if event == sg.WIN_CLOSED or event == '关闭':
            break
        elif event == '开始API采集':
            selected_rank_ids = values['-RANK_IDS-']
            br_Port=values['-BR_PORT-']
            pages = values['-PAGES-']
            #COMMISSION_MIN
            commission_min = values['-COMMISSION_MIN-']
            pubNum_max = values['-PUBNUM_MAX-']
            selected_sortTypeText = values["-COMBO-"]
            sortType = sortTypeOptions.get(selected_sortTypeText, None)
            #pathDir -saveDir-
            pathDir = values['-saveDir-']
            # 确保目录存在
            if not os.path.exists(pathDir):
                os.makedirs(pathDir)


            if not selected_rank_ids:
                print("请选择至少一个类别")
                continue
            
            # 启动数据采集线程
            threading.Thread(target=run_data_collection, args=(selected_rank_ids, pages,commission_min,pubNum_max,sortType,br_Port,pathDir), daemon=True).start()
        elif event == '全选':
            # 设置所有类别为选中状态
            window['-RANK_IDS-'].update(set_to_index=list(range(len(rank_id_list))))

    window.close()

# 定义合并 CSV 文件的函数
def merge_csv_files(csv_files, output_filename):
    # 读取所有 CSV 文件并合并
    df_list = []
    for file in csv_files:
        if file and os.path.exists(file):
            df_list.append(pd.read_csv(file))
    
    if df_list:
        merged_df = pd.concat(df_list, ignore_index=True)
        # # 组合完整的文件路径
        # full_path = os.path.join(pathDir, csv_filename)

        merged_df.to_csv(output_filename, index=False, encoding="utf-8-sig")
        print(f"所有数据已合并并导出到 CSV 文件：{output_filename}")
        # 保存为 .xlsx 文件
        # merged_df.to_excel(output_filename, index=False, engine="openpyxl")
        excel_full_path=output_filename.replace(".csv",".xlsx")
        merged_df.to_excel(excel_full_path, index=False)
        print(f"所有数据已合并并导出到 Excel 文件：{excel_full_path}")
    else:
        print("没有文件可以合并")

        
def run_data_collection(selected_rank_ids, pages,commission_min,pubNum_max,sortType,br_Port,pathDir):
    try:
        with sync_playwright() as playwright:
            # 尝试连接到浏览器
            try:
                browser = playwright.chromium.connect_over_cdp(f"http://localhost:{br_Port}/")
            except playwright._impl._api_types.Error as e:
                if "ECONNREFUSED" in str(e):
                    print("===请手工打开预设端口浏览器 并设置好京东上品界面===")
                    return
                else:
                    raise
            
            context = browser.contexts[0]
            page = context.pages[0]

            generated_csv_files = [] # 初始化一个列表来保存生成的 CSV 文件名   
            # 处理每个选中的类别
            for rank_id_entry in selected_rank_ids:
                rank_id = rank_id_entry.split(' - ')[0]
                print(f"开始处理类别: {rank_id_entry}")
                # 调用数据处理函数                
                csv_file=processSaveCSV(page, int(rank_id), int(pages),commission_min,pubNum_max,sortType,pathDir)
                if csv_file:
                    generated_csv_files.append(csv_file)

            # 合并所有生成的 CSV 文件
            final_output_filename = f"热销-类型{sortType}-{pages}页-{today}.csv"
            # 组合完整的文件路径
            full_path = os.path.join(pathDir, final_output_filename)

            merge_csv_files(generated_csv_files, full_path)
                

            # 关闭浏览器
            browser.close()
    except Exception as e:
        print(f"发生错误: {e}")

def processSaveCSV(page, rankId, pages,commission_min,pubNum_max,sortType,pathDir):
    # 获取数据
    dataListAll = getMorePagesList(rankId, pages,sortType)
    print(f"当前 大类： {rankId},总量： {len(dataListAll)},起佣：{commission_min}")
    
    if len(dataListAll) > 0:
        # 创建DataFrame
        df = pd.DataFrame(dataListAll)
        columns_to_drop = ["purchasePriceInfo", "goodComments", "goodCommentsShare"]
        df = df.drop(columns=columns_to_drop)
        # rankId
        df["类别ID"] =rankId

        df["简化标题"] = df["skuName"].apply(format_title)
        df["SKUID"] = df["itemId"].apply(getSKUID)
        #"commission": "佣金",        # commission_min
        df = df[df["commission"].fillna(0) >= int(commission_min)]
        row_count = df.shape[0]
        print("===行数===:", row_count)
        
        # 获取发布次数
        df["pubNum"] = df["SKUID"].apply(lambda skuid: gePUBNum(page, skuid) if len(skuid) > 3 else None)

        df = df[df["pubNum"].fillna(0) < int(pubNum_max)]
        


        df["SKUID"] = df["SKUID"].astype(str)
        df["网址"] = df["SKUID"].apply(buildFullUrl)
        
        # 重命名字段并导出为CSV
        df = df.rename(columns={
            "itemId": "京粉ID",
            "skuName": "商品名",
            "comments": "评价数量",
            "commission": "佣金",
            "commissionShare": "佣比",
            "imageUrl": "主图"
        })
        csv_filename = f"{rankId}-24小时销售排行-{pages}页-{today}.csv"
        # 组合完整的文件路径
        full_path = os.path.join(pathDir, csv_filename)
        df.to_csv(full_path, index=False, encoding="utf-8-sig")
        print(f"数据已导出到 CSV 文件：{csv_filename}")
        return full_path
    else:
        print("获取数据失败")
        return None

# 其他函数保持不变，直接从你的代码中复制过来
def format_title(title):
    title = re.sub(r'\（.*?\）', '', title)
    title = re.sub(r'\【.*?\】', '', title)
    title = re.sub(r'\《.*?\》', '', title)
    title = re.sub(r'[/%&*]', '', title)
    # 使用正则表达式去除所有非字母、非数字、非空格的字符
    title = re.sub(r'[^\w\s]', '', title)

    return title

def get_redirected_url(url):
    try:
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        response = requests.get(url, headers=headers, allow_redirects=True)
        if len(response.history) > 2:
            first_redirect_response = response.history[2]
            reUrl = first_redirect_response.url
        else:    
            reUrl = response.url
            parsed_url = urlparse(reUrl)
            query_params = parse_qs(parsed_url.query)
            returnurl_encoded = query_params.get('returnurl', [None])[0]
            if returnurl_encoded:
                reUrl = unquote(returnurl_encoded)
        return reUrl
    except requests.exceptions.RequestException as e:
        print(f"取重定向网址 请求错误: {e}")
        return None

def extract_SKU_id(url):
    try:
        pattern = r"https://item\.jd\.com/(\d+)\.html"
        match = re.search(pattern, url)
        return match.group(1) if match else "0"
    except:
        return "0"

def getSKUID(itemId):
    skuid = "0"
    url = f"http://jingfen.jd.com/detail/{itemId}.html"
    reURL = get_redirected_url(url)
    print(f"取得重写向网址:{reURL}")
    if reURL:
        skuid = extract_SKU_id(reURL)
    return skuid

def setAddProURLs(page, SKUID):
    proURLs = []
    proURLs.append(str(SKUID))

    addUrlOK = False
    if len(proURLs) > 0:
        page.click('div[class="addgoods-upload"]')
        page.wait_for_timeout(1000)
        print(f"成功点开 添加商品 弹层界面: setAddProURLs ")

        locator = page.locator("text=搜索添加")
        if locator.is_visible():
            locator.click()
            print("点击成功 搜索添加")
        else:
            print("未找到包含 搜索添加 的元素")

        print("已点 搜索添加， 已切换到对应tab界面")
        page.wait_for_timeout(250)

        input_locator = page.get_by_placeholder("请输入商品关键词或商品ID进行搜索，若有多个，请用英文逗号隔开，每次最多10个。")
        input_locator.click()
        print("已点 输入框， 准备粘贴网址")
        page.wait_for_timeout(50)

        for item in proURLs:
            proURL = item
            input_locator = page.locator("input[placeholder='请输入商品关键词或商品ID进行搜索，若有多个，请用英文逗号隔开，每次最多10个。']")
            input_locator.fill(proURL)

            page.wait_for_timeout(50)
            button_locator = page.locator("text=查询")
            button_locator.click()
            page.wait_for_timeout(1000)

            try:
                buttonYES = page.query_selector('div.sku-list-container > div.arco-spin.sku-list-container-loading > div > div:nth-child(1) > div > label > span > div')
                bt_enabled = buttonYES.is_enabled()
                if bt_enabled:
                    print("查询结果可用，商品网址可用,将写入参考文档，下一步要点选复选框 ; ")
                    page.click('div.sku-list-container > div.arco-spin.sku-list-container-loading > div > div:nth-child(1) > div > label > span > div')
                    page.wait_for_timeout(500)
                    print("已点选复选框，但未确定是否成功选中，需要进行选中否检测 ; ")
                    #body > div.arco-drawer-wrapper > div.arco-drawer.add-goods-drawer-container.slideRight-appear-done.slideRight-enter-done > div > span > div > div.arco-drawer-footer > div > div > div.custom-footer-extend > div > div:nth-child(1) > label
                    #<label aria-disabled="false" class="arco-checkbox arco-checkbox-checked">
                    # 使用 CSS 选择器定位元素
                    locator = page.locator(".arco-checkbox.arco-checkbox-checked")                    
                    if locator.is_visible():# 检查元素是否存在
                        print("复选框选中状态 是，正确，下面将 点 添加 按钮")
                        addUrlOK = True
                    else:
                        print("===???复选框选中状态: 否，???异常，可能被限制，需要手工切换帐号...暂停180秒 ...")
                        time.sleep(180)
                        
                    
                else:
                    print(f"确定按钮处于禁用状态,将循环下一条，当前:{proURL}")
                    continue
            except:
                continue

        
        page.click('body > div.arco-drawer-wrapper > div.arco-drawer.add-goods-drawer-container.slideRight-appear-done.slideRight-enter-done > div > span > div > div.arco-drawer-footer > div > div > div.custom-footer-btns > div > div:nth-child(3) > button')
        print("已点 右下方  添加 按钮")
        page.wait_for_timeout(250)

        if not addUrlOK:
            page.click('body > div.arco-drawer-wrapper > div.arco-drawer.add-goods-drawer-container.slideRight-appear-done.slideRight-enter-done > div > span > div > div.arco-drawer-footer > div > div > div.custom-footer-btns > div > div:nth-child(2) > button')
            page.wait_for_timeout(100)
            print("*** 检测到复选框禁用状态 ***已补点 右下方  取消 按钮")

    else:
        print("没有取得有效商品url网址/SKUID集")

def convert_to_number(pubNumStr):
    if 'w+' in pubNumStr.lower():
        number = float(pubNumStr.replace('w+', '')) * 10000
    elif 'w' in pubNumStr.lower():
        number = float(pubNumStr.replace('w', '')) * 10000
    else:
        number = float(pubNumStr)
    return int(number)

def getPubNumStr_fromPage(page):
    pubNumStr = ""
    element_text = ""
    selector = '#app > div > div > div > div.content > div:nth-child(2) > div.goods-advertise > div.add-goods-video > ul > li:nth-child(1) > div > div.goods-info > div.info-num'
    try:
        element_text = page.locator(selector).text_content()
        print(f"获取到的文本内容：{element_text}")
    except:
        print(f"无法定位元素 未取到的文本内容：渠道已上线*** ")
        print("===???异常 ，可能被限制未处理 ，手工点回主页面或切换帐号 ... 暂停180秒 ...")
        time.sleep(180)


    if element_text:
        match = re.search(r'渠道已上线(.*?)次', element_text)
        if match:
            pubNumStr = match.group(1).strip()
            print(f"提取到的上线次数：{pubNumStr}")
        else:
            print("未找到上线次数")
    else:
        print("未找到目标元素或元素内容为空")

    if pubNumStr:
        try:
            pubNumStr = convert_to_number(pubNumStr)
            pubNumStr = str(pubNumStr)
            print(f"转换后的上线次数：{pubNumStr}")
        except ValueError:
            print("无法转换为数字")
    return pubNumStr

def deletePro(page):
    page.wait_for_load_state("networkidle")

    image_locators = page.locator("div.goodsImg img.goods-item-pic").all()

    for image_locator in image_locators:
        try:
            image_locator.hover()
            print("鼠标已成功悬浮于商品图片元素上方")

            delete_locator = page.locator("div.delete-area")
            if delete_locator.is_visible():
                delete_locator.click()
                print("删除商品 按钮已成功点击")
            else:
                print("删除商品 按钮不可见")
        except Exception as e:
            print(f"处理商品图片时发生错误: {e}")

def gePUBNum(page, SKUID):
    deletePro(page)
    setAddProURLs(page, SKUID)
    pubNum = getPubNumStr_fromPage(page)
    try:
        pubNum = int(pubNum)
    except:
        pubNum = 1234567
    return pubNum

def buildFullUrl(SKUID):
    url = f"https://item.jd.com/{SKUID}.html"
    return url

def getMorePagesList(rankId, pages,sortType):
    dataListArrAll = []
    for pageIndex in range(1, pages + 1):
        dataListTmp = getListGoodsQuery(rankId, pageIndex,sortType)
        # print(f"第{pageIndex}页返回的数据: {dataListTmp}")
        dataListArrAll.extend(dataListTmp)
        print(f"已采集第{pageIndex}页,当前数据量:{len(dataListArrAll)}")
    return dataListArrAll

def getListGoodsQuery(rankId, pageIndex,sortType):
    
    dataListArr = []
    timestamp = (datetime.now() - timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
    timestampC = timestamp + ".043+0800"
    timestampCURL = timestampC.replace(":", "%3A").replace("+", "%2B").replace(" ", "+")
    app_key = API_KEY
    app_secret = ACCESS_TOKEN
    access_token = ""
    methodC = "jd.union.open.goods.rank.query"
    v = "1.0"
    arrSKU = {"RankGoodsReq": {"rankId": rankId, "sortType": sortType, "pageIndex": pageIndex, "pageSize": 10}}
    param_json = json.dumps(arrSKU)
    signStrV1 = "360buy_param_json" + param_json + "access_token" + access_token + "app_key" + app_key + "method" + methodC + "timestamp" + timestampC + "v" + v
    signStr = app_secret + signStrV1 + app_secret
    m = hashlib.md5(signStr.encode()).hexdigest().upper()
    sign = m
    webapiUrl = 'https://api.jd.com/routerjson?access_token=&app_key=' + app_key + '&method=' + methodC + '&v=1.0&sign=' + sign + '&360buy_param_json=' + param_json + '&timestamp=' + timestampCURL
    webapi_json_str = requests.get(webapiUrl).json()
    webapi_data = webapi_json_str
    try:
        webapi_responce = webapi_data['jd_union_open_goods_rank_query_responce']
        resultJStr = webapi_responce['queryResult']
        resultJStr_data = json.loads(resultJStr)
        if 'data' in resultJStr_data and len(resultJStr_data['data']) > 0:
            dataListArr = resultJStr_data['data']
    except:
        print(f"榜单商品列表查询API结果异常jd_union_open_goods_rank_query_responce：{webapi_json_str}")
    return dataListArr

if __name__ == "__main__":
    main()


# GUI数字人-3.口播视频加产品图片-改封面帧.py # pyinstaller -F -w -i iconJD.ico GUI数字人-1.B京东选品浏览器联盟页采集.py
# pyinstaller -F -w -i iconJD.ico GUI数字人-1.A京东选品热销API-浏览器SPU采集.py