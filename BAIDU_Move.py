import pandas as pd
import numpy as np
import requests
import re
import datetime
import json
import urllib.parse as urlparse
#####获取程序开始运行的时间
a_time = datetime.datetime.now ()

#####################################行政区编码字典
CityCode=pd.read_excel('201910.xlsx',header=1)
######省代码省名称一一对应，并去掉重复的一对值
ProvinceCode=np.asarray(CityCode[['省代码','省名称']],dtype=np.str)
###百度迁徙数据没有获取香港、澳门、台湾的数据，所以取下标0-31
ProvinceCode=np.unique(ProvinceCode,axis=0)[0:31]
###将省编码以及其名称从二维数组将为一维数组(faltten())并转为列表(tolist()),用于创建字典
ProvinceCode_list=ProvinceCode.flatten().tolist()
print(ProvinceCode_list)
###创建字典
dic = dict (zip (ProvinceCode_list[::2], ProvinceCode_list[1::2]))
###用于爬取数据时指定排序
province=['北京市','天津市','河北省','山西省','内蒙古自治区','辽宁省','吉林省','黑龙江省',
'上海市','江苏省','浙江省','安徽省','福建省','江西省','山东省','河南省','湖北省','湖南省','广东省',
'广西壮族自治区','海南省','重庆市','四川省','贵州省','云南省','西藏自治区','陕西省','甘肃省','青海省','宁夏回族自治区','新疆维吾尔自治区']
####province对应的城市编码
code=ProvinceCode_list[::2]

#获取网页内容的函数，返回对于url的response对象 
def get_page(url):
    try:
        response = requests.get(url)
        if response.content and response.status_code==200:  # 内容不为空并且状态码为200代表获取成功
            return response
        ##否则报错
        else :print('error')
    ###截取获取网页过程中连接网页失败的错误并报错
    except requests.ConnectionError as e:
        print('url出错', e.args)


##日期加1函数，百度迁徙数据url参数中date赋值类型为字符串,所以需要进行字符串以及日期的类型转换
##因为直接在字符串上面加1的话会加到100而不是月的天数
def add_one(string):
    ##字符串转为日期
    temp=datetime.datetime.strptime(string,'%Y%m%d')
    ###日期加1
    temp=temp+datetime.timedelta(days=1)
    ##日期转回为字符串
    str_date = temp.strftime("%Y%m%d")
    ##返回值
    return str_date

###第一层循环用于循环中国大部分省份（无台湾、香港、澳门数据）
for Pcode in code:
    ##百度迁徙数据开始日期，字符串类型
    date = '20210119'
    ##保存即将写入文件的第一列的列名
    header = ['迁入来源省份']
    ##保存即将写入文件的行名
    result = np.asarray (province)
    ###第二层循环用于循环日期
    while (date != '20210309'):
        #添加下一列的列名
        header.append (date)
        ####循环爬取不同的省份从20210119-20210308的数据，参数date和province循环改变，province的值是城市编码值哦
        url = 'https://huiyan.baidu.com/migration/provincerank.jsonp?dt=province&id=' + Pcode + '&type=move_out&date=' + date + '&callback=jsonp_1628560725309_1040442'
        # ##acquire id and the Chinese name of province
        # id=urlparse.parse_qs(urlparse.urlparse(url).query)['id']
        ##通过字典获取对应于编码的城市中文名称，是后面的保存文件名
        ProvinceName = dic[Pcode]
        ##获取网页的response对象，response的属性包括encoding（网页编码）、text（网页内容）
        response = get_page (url)
        ####正则化提取数据（re.findall函数）并且进行json数据解码（json.loads解码json文件）
        ###json文件从哪来呢，一般在爬虫时无法从网页源代码看到的数据一般使用json传播数据，url是从百度迁徙网页向服务器发送请求时得到的json数据传输的网页地址（可以这么理解）
        txt = json.loads (re.findall (r'[(](.*?)[)]', response.text)[0])
        ####对txt进行数据提取，Move_in是一个由字典组成的列表
        Move_in = txt['data']['list']
        Province = []
        value = []
        ####第三层循环依照province预先设定好的省份顺序，前往爬虫获取的数据中查询对应的省份迁徙数据并保存至value列表中
        for i in range (len (province)):
            ##是否查找到的标志
            flag = False
            for j in range (len (Move_in)):
                if (province[i] == Move_in[j]['province_name']):
                    value.append (Move_in[j]['value'])
                    ##改变标志
                    flag = True
            ##如果百度迁徙数据里面没有对应的省份则可能是数据为0
            if flag == False:
                value.append ('0')
        ###某一天的省份迁徙数据找完之后转换为数组（用于之后的保存）
        value = np.asarray (value)
        ##不断地合并数据，类似于a=0;sum=4;循环：sum=sum+a;a++;
        result = np.hstack ((result, value))
        ###往后挪一天
        date = add_one (date)
    ########第一行列名
    header = np.asarray (header, dtype=np.str)
    #######重组数组的维数
    result = result.reshape (50, 31).T
    ########合并列名和迁徙数据
    result = np.vstack ((header, result))
    print (result.shape)
    ###保存为csv,分隔符为逗号,格式为字符出(fmt=‘%s’)
    np.savetxt (ProvinceName + '-迁出目的省份.csv', result, delimiter=',', fmt='%s')
#获取现在的时间
b_time = datetime.datetime.now ()
###程序运行的时间
print ('数据处理时长:', (b_time - a_time))