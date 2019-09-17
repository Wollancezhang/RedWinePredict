import pandas as pd
#--encoding='utf-8'
#读取数据，并且把数据作为字典格式
import csv
import json

csvfile=open('../data/电商红酒.csv','r')
reader=csv.reader(csvfile)#读取到的数据是每行数据当做列表返回的
rows=[]#用来存储解析后的数据
for row in reader:
    row_str=",".join(row)
    #row为list类型需转换为str，该数据变为字典型字符串
    row_dict=json.loads(row_str)
    #每行数据中嵌套字典拆开存储到列表中
    newdict={}
    for k in row_dict:
        if type(row_dict[k])==str:
            #将键值对赋给新字典
            newdict[k]=row_dict[k]
        elif type(row_dict[k])==dict:#若存在嵌套字典，则将该字典中的key和value作为属性和属性值
            newdict.update(row_dict[k])
    rows.append(newdict)
#处理红酒名称 统一规范中文、英文格式
#去keyword中“红酒”字段
df=pd.DataFrame(rows)
df['keyword']=df['keyword'].str.split('红酒').str[0]
#整理红酒品牌
brand=pd.read_csv('../data/红酒品牌.csv',header=None,encoding="utf-8")
brands=[]
for k1,k2 in zip(list(brand[0]),list(brand[1])):
    if pd.isnull(k2):
        brands.append(k1)
    else:
        brands.append(k1+"/"+k2)
#品牌替换
def modify_keywords(w,lists):
    for b in lists:
        if w.strip() in b:
            return b
    return w.strip()
df['keyword']=df['keyword'].apply(modify_keywords,lists=brands)
df['keyword']=df['keyword'].str.lower()#转化为小写
print(df["keyword"].head(10))

#2.1.1去重
#若爬取数据有相同的url，则认为是相同数据，只保留一条
#数据是否有相同行，若有返回true，否则false
if df.duplicated(subset=["url"],keep=False).any():
    print("存在重复数据")
    df=df.drop_duplicates(subset=["url"],keep="first")
else:
    print("不存在重复数据")
print(df.shape)

#去除非红酒数据
df=df.dropna(subset=["甜度"])
print(df.shape)

#处理750ml价格
#将删除”产品重量“属性，因为包装不同，礼盒或者整箱产品包装中带有赠品，酒杯酒具等物品，质量变化数据不可靠，难以清洗
#提取瓶数和重新计算价格
#红酒商品描述中含有的数字主要有三种年份、容量和瓶数，容量固定750，主要识别瓶数
#挑出含750字样的数据
df=df[df["name"].str.contains("750")]
print(df.shape)

#处理价格
import jieba.posseg as pseg

def get_numbers(words,num_type="int"):
    #返回string中的数字，args:words:字符串 num_type:获取字符串中float型数字还是int数字
    nums=[]
    for w,p in pseg.cut(words):
        #jieba词性标注不能将“750ml*6”中的6识别为数字，换一种方式
        if num_type=="int":
            try:nums.append(int(w))
            except:continue
        if num_type=="float":
            try:nums.append(float(w))
            except:continue
    return nums

wine_bottles={}#存放红酒瓶数字典
with open("../data/wine_bottles.txt","r",encoding="utf-8") as f:
    for line in f.readlines():
        k,v=line.strip().split(" ")
        wine_bottles[k]=v

df_index =df.index.tolist()#获取df索引列表
df['price']=df["price"].astype("float")#更改数据类型

for i,name in zip(df_index,df["name"]):
    bottles=[int(wine_bottles[k]) for k in wine_bottles if name.find(k) !=-1]
    if len(bottles)==1:
        df['price'].loc[i]=float('%.2f' % (df['price'].loc[i]/bottles[0]))
        continue
    elif len(bottles)>1:
        df=df.drop(index=i,axis=0)
        continue

    numbers=get_numbers(name,"int")
    #bottles==0，用jieba分词提取红酒标题中的数字
    if 750 not in numbers:#选取是含有750样本，但结巴分词可能将750切成其他组合词，识不出来750这个数字
        df=df.drop(index=i,axis=0)
        continue
    numbers = [n for n in get_numbers(name, 'int') if n in [750, 1, 2, 3, 4, 5, 6, 8, 12]] #提取标题中特定数字
    if len(numbers) == numbers.count(750):#只有750数字，认为是单瓶不处理
        continue
    if numbers.count(750) > 1:#存在多个数字且750个数有多个，删除
        df = df.drop(index=i, axis=0)
        continue
    if numbers.index(750) == 0:#存在多个数字，750只有一个，且750后面有数字，更改
        df['price'].loc[i] = float('%.2f' % (df['price'].loc[i] / numbers[1]))
    elif numbers.index(750) == -1:#存在多个数字，750只有一个，且750前面有数字，更改
        df['price'].loc[i] = float('%.2f' % (df['price'].loc[i] / numbers[-2]))
    else:
        df = df.drop(index=i, axis=0) #存在多个数字，750只有一个，且750前面后面有数字，更改
df.drop(["name","包装","容量","产品重量（kg）"], axis=1, inplace=True)

#删除列
print('每列缺失值个数：')
print(df.isnull().sum())#统计每列的缺失值个数
#删除列数据，thresh作用，保留至少2329（11648*20%）个非NA数的列
df = df.dropna(axis=1,thresh=2329)
#再删除['id','shop_id', 'shop_name', 'sku_id', 'url']
df.drop(['id','shop_id', 'shop_name', 'sku_id', 'url'], axis=1, inplace=True)

#处理空值
#主要是处理类别和特性缺失值用众数代替
df.columns.values
df.isnull().sum()
df.groupby("特性").size().sort_values(ascending=False)

#众数替代缺失值

# 上述同居结果可以看出“特性”和“类别”出现缺失值
def process_man(col,gp_col,df):
    #计算该列分组众数，可能出现某个品牌的众数为nan，以“no match”代替
    df_mode = df.groupby(gp_col)[col].agg(lambda x: next(iter(x.value_counts().index),'no match'))
    df[col] = df[col].fillna(df[gp_col].map(df_mode))
    df = df[~df[col].isin(["no match"])] #删除“no match”
    return df

df = process_man("特性","keyword",df)
df = process_man("类别","keyword",df)

# 保存数据
df.to_csv("pre-processed.csv",encoding="utf-8_sig",index = False)

#2.3pandaBI观察各维度特征再处理

df.drop(["保质期","存储方法"],axis=1,inplace=True)

#处理原产地，甜度，颜色
def process_others(col,gp_col,df_other,df_mode,df):
    other_series=df_other[col]
    names=list(df.loc[other_series,gp_col])
    t=df_mode.loc[names,col]
    t.index=df.loc[other_series,col].index
    #col中含有其他的索引
    df.loc[other_series,col]=t
    return df

cols=["原产地","甜度","颜色"]
#需要处理的列
gp_col='keyword'#分组列
df_mode=df.groupby('keyword').agg(lambda x:x.value_counts().index[0])#根据keyword品牌分组计算众数
#print(df_mode[cols])#每个红酒品牌在属性列上的众数

for col in cols:
    df_other=pd.DataFrame(df[col]=='其他')
    #col中存在“其他”行
    df=process_others(col,gp_col,df_other,df_mode,df)
    #因为某品牌红酒在某列上的众数可能是“其他”，经过众数替换后再删除仍存在的“其他的行
    df=df[~df[col].isin(["其它"])]
    #通过~去取反，选取col中不包含“其他”的行

df.shape
# 处理 类别
# list(df.groupby("类别").count().index)
lbs = ["冰酒/贵腐/甜酒","白葡萄酒","果味葡萄酒","桃红葡萄酒","起泡酒/香槟","红葡萄酒"]

df["冰酒/贵腐/甜酒"] = df['类别'].apply(lambda x : 1 if str(x).find("冰酒") != -1 or str(x).find("贵腐")!= -1 or str(x).find("甜酒")!= -1 else 0)
df["白葡萄酒"] = df['类别'].apply(lambda x : 1 if str(x).find("白葡萄酒") != -1 else 0)
df["果味葡萄酒"] = df['类别'].apply(lambda x : 1 if str(x).find("果味葡萄酒") != -1 else 0)
df["桃红葡萄酒"] = df['类别'].apply(lambda x : 1 if str(x).find("桃红葡萄酒") != -1 else 0)
df["红葡萄酒"] = df['类别'].apply(lambda x : 1 if str(x).find("红葡萄酒") != -1 else 0)
df["起泡酒/香槟"] = df['类别'].apply(lambda x : 1 if str(x).find("起泡酒") != -1 or str(x).find("香槟") != -1 else 0)

# df[['类别']+lbs].head
df.drop("类别", axis=1, inplace=True)
# 处理葡萄品种
## 获取葡萄的所有品种类别
tmp = list(df.groupby("葡萄品种").count().index)
graps = []
for i in tmp:
    graps += i.split("|")
graps = list(set(graps))
print(graps)
del tmp

## 处理每个葡萄品种
for j in graps:
    df[j] = df['葡萄品种'].apply(lambda x: 1 if str(x).find(j) != -1 else 0)

# print(df[graps+['葡萄品种']].head)
df.drop("葡萄品种", axis=1, inplace=True)

import numpy as np


# 处理年份,
# 可能出现的数字有：年份(2005,2012等)，年份区间（2016-2018等），年月日(2015.08.06,2012.05等),年数(0,1,3等),年数区间（0-3等，1-3等）

def deal_year(x):
    numbers = []
    for w, p in pseg.cut(x):
        if p == 'm':
            try:
                numbers.append(int(w))
            except:
                continue
    if len(numbers) == 0:
        return 99999  # 年份中无数字，以99999代替
    years = []
    other_nums = []
    for n in numbers:
        if n > 1800 and n < 2020:
            years.append(n)
        elif n < 50:
            other_nums.append(n)
        else:
            continue
    if years:
        return 2019 - int(np.mean(years))
    elif other_nums:
        return int(np.mean(other_nums))
    else:
        return 99999  # 无合理数字，以99999代替


df["year"] = df["年份"].map(deal_year)
df.drop("年份", axis=1, inplace=True)


# 处理酒精度
def deal_alcohol(x):
    numbers = []
    for w, p in pseg.cut(x):
        if p == 'm':
            try:
                numbers.append(float(w))
            except:
                continue
    if len(numbers) > 1:
        return np.mean(numbers)
    elif len(numbers) == 1:
        return numbers[0]
    else:
        return 99999  # 酒精中无数字，以99999代替
df["alcohol"] = df["酒精度"].map(deal_alcohol)
df.drop("酒精度", axis=1, inplace=True)

df.to_csv("prepare_for_weibo.csv",encoding="utf-8_sig",index = False)

print(df["price"].min())
print(df["price"].max())


# 三种方式对价格的划分
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
#% matplotlib inline
plt.rcParams['font.family'] = ['sans-serif']
plt.rcParams['font.sans-serif'] = ['SimHei']

k = 8
kmodel = KMeans(n_clusters = k,n_jobs=5)

fig,ax= plt.subplots(1,3,figsize=(20,5))
cat = pd.cut(df['price'],k)
cat2 = pd.qcut(df['price'],k)
kmodel.fit(df['price'].values.reshape(len(df),1))
c = pd.DataFrame(kmodel.cluster_centers_).sort_values(0)
w = c.rolling(2).mean().iloc[1:]
w = [0] + list(w[0]) + [df['price'].max()]
cat3 = pd.cut(df['price'], w)

cat.value_counts(sort = False).plot.bar(grid= True,ax=ax[0],title = '等宽分箱')
#平均划分价格区间，发现大量的红酒价格都集中在较低的价格区间
cat2.value_counts(sort = False).plot.bar(grid= True,ax=ax[1],title = '等频分箱')
#平均花费数量
cat3.value_counts(sort = False).plot.bar(grid= True,ax=ax[2],title = 'KMeans聚类分箱')


df3 = df[df["price"]>500]
fig,ax= plt.subplots(1,1,figsize=(20,5))
cat4 = pd.qcut(df3['price'],3)
cat4.value_counts(sort = False).plot.bar(grid= True,title = '等频分箱')

df4 = df[df["price"]<=500]
fig,ax= plt.subplots(1,1,figsize=(20,5))
cat4 = pd.qcut(df4['price'],5)
cat4.value_counts(sort = False).plot.bar(grid= True,title = '等频分箱')


# 处理价格区间
# 价格区间划分为：[0,50],[50,100],[100,150],[150,250],[250,500],[500,1000],[1000,2000],[2000,max]
import sys
def get_price_scope(price):        #获取价格区间
    scope = [[0,50],[50,100],[100,150],[150,250],[250,500],[500,1000],[1000,2000],[2000,sys.maxsize]]
    for j in range(len(scope)):
        if price >= scope[j][0] and price < scope[j][1]:
            result = '-'.join(str(x) for x in scope[j])
    return result
df['price'] = df['price'].map(get_price_scope)

df.to_csv("wine_processed.csv",encoding="utf-8_sig",index = False)
df.groupby("keyword").agg({"price":["min","max"]})
