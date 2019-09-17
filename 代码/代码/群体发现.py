#4.1 微博红酒数据预处理
import csv
import json

csvfile = open('../data/微博红酒.csv', 'r')
reader = csv.reader(csvfile)  # 读取到的数据是将每行数据当做列表返回的
rows = []  # 用来存储解析后的没条数据
for row in reader:
    row_str = ",".join(row)  # row为list类型需转为str，该数据变为字典型字符串
    row_dict = json.loads(row_str)

    # 将每行数据中嵌套字典拆开存储到列表中
    newdict = {}
    for k in row_dict:
        if type(row_dict[k]) == str:  # 将键值对赋给新字典
            newdict[k] = row_dict[k]
        elif type(row_dict[k]) == dict:  # 若存在嵌套字典，将该字典中的key和value作为属性和属性值
            newdict.update(row_dict[k])
    rows.append(newdict)


len(rows)

rows[2]

import pandas as pd
df = pd.DataFrame(rows)#将字典型数组转为DataFrame形式
print(df.shape)
print(df.columns)


df = df[["keyword","post_time","所在地","性别","生日","gender"]]
print(df.shape)
print(df.columns)
#将DataFrame 写入到 csv 文件
df.to_csv("../data/wine_df_weibo.csv",encoding="utf-8_sig", index = False)

# 处理红酒名称（统一规范中文/英文的格式）
# 去keyword中“红酒”字符
df['keyword'] = df['keyword'].str.split('红酒').str[0]

# 整理红酒品牌
brand = pd.read_csv("../data/红酒品牌.csv", header=None, encoding="utf-8")
brands = []
for k1, k2 in zip(list(brand[0]), list(brand[1])):
    if pd.isnull(k2):
        brands.append(k1)
    else:
        brands.append(k1 + "/" + k2)


# 品牌替换
def modify_keywords(w, lists):
    for b in lists:
        if w.strip() in b:
            return b
    return w.strip()


df['keyword'] = df['keyword'].apply(modify_keywords, lists=brands)
df['keyword'] = df['keyword'].str.lower()  # 转化为小写

print(df["keyword"].head(10))


df.head(2)

# 处理性别
# df.count()
# df.groupby("gender").size().sort_values(ascending=False)
df.drop("性别",axis=1,inplace=True)


# 处理年龄
# df.count()
def age(x):
    index = str(x).find('年')
    if index == -1 :
        return 9999
    else:
        res = 2019-int(str(x)[:index])
        # 超过100岁，视为不合理
        if res>100:
            return 9999
        else:
            return 2019-int(str(x)[:index])

df['age'] = df['生日'].map(age)
df.drop("生日",axis=1,inplace=True)

df.head(20)

# 处理地区
df['所在地'].fillna("其他", inplace=True)


def place(x):
    words = str(x).split(" ")
    if len(words) > 1:
        return words[0].strip(), words[1].strip()
    else:
        return words[0], "其他"


df["所在地"] = df["所在地"].map(place)
df['country'] = df["所在地"].apply(lambda x: x[0])
df['region'] = df['所在地'].apply(lambda x: x[1])
df.drop("所在地", axis=1, inplace=True)

df.head(10)


# 处理发布日期
df.drop("post_time",axis=1,inplace=True)

df.head(2)



#4.2 聚类分析
#京东爬下来的数据是不太准的，比如长城的酒，可能有低价格的，也有高价格的，在A类中有300瓶，在B中有600瓶，
#那就认为长城是B类酒，其他类别的酒也是如此做对比。


import pandas as pd
winedf = pd.read_csv("../data/prepare_for_weibo.csv")
winedf.dtypes

resdf = pd.get_dummies(winedf) #非数值型都转化为one-hot

from sklearn.cluster import KMeans

kmodel = KMeans(n_clusters = 6,n_jobs=5)
kmodel.fit(resdf)        #训练模型
rs = kmodel.predict(resdf) #类别索引
winedf['label'] = rs

winedf.groupby("label").agg({'price':['min','max','count']})

classA = winedf[winedf.label==0].groupby(["keyword"]).size().sort_values(ascending=False).reset_index()
classA.columns=["keyword","A"]
classB = winedf[winedf.label==1].groupby(["keyword"]).size().sort_values(ascending=False).reset_index()
classB.columns=["keyword","B"]
classC = winedf[winedf.label==2].groupby(["keyword"]).size().sort_values(ascending=False).reset_index()
classC.columns=["keyword","C"]
classD = winedf[winedf.label==3].groupby(["keyword"]).size().sort_values(ascending=False).reset_index()
classD.columns=["keyword","D"]
classE = winedf[winedf.label==4].groupby(["keyword"]).size().sort_values(ascending=False).reset_index()
classE.columns=["keyword","E"]
classF = winedf[winedf.label==5].groupby(["keyword"]).size().sort_values(ascending=False).reset_index()
classF.columns=["keyword","F"]

print(classA.shape)
print(classB.shape)
print(classC.shape)
print(classD.shape)
print(classE.shape)
print(classF.shape)

classC[:10]

#微博上爬的红酒信息，某个人的信息中可能出现了A类酒中的一个品牌，那这个人就认为关注A类酒。
#把所有关注A类酒的信息做一个统计，显示出来。，就可以看到A类酒受那些人群的关注。
#然后可以做一个精准营销。
level = pd.DataFrame(list(set(winedf['keyword'])),columns=["keyword"])
level = pd.merge(level,classA,how='left',on=["keyword"])
level = pd.merge(level,classB,how='left',on=["keyword"])
level = pd.merge(level,classC,how='left',on=["keyword"])
level = pd.merge(level,classD,how='left',on=["keyword"])
level = pd.merge(level,classF,how='left',on=["keyword"])
level = level.fillna(0)

level['max_value']=level.max(axis=1)
level=level[level.max_value>0]
print(level.shape)
def appendlevel(sr):
    levels=list('ABCDF')
    for i in levels:
        if sr[i] == sr['max_value']:
            return i
level['level'] = level.apply(lambda x:appendlevel(x),axis=1)#每一行apply


level.groupby('level').size()

tmp = level[level.level=='F'].sort_values("max_value",ascending=False)
list(tmp[tmp.max_value>=0]['keyword'])

result = pd.merge(df,level[["keyword","level"]],how="left",on="keyword")

# 删除level为空的数据
result.dropna(axis='index',inplace=True)
print(result.shape)

result.to_csv("level.csv",encoding="utf-8_sig",index = False)

