import pandas as pd
df = pd.read_csv("../data/wine_processed.csv")

df.head(2)

df.groupby("price").size()

# 平衡数据
tmp1 = df[df.price=='1000-2000']
tmp2 = df[df.price=='2000-9223372036854775807']
tmp3 = df[df.price=='500-1000']
for i in range(6):
    df = df.append(tmp1, ignore_index=True)
for j in range(3):
    df = df.append(tmp2,ignore_index=True)
df = df.append(tmp3,ignore_index=True)

## 剔除50-100的一半数据
tmp4 = df[df.price=='50-100']
df = df[df.price!='50-100']
df = df.append(tmp4[::2],ignore_index=True)

print(df.groupby("price").size())

# 划分X,Y
X = df[df.columns.difference(['price'])]
Y = df['price']
print(X.shape)
print(Y.shape)

# 对非字符型特征进行数值编码
# X = pd.get_dummies(X)        #onehot编码
from sklearn.preprocessing import LabelEncoder
le = LabelEncoder()
features = [col for col in X.columns.values if X[col].dtype == "object"]         #筛选出object类型的特征
encoders = dict()
for f in features:
    cc = le.fit(X[f])
    encoders[f] = cc.classes_
    X[f] = cc.transform(X[f])
#     X[f] = le.fit_transform(X[f])

# 保存字符转换的规则
import pickle
with open("encoders.dict", "wb") as f:
    pickle.dump(encoders, f)

X.dtypes

from sklearn.model_selection import train_test_split

# 划分数据训练集，测试集
# 80%训练集，20%的测试集；为了复现实验，设置一个随机数,划分结果是确定的
x_train, x_test, y_train, y_test = train_test_split(X, Y, test_size = 0.2, random_state = 42 )

# 模型相关（载入模型--训练模型--模型预测）
from xgboost import XGBClassifier
model = XGBClassifier(learning_rate=0.25, n_estimators=100, objective= "multi:softmax", num_class=8, max_depth=12, subsample =0.95)
model.fit(x_train,y_train)            # 训练模型（训练集）
y_pred = model.predict(x_test)        # 模型预测（测试集），y_pred为预测结果

# 性能评估
from sklearn.metrics import accuracy_score   # 准确率
accuracy = accuracy_score(y_test,y_pred)
print("accuarcy: %.2f%%" % (accuracy*100.0))

from sklearn.metrics import  classification_report
#输出详细的分类性能
print(classification_report(y_pred,y_test,target_names=['0-50','50-100','100-150','150-250','250-500','500-1000','1000-2000','2000-sys.maxsize']))

label = pd.DataFrame(y_test,columns=["price"])
label.groupby("price").size()

prediction = pd.DataFrame(y_pred,columns=["prediction"])
prediction.groupby("prediction").size()

feature_importance=pd.DataFrame(list(model.get_booster().get_fscore().items()),
columns=['feature','importance']).sort_values('importance', ascending=False)
print('',feature_importance)

# 掌握特征重要性之后重新再训练
choose = list(feature_importance[feature_importance.importance > 400]["feature"])
# 划分X,Y
# X = df[df.columns.difference(['price'])]
X = df[choose]
Y = df['price']
print(X.shape)
print(Y.shape)

from sklearn.preprocessing import LabelEncoder

le = LabelEncoder()
features = [col for col in X.columns.values if X[col].dtype == "object"]  # 筛选出object类型的特征
for f in features:
    X[f] = le.fit_transform(X[f])

from sklearn.model_selection import train_test_split

# 划分数据训练集，测试集
# 80%训练集，20%的测试集；为了复现实验，设置一个随机数,划分结果是确定的
x_train, x_test, y_train, y_test = train_test_split(X, Y, test_size = 0.2, random_state = 42 )
#
# # 模型相关（载入模型--训练模型--模型预测）
from xgboost import XGBClassifier
model = XGBClassifier(learning_rate=0.25, n_estimators=100, objective= "multi:softmax", num_class=8, max_depth=12, subsample =0.95)
model.fit(x_train,y_train)            # 训练模型（训练集）
y_pred = model.predict(x_test)        # 模型预测（测试集），y_pred为预测结果
#
# # 性能评估
from sklearn.metrics import accuracy_score   # 准确率
accuracy = accuracy_score(y_test,y_pred)
print("accuarcy: %.2f%%" % (accuracy*100.0))

pickle.dump(model, open("pima.pickle.dat", "wb"))

# 加载模型
import xgboost as xgb
# bst = xgb.Booster({'nthread':4}) #init model
# bst.load_model("xgb1.model") # load data
bst = pickle.load(open("pima.pickle.dat", "rb"))

# 载入测试数据
test_data = df.iloc[[0]]
test = test_data[test_data.columns.difference(['price'])]
Y = test_data['price']
print("the true result: ",Y[0])

# 转换数据
coders = {}
with open("encoders.dict", "rb") as f:
    coders = pickle.load(f)
aa = LabelEncoder()
for x,y in coders.items():
    aa.classes_ = y
    test[x] = aa.transform(test[x])
print(test)
# 预测
p = bst.predict(test[choose])
print("the prediction of test: ",p)
