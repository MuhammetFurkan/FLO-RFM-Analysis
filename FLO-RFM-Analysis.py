import datetime as dt
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.float_format',lambda x:'%.2f'%x)
pd.set_option('display.width',1000)


df_ = pd.read_csv(r"Homeworks/furkan_islamoglu/2_CRM_Analitigi/hw1/flo_data_20k.csv")
df = df_.copy()



df.head(10)
df.columns
df.shape
df.describe().T
df.isnull().sum()
df.info()

# For omnichannel total over {online + offline}

df["order_num_total"] = df["order_num_total_ever_online"] + df["order_num_total_ever_offline"]

# Total cost for omnichannel

df["customer_value_total"] = df["customer_value_total_ever_online"] + df["customer_value_total_ever_offline"]

#  Change the type of date expressing variables to date.

date_columns = df.columns[df.columns.str.contains("date")]
df[date_columns] = df[date_columns].apply(pd.to_datetime)
df.info

# order channel , total of purchase and total expenditure distribution

df.groupby("order_channel").agg({"master_id":"count",
                                 "order_num_total":"sum",
                                "customer_value_total":"sum"})

#  Top 10 shoppers

df.sort_values("customer_value_total",ascending=False)[:10]

# Top 10 customers with the most orders giving

df.sort_values("order_num_total",ascending=False)[:10]


def data_prep(dataframe):
    dataframe["order_num_total"] = dataframe["order_num_total_ever_online"] + dataframe["order_num_total_ever_offline"]
    dataframe["customer_value_total"] = dataframe["customer_value_total_ever_online"] + dataframe["customer_value_total_ever_offline"]
    date_columns = dataframe.columns[dataframe.columns.str.contains("date")]
    dataframe[date_columns] = dataframe[date_columns].apply(pd.to_datetime)

    return df

############################# Calculating RFM Metrics #############################

df["last_order_date"].max()

analysis_date = dt.datetime(2021,6,1)

rfm = pd.DataFrame()

rfm["customer_id"] = df["master_id"]

rfm["recency"] = (analysis_date - df["last_order_date"]).astype('timedelta64[D]')

rfm["frequency"] = df["order_num_total"]

rfm["monetary"] = df["customer_value_total"]

rfm.head()

############################# Calculating RF and RFM Scores #############################

rfm["recency_score"] = pd.qcut(rfm['recency'],5,labels=[5,4,3,2,1])

rfm["frequency_score"] = pd.qcut((rfm['frequency']).rank(method="first"),5,labels=[1,2,3,4,5])

rfm["monetary_score"] = pd.qcut(rfm['monetary'],5,labels=[1,2,3,4,5])

# Recency,Frequency ---> RF SCORE

rfm["RF_SCORE"] = (rfm["recency_score"].astype(str) +
                   rfm["frequency_score"].astype(str))

# Recency,Frequency,Monetary Metrics ---> RFM SCORE

rfm["RFM_SCORE"] = (rfm['recency'].astype(str) + rfm['frequency'].astype(str) + rfm['monetary'].astype(str))

#############################  Definition of RF Scores as Segments #############################

seg_map = {
    r'[1-2][1-2]': 'hibernating',
    r'[1-2][3-4]': 'at_Risk',
    r'[1-2]5': 'cant_loose',
    r'3[1-2]': 'about_to_sleep',
    r'33': 'need_attention',
    r'[3-4][4-5]': 'loyal_customers',
    r'41': 'promising',
    r'51': 'new_customers',
    r'[4-5][2-3]': 'potential_loyalists',
    r'5[4-5]': 'champions'
}

rfm["segment"] = rfm["RF_SCORE"].replace(seg_map,regex=True)
rfm.head()


rfm[["segment","recency","frequency","monetary"]].groupby("segment").agg(["mean","count"])




############################# CASES #############################
"""

Case 1: A new brand of women's shoes will be added. It was planned as target customers (champions, loyal customers).
Reach customers' identification numbers.

"""

target_segments_customer_ids = rfm[rfm["segment"].isin(["champions","loyal_customers"])]["customer_id"]
cust_ids = df[(df["master_id"].isin(target_segments_customer_ids)) &(df["interested_in_categories_12"].str.contains("KADIN"))]["master_id"]
cust_ids.to_csv("new_brand_target_customer_id.csv", index=False)
cust_ids.shape
rfm.head()

"""

Case 2: Up to 40% discount is planned for Men's and Children's products. New customers who have not shopped for a long time are particularly targeted.
Reach customers' identification numbers.

"""



target_segments_customer_ids = rfm[rfm["segment"].isin(["cant_loose","hibernating","new_customers"])]["customer_id"]
cust_ids = df[(df["master_id"].isin(target_segments_customer_ids)) & ((df["interested_in_categories_12"].str.contains("ERKEK"))|(df["interested_in_categories_12"].str.contains("COCUK")))]["master_id"]
cust_ids.to_csv("indirim_hedef_müşteri_ids.csv", index=False)