import os
from cloud import get_all
import collections
import pandas as pd
import seaborn as sns

def make_fig(site_data,graph):
    if graph=='hist':
        brands_counter = collections.Counter()
        for i in site_data['Brand']:
            brands_counter[i] += 1
        brands_counter_1 = dict()
        for i in brands_counter.keys():
            if brands_counter[i] > 2:
                brands_counter_1[i] = brands_counter[i]
        brands_data = pd.DataFrame(brands_counter_1.items(), columns=['Brand', 'counter'])
        return brands_data.pivot_table(index='Brand').plot.bar(rot=0, stacked=True)
    elif graph=='heatmap':
        site_data_group_1 = site_data.groupby(by=['Category', 'Brand'], observed=True)['Price'].mean()
        prices = []
        for i in site_data_group_1:
            prices.append(i)
        brands = []
        types = []
        for i in site_data_group_1.keys():
            brands.append(i[1])
            types.append(i[0])
        site_data_1 = pd.DataFrame({'price': prices, 'brand': brands, 'category': types})
        return sns.heatmap(site_data_1.pivot(index='brand', columns='category')['price'])