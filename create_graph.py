import pandas as pd
import csv
import matplotlib.pyplot as plt
import seaborn as sns

if graph=='hist':
        if os.path.isfile('site/static/Figure.png') is True:
            os.remove('site/static/Figure.png')
        for brand in categlist[0].item:
            names.append(brand)
            colvo.append(len(categlist[0].item[brand]))
        data = pd.DataFrame({'name':names, 'number': colvo})
        return data.pivot_table(index = 'name').plot.bar(rot=0, stacked=True)
        plt.savefig('site/static/Figure.png')
    elif graph=='heatmap':
    #show heatmap
        if os.path.isfile('site/static/Figure.png') is True:
            os.remove('site/static/Figure.png')
        for categ in categlist:
            if categ.category in ['Phones','Ноутбук ', 'Планшет ',"Смарт часы "]:
                for brand in categ.item:
                    # for price in categ.item[brand]:
                    ctgr.append(categ.category)
                    names.append(brand)
                    summ=0
                    for price in categ.item[brand]:
                        summ+=int(price)
                    colvo.append(summ/len(categ.item[brand]))
        data = pd.DataFrame({'category':ctgr,'brand':names, 'price': colvo})
        data.to_csv('dataframe.csv', sep='\t', encoding='utf-8')
        # print(data)
        result=data.pivot(index='category',columns='brand',values='price')
        return sns.heatmap(result,annot=True,fmt="g", cmap='viridis')
        # print(data)
        plt.savefig('site/static/Figure.png')