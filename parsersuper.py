from bs4 import BeautifulSoup
import requests, re, asyncio,aiohttp, csv
import pandas as pd

def asparsecat(page):
    cataloglinks=[]
    soup=BeautifulSoup(page, 'html.parser')
    tags=soup.find_all('a',href=True)
    for tag in tags:
        link=tag['href']
        if re.search(r'\/pages', str(link)) and link not in cataloglinks:
            if re.search(r'javascript',str(link)):
                break
            cataloglinks.append(link)
    return cataloglinks

def asparseproduct(page):
    prodlinks=[]
    soup=BeautifulSoup(page, 'html.parser')
    tags=soup.find_all('a',href=True)
    for tag in tags:
        link=tag['href']
        if re.search(r'\/product', str(link)):
            if re.search(r'javascript',str(link)) is None:
                prodlinks.append(link)
    return prodlinks

def category_finder(soup):
    tags=soup.find_all("dl", attrs={'class':['n-product-spec']})
    for tag in tags:
        if tag.find('span',attrs={'class':'n-product-spec__name-inner'}).text=="Тип устройства":
            return tag.find('span',attrs={'class':'n-product-spec__value-inner'}).text
        elif tag.find('span',attrs={'class':'n-product-spec__name-inner'}).text=="Тип":
            return tag.find('span',attrs={'class':'n-product-spec__value-inner'}).text
    return None

def brand_finder(name):
    alph='йцукенгшщзхъфывапролджэячсмитьбюё#ЙЦУКЕНГШЩЗХЪФЫВАПРОЛДЖЭЯЧСМИТЬБЮ'
    if "Flash" in name:
        words=name.split()
        return words[1],words[4], words[2]+" "+words[3]
    words=name.split()
    i=0
    category=''
    while words[i][0] in alph:
        if words[i][0] !='#':
            category+=(str(words[i])+' ')
        i+=1
    try:
        (words[i][0])
        brand=words[i]
        model=''
        for po in words[i+1:]:
            model+=str(po)+' '
        if category=='':
            category="cмартфон"
        return brand, model, category
    except:
        brand=category
        model=category
        return brand, model, category

def prettify(brand, model, category):
    brand=brand.replace('\xa0','')
    model=model.replace('\xa0','')
    category=category.replace('\xa0','')
    try:
        int(brand)
        model=brand+model
        brand="Noname"
        return brand, model, category
    except:
        if brand=="Redmi" or brand=="Mi":
            model=brand+" "+model
            brand="Xiaomi"
        return brand, model, category

country_to_brand = {'China': ['Huawei', 'Xiaomi', 'OnePlus'],
                        'USA': ['Apple', 'Beats', 'GarmiH', 'Google'],
                        'South Korea': ['Samsung', 'LG'],
                       'Japan': ['Sony']} 
def know_country(brand):
    for i in country_to_brand.keys():
        if brand in country_to_brand[i]:
            return i
    return None

def asparseprice(page):
    soup = BeautifulSoup(page, 'html.parser')
    try:
        name = str(soup.find('h1').text)
        price=int(re.findall(r"\d+\s\d+", str(soup.find("h2")))[0].replace('\xa0', ''))
        category=category_finder(soup)
        brand, model, compare_category = brand_finder(name)
        if category is None:
            category=compare_category
        brand, model,category = prettify(brand, model, category)
        return brand, model,category, price, know_country(brand)
    except:
        return None

async def asnc(link,way):
    async with aiohttp.ClientSession() as session:
        async with session.get('https://hatewait.ru'+link) as resp:
            if way=='catalog':
                fin = asparsecat(await resp.text())
            elif way=='product':
                fin = asparseproduct(await resp.text())
            elif way=='price':
                fin = asparseprice(await resp.text())
            return fin, link

def save_file(name, data, main_link=""):
    fl=open(name,'w+')
    for i in sorted(data):
        fl.write(main_link+str(i)+'\n')
    fl.close()
    print('file_saved')

def read_file(name):
    data=[]
    with open(name) as f:
        for line in f:
            data.append(line[:-1])
        f.close()
    return data

def parse_catalogs(write=False):
    cataloglinks=[]
    finalcatalogs=set()
    itr=3
    for i in range(int(itr)):
        tasks=[]
        loop= asyncio.new_event_loop()
        try:
            for j in cataloglinks[i-1]:
                tasks.append(loop.create_task(asnc(j,'catalog')))
        except:
            tasks.append(loop.create_task(asnc('','catalog')))
        fin = loop.run_until_complete(asyncio.wait(tasks))
        loop.close()
        for i in fin[0]:
            if i._result is not None:
                cataloglinks.append(i._result[0])     
    for i in cataloglinks:
        finalcatalogs.update(i)
    if write is True:
        save_file('hatewaitcatalog.txt',finalcatalogs)
    return finalcatalogs

def parse_product_pages(finalcatalogs=[],write=False):
    try:
        finalcatalogs=read_file('hatewaitcatalog.txt')
    except:
        finalcatalogs=parse_catalogs(write=True)
    productlinks=set()
    tasks=[]
    loop=asyncio.new_event_loop()
    for catalog in finalcatalogs:
        tasks.append(loop.create_task(asnc(catalog,'product')))
    fin = loop.run_until_complete(asyncio.wait(tasks))
    loop.close()
    for i in fin[0]:
        if i._result is not None:
            for link in i._result[0]:
                productlinks.add(link)
    if write is True:
        save_file('hatewaitproduct.txt',productlinks)
    return productlinks

def get_product_info(productlinks=[],write=False):
    try:
        productlinks=read_file('hatewaitproduct.txt')
    except:
        productlinks=parse_product_pages(write=True)
    dataset=[]
    event_loop=[]
    events=[]
    count=0
    for link in productlinks:
        events.append(link)
        count+=1
        if count == 20:
            count=1
            event_loop.append(events)
            events=[]
    if len(events) != 0:
        event_loop.append(events)
    for event in event_loop:
        tasks=[]
        loop= asyncio.new_event_loop()
        for link in event:
            tasks.append(loop.create_task(asnc(link,'price')))
        fin = loop.run_until_complete(asyncio.wait(tasks))
        for i in fin[0]:
            if i._result[0] is not None:
                dataset.append([i._result[0][0],i._result[0][1],i._result[0][2],
                i._result[0][3],i._result[0][4],'https://hatewait.ru'+i._result[1]])
        loop.close()
    dataset=pd.DataFrame(dataset,columns=['Brand','Model','Category','Price','Country','Link'])
    if write is True:
        dataset.to_csv('site_data.csv', encoding='utf-8')
    # if write is True:
    #     save_file("hatewait_raw_data.txt", dataset)
    return dataset

def download_all_data():
    catalog=parse_catalogs(write=True)
    products=parse_product_pages(catalog,write=True)
    get_product_info(products,write=True)

def get_all_data():
    try:
        dataset=pd.read_csv('site_data.csv', encoding='utf-8')
    except:
        dataset=get_product_info(write=True)
    brands=dataset['Brand'].unique().tolist()
    countries=list(country_to_brand.keys())
    categories=dataset["Category"].unique().tolist()
    brands_df=pd.DataFrame({"Brand Name":brands})
    countries_df=pd.DataFrame({"Country Name":countries})
    categories_df=pd.DataFrame({"Category Name":categories})
    dataset["Brand"].replace(brands,brands_df.index.to_list(),inplace=True)
    dataset["Country"].replace(countries, countries_df.index.to_list(),inplace=True)
    dataset["Category"].replace(categories,categories_df.index.to_list(),inplace=True)
    return dataset, brands_df, countries_df, categories_df
    



