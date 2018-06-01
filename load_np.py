# Загрузка область, район, муниципальное образование, населенный пункт из файла из ССФО в базу
import csv
from configobj import ConfigObj
import pandas as pd

#df = pd.read_csv('af.csv', sep = ';', header=None, skiprows=1, encoding= 'cp1251')
df = pd.read_csv('mrf.csv', sep = ';', header=None, skiprows=1, encoding= 'cp1251')
#print(df)
region = df[1].drop_duplicates()
for row_region in region:
    print(row_region)
    municipalitet = df.loc[df[1] == row_region][2].drop_duplicates()
    print(municipalitet)
    for row_municipalitet in municipalitet:
        poselenie = df.loc[(df[1] == row_region) & (df[2] == row_municipalitet)][3].drop_duplicates()
        print(poselenie)
    

# with open("af.csv") as csvfile:
#     reader = csv.DictReader(csvfile, delimiter=";")
#     next(reader)
#     p = []
#     for row in reader:
#         print(row)
#         p.append(row)
#     #p = list(reader)

# print(p)



# reader = csv.DictReader(open('mrf.csv', 'rb'))
# #dict = pd.read_csv('mrf.csv')
# dict_list = []
# for line in reader:
#     print(line)
#     dict_list.append(line)
# print()