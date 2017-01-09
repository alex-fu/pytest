# coding=utf-8

import csv


def convert_to_float(percent_str):
    index = percent_str.find('%')
    percent = percent_str[0:index]
    # print(percent)
    return float(percent) / 100


class StockBussinessInfo:
    def __init__(self, code, name, business):
        self.code = code
        self.name = name
        self.business_orig = business
        self.business_map = self.format_business(business)

    def format_business(self, business_str):
        r1 = business_str.strip().split(',')
        r = {}
        for x in r1:
            pair = x.strip().split(':')
            if len(pair) == 2:
                r[pair[0]] = pair[1]

        # print("r", r)
        return r

    # return: abroad_business_percent (double)
    def get_abroad_business(self, abroad_keywords, not_abroad_keywords):
        def area_contains_key(area, keys):
            for key in keys:
                if key in area:
                    return True
            return False

        percent = 0.0
        for area, per in self.business_map.items():
            if area_contains_key(area, abroad_keywords) and not area_contains_key(area, not_abroad_keywords):
                percent += convert_to_float(per)

        # print(self.code, self.name, self.business_orig, percent)
        return percent

    def __str__(self):
        return "[{}, {}, {}]".format(self.code, self.name, self.business_orig)


if __name__ == '__main__':
    abroad_keywords_file = '/home/fuyf/vmshare/cjd/abroad_keywords.csv'
    not_abroad_keywords_file = '/home/fuyf/vmshare/cjd/not_abroad_keywords.csv'
    stock_bussiness_file = '/home/fuyf/vmshare/cjd/bussiness.csv'

    abroad_keywords = []
    not_abroad_keywords = []
    stock_bussinesses = []

    with open(abroad_keywords_file) as f:
        for key in csv.reader(f):
            abroad_keywords.append(key[0])

    with open(not_abroad_keywords_file) as f:
        for key in csv.reader(f):
            not_abroad_keywords.append(key[0])

    with open(stock_bussiness_file) as f:
        header_stripped = False
        for row in csv.reader(f):
            if not header_stripped:
                header_stripped = True
                continue
            stock_bussinesses.append(StockBussinessInfo(row[0], row[1], row[2]))

    print("Abroad keys:", abroad_keywords)
    print("Not abroad keys:", not_abroad_keywords)

    r1 = {}
    r2 = {}
    for x in stock_bussinesses:
        percent = x.get_abroad_business(abroad_keywords, not_abroad_keywords)
        r1[x.code] = x
        r2[x.code] = percent

    r = sorted(r2.items(), key=lambda d: d[1], reverse=True)

    r_over_40 = [x for x in r if x[1] >= 0.4]
    r_30_to_40 = [x for x in r if 0.3 <= x[1] < 0.4]
    r_20_to_30 = [x for x in r if 0.2 <= x[1] < 0.3]
    r_10_to_20 = [x for x in r if 0.1 <= x[1] < 0.2]
    r_less_10 = [x for x in r if x[1] < 0.1]


    # print(r_over_40)
    # print(r_30_to_40)
    # print(r_20_to_30)
    # print(r_10_to_20)
    # print(r_less_10)

    # output
    def gen_output_row(info, per):
        row = []
        row.append(info.code)
        row.append(info.name)
        row.append("%.2f%%" % (per * 100))
        row.append("%.2f%%" % (100 - per * 100))
        row.append(info.business_orig)
        return row


    # r_over_40
    r_over_40_row_group = []
    for code, per in r_over_40:
        r_over_40_row_group.append(gen_output_row(r1[code], per))

    # r_30_to_40
    r_30_to_40_row_group = []
    for code, per in r_30_to_40:
        r_30_to_40_row_group.append(gen_output_row(r1[code], per))

    # r_20_to_30
    r_20_to_30_row_group = []
    for code, per in r_20_to_30:
        r_20_to_30_row_group.append(gen_output_row(r1[code], per))

    # r_10_to_20
    r_10_to_20_row_group = []
    for code, per in r_10_to_20:
        r_10_to_20_row_group.append(gen_output_row(r1[code], per))

    # r_less_10
    r_less_10_row_group = []
    for code, per in r_less_10:
        r_less_10_row_group.append(gen_output_row(r1[code], per))

    fieldnames = ['证券代码', '证券名称', '国外收入占比', '国内收入占比', '详细收入情况']
    with open('/home/fuyf/vmshare/cjd/r_over_40.csv', 'w', newline='', encoding='gbk') as f:
        writer = csv.writer(f)
        writer.writerow(fieldnames)
        writer.writerows(r_over_40_row_group)

    with open('/home/fuyf/vmshare/cjd/r_30_to_40.csv', 'w', newline='', encoding='gbk') as f:
        writer = csv.writer(f)
        writer.writerow(fieldnames)
        writer.writerows(r_30_to_40_row_group)

    with open('/home/fuyf/vmshare/cjd/r_20_to_30.csv', 'w', newline='', encoding='gbk') as f:
        writer = csv.writer(f)
        writer.writerow(fieldnames)
        writer.writerows(r_20_to_30_row_group)

    with open('/home/fuyf/vmshare/cjd/r_10_to_20.csv', 'w', newline='', encoding='gbk') as f:
        writer = csv.writer(f)
        writer.writerow(fieldnames)
        writer.writerows(r_10_to_20_row_group)

    # print(r_less_10_row_group)
    with open('/home/fuyf/vmshare/cjd/r_less_10.csv', 'w', newline='', encoding='gbk') as f:
        writer = csv.writer(f)
        writer.writerow(fieldnames)
        writer.writerows(r_less_10_row_group)
