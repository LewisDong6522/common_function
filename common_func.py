#!usr/bin/python
# -*- coding:utf-8 -*-
########################################################################################################################
# Date: 1/18/2018
# Author: Wokea
########################################################################################################################
# 这个函数包主要用来定义一些工作中经常要用到的函数，把其全部保存在这里，以后有需要，则直接调用即可。
########################################################################################################################


def accumulate(list_0):
    """
    这是一个累计贡献率函数，给定一个list，然后返回其累计贡献率list
    :param list_0: 输入序列
    :return: 返回百分比序列
    """
    accumulate_list = []
    for i in range(0, len(list_0)):
        accumulate_list.append(sum(list_0[0:i+1])/sum(list_0))
    return accumulate_list


def bi_search(sequence, number, lower=0, upper=None):
    """
    这是一个二分法的函数，输入一个序列，找到一个数在序列中的位置
    :param sequence: 输入序列
    :param number: 搜索的数字
    :param lower: 序列下限
    :param upper: 序列上线
    :return: 返回所在的位置参数
    """
    if upper is None:
        upper = len(sequence)-1
    if lower == upper:
        assert number == sequence[upper], 'worng'
        return upper
    else:
        middle = (lower+upper)//2
        if number > sequence[middle]:
            return bi_search(sequence, number, middle+1, upper)
        else:
            return bi_search(sequence, number, lower, middle)


def mapping_dict(word, dict_map):
    """
    判断word是否存在字典dict_map当中
    :param word: 新词
    :param dict_map: 存在字典
    :return:
    """
    if word in dict_map.keys():
        print("this element is already in dict")
    else:
        dict_map.setdefault(word, "setDefaultValues")
        print("element is not in dict, we set ")
    return dict_map


def nowTime():
    """
    按照格式打印时间戳
    :return: 规定的时间格式
    """
    dateformat = "%Y-%m-%d %H:%M:%S"
    gettime = datetime.datetime.now().strftime(dateformat)
    return gettime


def is_hanzi(string):
    """
    判断字符串是否为汉字，如果为汉字，则返回来，否则返回false
    :param string: 输入字符串
    :return: 原始字符串或者空值
    """
    if u'\u4e00' < string < u'\u9fa5':
        return string
    else:
        return ''


def read_file(input_file):
    """
    给定一个文件路径，读取其中的内容
    :param input_file: 待输入文件路径
    :return:
    """
    with open(input_file, "r") as file_read:
        while True:
            lines = file_read.readline()
            lines = lines.decode("utf-8").encode("utf-8")
            if not lines:
                break


def filter_string(string0, lists):
    """
    判断字符串是否包含数组中的相关内容，如果包含，则返回false，否则true
    :param string0: 输入字符串
    :param lists: 判断单词列表
    :return:
    """
    count = 0
    lists = lists.split("、")
    for item in lists:
        if item in string0:
            count += 1
    if count == 0:
        return True
    else:
        return False


class MyCorpus(object):
    """
    返回一个语料生成器，常用在word2vec和doc2vec场景下
    """
    def __init__(self, file_pattern):
        self.dirname = file_pattern

    def __iter__(self):
        for file_name in os.listdir(self.dirname):
            with open(os.path.join(self.dirname, file_name)) as file_read:
                for line in file_read:
                    word_list = line.lower().decode('utf8').strip().split()
                    yield word_list


def append_list(a, b):
    """
    :param a: list1
    :param b: list2
    :return: list1.append(list2)
    """
    a.append(b)
    return a


def merge_list(a, b):
    """
    :param a: list1
    :param b: list2
    :return: list1.extend(list2)
    """
    a.extend(b)
    return a

########################################################################################################################


def test():
    pass
########################################################################################################################


if __name__ == '__main__':
    test()
