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
########################################################################################################################


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
########################################################################################################################


def test():
    pass
########################################################################################################################


if __name__ == '__main__':
    test()