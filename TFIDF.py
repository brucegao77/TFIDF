import jieba
import math
import pymongo
from collections import Counter
import jieba.analyse


class TFIDF(object):
    def __init__(self):
        self.count = 0

    def content_cut(self, stoppath, content):  # 使用jieba分词
        """
        :return words: jieba处理后的所有分词
        """
        jieba.analyse.set_stop_words(stoppath)
        words = jieba.lcut(content)
        self.count += 1
        print(int(self.count / 19004 * 100), '%')
        return words

    def combine(self,stoppath, contents):  # 对words进行合并
        """
        :return: allwords: 包含重复词汇的所有词汇列表
        """
        allwords = []
        for content in contents:
            words = self.content_cut(stoppath, content)
            allwords += list(set(words))
        return allwords

    def idf(self, stoppath, contents):  # 计算每个词的IDF
        word_count = Counter(self.combine(stoppath, contents))
        word_idf = {}
        for word in word_count.keys():
            word_idf[word] = math.log(len(contents) / word_count[word])
        return word_idf

    def tfidf(self, stoppath, idfpath, content):  # TFIDF计算
        jieba.analyse.set_stop_words(stoppath)
        jieba.analyse.set_idf_path(idfpath)
        keywords = [i for i in jieba.analyse.extract_tags(content, topK=100, withWeight=False)]
        return keywords


if __name__ == '__main__':
    client = pymongo.MongoClient('127.0.0.1', 27017)
    db = client['DDP']
    col = db['content']
    col_tfidf = db['keywords']

    data = col.find({}, {'_id': 0, 'id': 1, 'content': 1})
    ti = TFIDF()
    stoppath = r'H:\doc\百度停用词表.txt'
    idfpath = r'H:\doc\idf.txt'

    # 创建本次文档的IDF
    contents = [i['content'] for i in data]
    word_idf = ti.idf(stoppath, contents)
    with open(idfpath, 'a', encoding='utf-8') as f:
        for word in word_idf.keys():
            f.write('{0} {1}\n'.format(word, word_idf[word]))

    # 通过TFIDF提取特征关键词并存入数据库
    count = 0
    for i in data:
        item = {'id': i['id'], 'keywords': ti.tfidf(stoppath, idfpath, i['content'])}
        col_tfidf.insert(item)
        count += 1
        print('save to mongo: {}'.format(count))


#  因为创建的idf 没有对数据进行清理，因此对jieba 的tfidf.py 作了修改
#     def set_new_path(self, new_idf_path):
#         if self.path != new_idf_path:
#             self.path = new_idf_path
#             content = open(new_idf_path, 'rb').read().decode('utf-8')
#             self.idf_freq = {}
#             for line in content.splitlines():
#                 try:  # 修改部分
#                     word, freq = line.strip().split(' ')
#                     self.idf_freq[word] = float(freq)
#                 except:
#                     print(line)
#             self.median_idf = sorted(
#                 self.idf_freq.values())[len(self.idf_freq) // 2]
