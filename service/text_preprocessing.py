from datetime import datetime
from kiwipiepy import Kiwi
from kiwipiepy.utils import Stopwords


class TextPreprocessing:
    def __init__(self, stopwords=None):
        if stopwords is not None:
            if type(stopwords) is not list:
                raise "The type of custom stopwords should be list"
            self.stopwords = stopwords
        else:
            self.stopwords = [w for w, t in Stopwords().stopwords]
        self.documents = []
        self.timestamps = []
        self.original_doc = []
        self.star_rating_list = []
        self.kiwi = Kiwi(typos='basic')

    def add_stopwords(self, word):
        if type(word) is not str:
            raise "Word should be string."
        self.stopwords.append(word)

    def proprocess_text(self, df, product_name=None, star_rating_range=None, pos_list=None):
        # df should have 'content' column.
        if pos_list is None:
            pos_list = ['NNG', 'NNP', 'VV', 'VA']

        def _get_content(row):
            x = row['content']
            time = row['time']
            sr = row['star_rating']
            # sent_tokens = self.kiwi.split_into_sents(x, return_tokens=True)
            # for s in sent_tokens:
            result = []
            word_tokens = self.kiwi.tokenize(x)
            # print(word_tokens)
            for word in word_tokens:
                w = word[0]
                p = word[1]
                if len(w) <= 1 or w in self.stopwords:
                    continue
                if p in pos_list:
                    result.append(w)
            if len(result) > 1:
                self.documents.append(' '.join(result))
                self.timestamps.append(datetime.strptime(time, '%Y-%m-%dT%H:%M:%S.%f%z'))
                self.original_doc.append(x)
                self.star_rating_list.append(sr)

        if star_rating_range is not None:
            df = df[(df['star_rating'] >= star_rating_range[0]) & (df['star_rating'] <= star_rating_range[1])]
        df.apply(_get_content, axis=1)

        pp_timestamps = []
        pp_original_doc = []
        preprocessed_documents = []
        for l in range(len(self.documents)):
            line = self.documents[l]
            if line and not line.replace(' ', '').isdecimal():
                preprocessed_documents.append(line)
                pp_timestamps.append(self.timestamps[l])
                pp_original_doc.append(self.original_doc[l])

        self.documents = preprocessed_documents
        self.timestamps = pp_timestamps
        self.original_doc = pp_original_doc
        # print(product_name, preprocessed_documents[:5])
        print('''Preprocessing is done, You can access to preprocessed data with ".documents"''')


class SimpleTokenizerForBERTopic:
    def __call__(self, sent):
        # sent = sent[:1000000]
        word_tokens = sent.split(' ')
        result = []
        for w in word_tokens:
            if len(w) >= 1:
                result.append(w)

        return result