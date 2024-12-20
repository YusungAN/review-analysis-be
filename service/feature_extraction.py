import pandas as pd
import numpy as np
from bertopic.representation import KeyBERTInspired
from bertopic import BERTopic
from service.text_preprocessing import TextPreprocessing, SimpleTokenizerForBERTopic
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from sklearn.preprocessing import normalize
from datetime import datetime, timezone
from collections import defaultdict
from util.time_similarity_metric import pearson_corr, mse, dynamic_time_warping, minmax_scaler

class FeatureExtraction:
    def __init__(self):
        self.csv_path = ''
        self.raw_data = None
        self.product_name = None
        self.topic_model = None
        self.n_topic = None
        self.timestamps = None
        self.documents = None
        self.word_tfidf_per_month = None
        self.word_set = None

    def train_topic_model_with_bertopic(self, csv_path, product_name, n_topic=5, star_rating_range=None):
        self.csv_path = csv_path
        self.raw_data = pd.read_csv(csv_path)
        custom_tokenizer = SimpleTokenizerForBERTopic()
        vectorizer = CountVectorizer(tokenizer=custom_tokenizer, max_features=3000)
        representation_model = KeyBERTInspired()

        text_pp = TextPreprocessing()
        text_pp.proprocess_text(self.raw_data, product_name=product_name, star_rating_range=star_rating_range)

        model = BERTopic(embedding_model="beomi/KcELECTRA-base-v2022",
                         representation_model=representation_model,
                         vectorizer_model=vectorizer,
                         nr_topics=n_topic,
                         top_n_words=30,
                         calculate_probabilities=True)
        print(text_pp.documents[:10])
        model.fit_transform(text_pp.documents) #[:100])
        self.topic_model = model
        self.n_topic = n_topic
        self.timestamps = text_pp.timestamps
        self.documents = text_pp.documents
        print('traning end')
        if star_rating_range is None:
            return self.raw_data.content[:10].values.tolist(), [{'document': text_pp.original_doc[i], 
                                                                 'tokens': self.documents[i], 
                                                                 'topic': self.topic_model.topics_[i], 
                                                                 'month': '{}. {}.'.format(self.timestamps[i].year, self.timestamps[i].month), 
                                                                 'star_rating': text_pp.star_rating_list[i],
                                                                 'representative_topic': None} for i in range(len(self.timestamps))]


    def optimize_topic_number(self):
        raise "not implemented"

    def get_topics_with_keyword(self, top_n_word=10):
        ret = []
        for i in range(0, self.n_topic - 1):
            if not self.topic_model.get_topic(i):
                continue
            print('topic {}:'.format(i + 1), end=' ')
            tmp = []
            for w, p in self.topic_model.get_topic(i)[:top_n_word]:
                tmp.append(w)
            print(*tmp)
            ret.append(tmp)
        
        rep_docs = [self.topic_model.representative_docs_[key] for key in self.topic_model.representative_docs_.keys()]
                
        return ret, rep_docs

    def get_topics_per_month(self):
        labels = self.topic_model.topics_

        # 임시 제한
        # labels = labels[:100]
        # self.documents = self.documents[:100]
        # self.timestamps = self.timestamps[:100]

        sorted_timestamps = sorted(self.timestamps)
        min_year = sorted_timestamps[0].year
        min_month = sorted_timestamps[0].month
        max_year = sorted_timestamps[-1].year
        max_month = sorted_timestamps[-1].month

        # per month
        now_year = min_year
        now_month = min_month
        topics_over_time = []
        topic_word_per_month = {}

        documents = pd.DataFrame({"Document": self.documents, "Topic": labels, "Timestamps": self.timestamps})

        while not now_year < max_year or (now_year == max_year and now_month <= max_month):
            
            start_time = datetime(now_year, now_month, 1).astimezone(timezone.utc)
            end_time = datetime(now_year if now_month < 12 else now_year + 1, now_month + 1 if now_month < 12 else 1,
                                1).astimezone(timezone.utc)
            dddf = documents[documents['Timestamps'] >= start_time]
            selection = dddf[dddf['Timestamps'] < end_time]
            documents_per_topic = selection.groupby(['Topic'], as_index=False).agg({'Document': ' '.join,
                                                                                    "Timestamps": "count"})
            try:
                c_tf_idf, words = self.topic_model._c_tf_idf(documents_per_topic, fit=False)
                c_tf_idf = normalize(c_tf_idf, axis=1, norm='l1', copy=False)
            except:
                now_year = now_year if now_month < 12 else now_year + 1
                now_month = now_month + 1 if now_month < 12 else 1
                continue

            if not (now_year == min_year and now_month == min_month):
                current_topics = sorted(list(documents_per_topic.Topic.values))
                overlapping_topics = sorted(list(set(previous_topics).intersection(set(current_topics))))

                current_overlap_idx = [current_topics.index(topic) for topic in overlapping_topics]
                previous_overlap_idx = [previous_topics.index(topic) for topic in overlapping_topics]

                c_tf_idf.tolil()[current_overlap_idx] = ((c_tf_idf[current_overlap_idx] +
                                                          previous_c_tf_idf[previous_overlap_idx]) / 2.0).tolil()

            words_per_topic = self.topic_model._extract_words_per_topic(words, selection, c_tf_idf, calculate_aspects=False)
            topic_word_per_month['{}. {}.'.format(now_year, now_month)] = words_per_topic
            topic_frequency = pd.Series(documents_per_topic.Timestamps.values,
                                        index=documents_per_topic.Topic).to_dict()

            topics_at_timestamp = [(topic,
                                    ", ".join([words[0] for words in values][:30]),
                                    topic_frequency[topic],
                                    '{}. {}.'.format(now_year, now_month)) for topic, values in words_per_topic.items()]
            topics_over_time.extend(topics_at_timestamp)

            previous_topics = sorted(list(documents_per_topic.Topic.values))
            previous_c_tf_idf = c_tf_idf.copy()

            now_year = now_year if now_month < 12 else now_year + 1
            now_month = now_month + 1 if now_month < 12 else 1

        word_tfidf_per_time, word_set = self._get_word_tfidf_per_month(topic_word_per_month, topic_idx=1)
        # print('test', word_tfidf_per_time)
        self.word_tfidf_per_month = word_tfidf_per_time
        self.word_set = word_set
        dtm = pd.DataFrame(topics_over_time, columns=["topic", "words", "Frequency", "Timestamp"])

        return dtm

    @staticmethod
    def _get_word_tfidf_per_month(topic_word_per_month, topic_idx):
        word_tfidf_per_time = defaultdict(list)
        word_set = set()
        for idx, time in enumerate(topic_word_per_month.keys()):
            try:
                for idx2, wp in enumerate(topic_word_per_month[time][topic_idx]):
                    w, p = wp
                    if not w in word_set:
                        word_set.add(w)
                        for _ in range(idx):
                            word_tfidf_per_time[w].append(0)

                    if len(word_tfidf_per_time[w]) == idx:
                        word_tfidf_per_time[w].append(p)
                    else:
                        word_tfidf_per_time[w][-1] += p

                for w in list(word_set):
                    if idx + 1 != len(word_tfidf_per_time[w]):
                        word_tfidf_per_time[w].append(0)
            except KeyError:
                pass

        return word_tfidf_per_time, word_set

    def get_keywords_with_time_series(self, dtm, topic_idx, metric='pearson', top_n_words=5):
        metric_li = ['pearson', 'mse', 'dtw']
        topic_freq = dtm.loc[dtm.Topic == topic_idx, 'Frequency'].values

        if not metric in metric_li:
            raise "metric should be one of the {}.".format(metric_li)

        loss_func = {'pearson': pearson_corr, 'mse': mse, 'dtw': dynamic_time_warping}
        s_idx = []
        corr = []
        for w in list(self.word_set):
            s_idx.append(w)
            corr.append(loss_func[metric](np.array(self.word_tfidf_per_month[w]), minmax_scaler(topic_freq)))

        corr_res = pd.Series(corr, index=s_idx)
        return corr_res.sort_values(ascending=False if metric == 'pearson' else True)[:top_n_words]