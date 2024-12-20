from service.crawl import get_crawl_data
from service.custom_error import NotValidKeywordError, NotEnoughSearchVolumeError
from util.handle_user import change_user_status, delete_status
from service.feature_extraction import FeatureExtraction
from service.forecast import predict_trend
import json
from api.endpoint.data import supabase

def crawl_analysis_background(url, filename, project_name, product_name, category):
    
    # revire crawling
    change_user_status(project_name, 1)
    try:
        res = get_crawl_data(url, filename)
    except NotValidKeywordError:
        change_user_status(project_name, 6)
        return
    change_user_status(project_name, 2)
    
    fe = FeatureExtraction()
    # pros extraction
    fe.train_topic_model_with_bertopic(filename, product_name, star_rating_range=[5, 5])
    pros_topics, pros_rep_token = fe.get_topics_with_keyword(top_n_word=10)
    
    # cons extraction
    try:
        fe.train_topic_model_with_bertopic(filename, product_name, star_rating_range=[1, 3])
        cons_topics, cons_rep_token = fe.get_topics_with_keyword(top_n_word=10)
    except:
        cons_topics = []

    change_user_status(project_name, 3)


    # dtm
    review_to_summ, original_doc = fe.train_topic_model_with_bertopic(filename, product_name)
    dtm_result = fe.get_topics_per_month().to_dict('records')

    for topic_idx in range(len(pros_rep_token)):
        topic_tokens = pros_rep_token[topic_idx]
        for tokens in topic_tokens:
            for i in range(len(original_doc)):
                if original_doc[i]['tokens'] == tokens:
                    original_doc[i]['representative_topic'] = topic_idx+1

    if len(cons_topics) > 0:
        for topic_idx in range(len(cons_rep_token)):
            topic_tokens = cons_rep_token[topic_idx]
            for tokens in topic_tokens:
                for i in range(len(original_doc)):
                    if original_doc[i]['tokens'] == tokens:
                        original_doc[i]['representative_topic'] = -(topic_idx+1)

    change_user_status(project_name, 4)

    #summ_text = summarize(review_to_summ, summ_tokenizer, summ_model)
    summ_text = ' '.join(pros_topics[0])

    forecasting_conducted = True
    forecasting_warning = False
    try:
        past_trend, forecast, start_date, end_date = predict_trend(summ_text, product_name, category, url)
        past_trend = [i*100 for i in past_trend]
        zero_cnt = 0
        for i in past_trend:
            if i == 0:
                zero_cnt += 1
        if zero_cnt > 52:
            forecasting_warning = True
    except NotValidKeywordError:
        change_user_status(project_name, 6)
        return
    except NotEnoughSearchVolumeError:
        forecasting_conducted = False
    except:
        change_user_status(project_name, 6)

    # # db
    product_insert = supabase.table('products').insert({
        'product_name': product_name,
        'pros': pros_topics,
        'cons': cons_topics,
        'csvname': filename,
        'trend': past_trend + forecast.tolist() if forecasting_conducted else [-1],
        'project_name': project_name,
        'trend_start_date': start_date if forecasting_conducted else None,
        'trend_end_date': end_date if forecasting_conducted else None,
        'trend_warning': forecasting_warning,
        'trend_keyword1': product_name,
        'trend_keyword2': category
    }).execute()
    
    product_id = json.loads(product_insert.json())['data'][0]['id']

    original_doc = [{'document': i['document'], 'tokens': i['tokens'], 'topic': i['topic'], 'month': i['month'], 'product_id': product_id, 'representative_topic': i['representative_topic'], 'star_rating': i['star_rating']} for i in original_doc]
    supabase.table("originaldoc").insert(original_doc).execute()

    dtm_result = [{'topic': i['topic'], 'month': i['Timestamp'], 'words': i['words'], 'product_id': product_id} for i in dtm_result]
    supabase.table("dtm").insert(dtm_result).execute()

    delete_status(project_name)
