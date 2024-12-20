from fastapi import APIRouter
import json
from statsmodels.tsa.seasonal import STL
import pandas as pd
from service.crawl import get_product_basic_info
from db.init_db import supabase
from service.autoperiod import calc_seasonality_score
import numpy as np

router = APIRouter()

@router.get('/getdata')
def get_data(product_id: int):
    res = json.loads(supabase.table('products').select('*').eq('id', product_id).execute().json())['data']
    if len(res) == 0:
        return {'success': False, 'message': 'not exist item'}
    dtm_res = json.loads(supabase.table('dtm').select('*').eq('product_id', product_id).execute().json())['data']

    series_decompose = pd.Series(res[0]['trend'], index=pd.date_range(start="12-31-2018", end="1-2-2023", freq="W"), name="seasonal")
    stl = STL(series_decompose, seasonal=13, period=12)
    decompose_res = stl.fit()
    seasonal_data = decompose_res.seasonal.values.tolist()
    trend_data = decompose_res.trend.values.tolist()

    period, seasonality_score = calc_seasonality_score(np.array(res[0]['trend'][:157]))

    return {'success': True, 'message': None, 'data': {'p_data': res[0], 'dtm_result': dtm_res, 'decomposed_trend': trend_data, 'decomposed_seasonal': seasonal_data, 'seasonality_score': seasonality_score, 'period': period}}


@router.get('/getoriginalreview')
def get_original_review(product_id: int, word: str):
    res = json.loads(supabase.table('originaldoc').select('document, tokens').like('tokens', '%'+word+'%').execute().json())['data']

    return {'success': True, 'message': None,'data': res}


@router.get('/getwordtrend')
def get_word_trend(product_id: int, word: str):
    res = json.loads(supabase.rpc('get_trend', {'word': word, 'pid': product_id}).execute().json())['data']
    return {'success': True, 'message': None, 'data': res}


@router.get('/getlist')
def get_list():
    res = json.loads(supabase.table('products').select('id, project_name').execute().json())['data']
    return {'success': True, 'message': None, 'data': res}


@router.get('/basicinfo')
def get_basic_info(url: str):
    try:
        res = get_product_basic_info(url)
        return {'success': True, 'message': None, 'data': res}
    except Exception as e:
        print(e)
        return {'success': False, 'message': None, 'data': None}
    

@router.get('/representative_review')
def get_representative_topic(product_id: int):
    try:
        res = json.loads(supabase.table('originaldoc').select('document, month, star_rating, representative_topic').eq('product_id', product_id).not_.is_('representative_topic', 'null').execute().json())['data']
        return {'success': True, 'message': None, 'data': res}
    except:
        return {'success': False, 'message': None, 'data': None}
    

@router.get('/project_status')
def get_project_status():
    try:
        with open('util/user_status.json', 'r') as file:
            user_status = json.load(file)
            return {'success': True, 'message': None, 'data': user_status}
    except:
        return {'success': False, 'message': None, 'data': {}}