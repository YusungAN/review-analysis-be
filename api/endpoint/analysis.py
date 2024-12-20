from fastapi import APIRouter
from fastapi import BackgroundTasks
from pydantic import BaseModel
import datetime as dt
from util.handle_user import change_user_status
import json
from service.crawl import check_url
from service.analysis import crawl_analysis_background
from db.init_db import supabase

router = APIRouter()

class StartParam(BaseModel):
    url: str
    project_name: str
    product_name: str
    category: str

@router.post('/start')
def crawl_data(info: StartParam, background_tasks: BackgroundTasks):
    # return {'success': False, 'message': '현재 서버 리소스가 부족하여 상품 분석 진행이 어렵습니다.'}

    with open('util/user_status.json', 'r') as file:
            user_status = json.load(file)
            if len(user_status.keys()) > 0:
                 return {'success': False, 'message': '현재 서버 리소스가 부족하여 상품 분석은 한번에 하나의 상품만 가능합니다.'}
    change_user_status(info.project_name, 0)
    now = dt.datetime.now()
    now_str = now.strftime("%Y%m%d%H%M%S")
    filename = 'csv/reviews_{}.csv'.format(now_str)
    project_names = json.loads(supabase.table('products').select('*').eq('project_name', info.project_name).execute().json())
    if len(project_names['data']) > 0:
        return {'success': False, 'message': 'exist project name', 'code': 2}
    if check_url(info.url):
        background_tasks.add_task(crawl_analysis_background, info.url, filename, info.project_name, info.product_name, info.category)
        return {'success': True, 'message': 'crawling in background', 'code': 0}
    return {'success': False, 'message': 'failed to get information, check your url', 'code': 1}