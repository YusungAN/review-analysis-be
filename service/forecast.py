import torch
import pandas as pd
import numpy as np
import pytorch_lightning as pl
from service.GTM import GTM
from pathlib import Path
from sklearn.preprocessing import MinMaxScaler
from sentence_transformers import SentenceTransformer
from service.crawl import get_search_volume


def predict_trend(text, product_name, category, url):
    product_trend, start_date, end_date = get_search_volume(product_name, url)
    cat_trend, start_date, end_date = get_search_volume(category, url)

    print('test', len(product_trend), product_trend)
    print('test', len(cat_trend), cat_trend)

    product_trend = np.array(product_trend)
    cat_trend = np.array(cat_trend)

    print('test2', product_trend)
    print('test2', cat_trend)

    product_trend = MinMaxScaler().fit_transform(product_trend.reshape(-1, 1)).flatten()
    cat_trend = MinMaxScaler().fit_transform(cat_trend.reshape(-1, 1)).flatten()
    multitrends = np.vstack([product_trend, cat_trend])

    former_trend = [multitrends]
    former_trend = torch.FloatTensor(np.array(former_trend))


    print(text, 'embedding start')
    embedding_model = SentenceTransformer('beomi/KcELECTRA-base-v2022')
    text = torch.FloatTensor(embedding_model.encode([text]))
    print('embedding end')

    device = torch.device(f'cpu')
    pl.seed_everything(21)

    model = GTM(
            embedding_dim=32,
            hidden_dim=64,
            output_dim=52,
            num_heads=4,
            num_layers=1,
            use_text=1,
            use_img=0,
            trend_len=157,
            num_trends=2,
            use_encoder_mask=1,
            autoregressive=0,
            gpu_num=0
        )
    
    print('load start')
    model.load_state_dict(torch.load('util/gtm-summed.ckpt', map_location=device)['state_dict'], strict=False)
    print('load end')
    model.to(device)
    model.eval()
    y_pred, att = model(text, former_trend)
    y_pred = y_pred.detach().cpu().numpy().flatten()[:52]
    print(y_pred)
    final_y = 100*(y_pred-np.min(y_pred))/(np.max(y_pred)-np.min(y_pred))
    return product_trend, final_y, start_date, end_date

def summarize(text_li, tokenizer, model):
    text = ' '.join(text_li)
    text = text.replace('\n', ' ')
    raw_input_ids = tokenizer.encode(text)
    if len(raw_input_ids) > 1024:
        raw_input_ids = raw_input_ids[:1024]
    input_ids = [tokenizer.bos_token_id] + raw_input_ids + [tokenizer.eos_token_id]

    summary_ids = model.generate(torch.tensor([input_ids]), num_beams=4, max_length=1024, eos_token_id=1)
    return tokenizer.decode(summary_ids.squeeze().tolist(), skip_special_tokens=True)
