from supabase import create_client, Client
import os
from dotenv import load_dotenv

load_dotenv()

SUPA_URL = os.environ.get('SUPA_URL')
SUPA_SECRET = os.environ.get('SUPA_PW')

supabase: Client = create_client(SUPA_URL, SUPA_SECRET)