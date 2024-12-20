from fastapi import APIRouter
from fastapi.responses import FileResponse

router = APIRouter()

@router.get("/downloadcsv")
def download_csv(filename: str):
    if filename[:7] != 'reviews' or filename[-3:] != 'csv':
        return None;
    return FileResponse(path='csv/'+filename, filename=filename)