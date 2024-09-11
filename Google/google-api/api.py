from fastapi import FastAPI
from pydantic import BaseModel
from search import Search
from fastapi.middleware.cors import CORSMiddleware


s = Search()
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://google-v2f-fr.preview-domain.com"],  # Spécifiez les domaines autorisés
    allow_credentials=True,
    allow_methods=["*"],  # Autorise toutes les méthodes, vous pouvez aussi spécifier seulement ["GET", "POST"]
    allow_headers=["*"],  # Autorise tous les headers, ajustez selon vos besoins
)

class Search(BaseModel):
    query: str

@app.post("/search/")
async def search(item: Search):
    return s.search(item.query)