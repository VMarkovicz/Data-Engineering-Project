# src/rag_api/main.py
import os
import datetime
from typing import List, Optional
from fastapi.middleware.cors import CORSMiddleware

from fastapi import FastAPI
from pydantic import BaseModel
from openai import OpenAI

from rag.rag_execution import EnhancedRAGRetrieval  # adjust import

# ---- OpenAI + DB setup ----
OPEN_AI_KEY = os.getenv("OPEN_AI_KEY")

DB_SETTINGS = {
    "dbname": os.getenv("POSTGRES_DB", "rag_db"),
    "user": os.getenv("POSTGRES_USER", "rag_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "rag_password"),
    "host": os.getenv("POSTGRES_HOST", "db"),  # <--- important: docker service name
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
}

client = OpenAI(api_key=OPEN_AI_KEY)
rag = EnhancedRAGRetrieval(DB_SETTINGS, client)

# ---- FastAPI app ----
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],        # allow from any origin, including file:// (null)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class Message(BaseModel):
    role: str
    content: str

class ChatRequest(BaseModel):
    model: Optional[str] = None
    messages: List[Message]

@app.post("/v1/chat/completions")
async def chat_completions(req: ChatRequest):
    # take last user message as the query
    user_msg = next(
        (m.content for m in reversed(req.messages) if m.role == "user"),
        req.messages[-1].content,
    )

    answer = rag.answer_question_with_balanced_context(
        user_msg,
        total_k=200,
        final_k=150,
    )

    # OpenAI‑style response
    return {
        "id": f"chatcmpl-{datetime.datetime.utcnow().timestamp()}",
        "object": "chat.completion",
        "created": int(datetime.datetime.utcnow().timestamp()),
        "model": rag.chat_model,
        "choices": [
            {
                "index": 0,
                "finish_reason": "stop",
                "message": {
                    "role": "assistant",
                    "content": answer,
                },
            }
        ],
    }
