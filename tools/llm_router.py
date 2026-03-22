"""Smart LLM routing with rate limiting for Groq."""

import os
import threading
import time
from typing import Any, List, Optional

from dotenv import load_dotenv
from langchain_openai import ChatOpenAI

load_dotenv()


class SmartLLMRouter:
    """Rate-limited Groq client. Stays under 28 requests/minute."""

    max_per_minute: int = 28

    def __init__(self) -> None:
        groq_key = os.getenv("GROQ_API_KEY")
        if not groq_key:
            raise RuntimeError("GROQ_API_KEY not set in .env")
        self.llm = ChatOpenAI(
            base_url="https://api.groq.com/openai/v1",
            api_key=groq_key,
            model="moonshotai/kimi-k2-instruct",
            temperature=0.4,
            max_tokens=4000,
        )
        self._timestamps: List[float] = []
        self._lock = threading.Lock()

    def _clean_timestamps(self, now: float) -> None:
        self._timestamps = [t for t in self._timestamps if t > now - 60.0]

    def invoke(self, prompt_or_messages: Any) -> Any:
        with self._lock:
            now = time.time()
            self._clean_timestamps(now)
            if len(self._timestamps) >= self.max_per_minute:
                sleep_time = 60.0 - (now - self._timestamps[0]) + 0.5
                self._lock.release()
                time.sleep(sleep_time)
                self._lock.acquire()
                now = time.time()
                self._clean_timestamps(now)
            self._timestamps.append(now)
        result = self.llm.invoke(prompt_or_messages)
        time.sleep(4)
        return result


router = SmartLLMRouter()


def get_llm() -> SmartLLMRouter:
    return router


if __name__ == "__main__":
    r = get_llm()
    response = r.invoke("Say hello")
    text = getattr(response, "content", response)
    print(f"provider: groq")
    print(f"response: {text}")
