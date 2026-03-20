"""Генерация полных работ."""

import logging
from .client import DeepSeekClient
from .prompts import SYSTEM_PROMPT, PLAN_PROMPT_TEMPLATE, GENERATE_SECTION_PROMPT

logger = logging.getLogger(__name__)

class WorkGenerator:
    def __init__(self):
        self.client = DeepSeekClient()

    def generate_plan(self, work_type: str, title: str, notes: str = "") -> str:
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": PLAN_PROMPT_TEMPLATE.format(
                work_type=work_type, title=title, notes=notes
            )}
        ]
        return self.client.generate(messages)

    def generate_section(self, section_title: str, work_type: str, title: str,
                         data_context: str = "", previous_text: str = "",
                         target_length: int = 500) -> str:
        prompt = GENERATE_SECTION_PROMPT.format(
            section_title=section_title,
            work_type=work_type,
            title=title,
            data_context=data_context,
            previous_text=previous_text,
            target_length=target_length
        )
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt}
        ]
        return self.client.generate(messages)