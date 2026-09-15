"""Bounded run using the existing evaluator with the frozen reduced MCP catalog."""
import asyncio
import evaluate
from replay_catalog import ContextTools
if __name__=='__main__':
    evaluate.MCPTools=ContextTools
    asyncio.run(evaluate.run('human_keywords_v1'))
