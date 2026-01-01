@echo off
start "News Producer" uv run src/sandbox_producers/sandbox_news_producer.py
start "Track Producer" uv run src/sandbox_producers/sandbox_track_producer.py
