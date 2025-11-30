# Spotify Charts vs News Sentiment Analysis

Analyzing correlations between musical features of German Spotify Top 200 charts and news sentiment.

## Overview

This system investigates whether daily music listening patterns reflect the mood of news events in Germany (elections, major events, etc.).

![Architecture Diagram](architecture.png)

## Architecture

**Kappa Architecture** using streaming data:

1. **Data Sources**

   - Spotify Top 200 charts (web scraping)
   - Musical features via RapidAPI (tempo, energy, danceability, valence, etc.)
   - News sentiment via GDELT API

2. **Kafka Topics**

   - `daily_charts` - Daily Spotify rankings
   - `musical_features` - Track audio characteristics
   - `news_tones` - Daily news sentiment scores

3. **Processing**

   - **Spark** consumes Kafka topics, joins by date, calculates correlations

4. **Storage**
   - **Supabase** stores raw data and correlation results

## Tech Stack

- **uv** - Python package management
- **Kafka** - Event streaming
- **Spark** - Stream processing
- **Supabase** - Data warehouse
- Python for producers/scrapers

## Project Status

Initial setup phase - code coming soon
