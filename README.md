# News-Driven Company Sentiment Pipeline

## Overview

I have built a pipeline that collects company-related news articles,
stores them in Snowflake,
links articles to tracked companies,
computes sentiment scores,
and prepares summary tables for analysis and visualization.

## Architecture

GDELT
→ Kafka
→ Spark Structured Streaming
→ VADER Sentiment Analysis
→ Snowflake
→ Dashboard

## Technologies

- Python
- Kafka
- Spark Structured Streaming
- Snowflake
- VADER Sentiment Analysis
- SQL

## Key Features

- Real-time news ingestion
- Company mention detection
- Sentiment analysis
- Streaming analytics
- Dashboard reporting

## Repository Structure

...
