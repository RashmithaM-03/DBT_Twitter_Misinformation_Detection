# Twitter Misinformation Detection System

This project implements a real-time and batch-processing pipeline to detect misinformation on Twitter. It leverages stream processing, machine learning, and scalable data engineering tools to classify tweets as legitimate or misleading.

## Overview

- **Real-time Processing:** Utilizes Apache Kafka to ingest and stream tweets continuously.
- **Batch Processing:** Periodic analysis using Apache Spark for historical trend insights.
- **ML-based Detection:** Applies trained models to classify tweet content.
- **Storage & Access:** Stores results in MySQL for downstream applications.
- **Visualization:** Supports dashboarding and monitoring through external tools.

## Tech Stack

- **Python**
- **Apache Kafka**
- **Apache Spark**
- **MySQL**
- **scikit-learn / NLP Models**
- **Docker (for containerization)**

## Features

- Dual-mode processing: Real-time + Batch
- Misinformation classification using NLP
- Scalable and modular pipeline design
- Easily extensible for other social platforms

## Getting Started

1. Clone the repository
2. Configure the Kafka, Spark, and MySQL environments
3. Run the data ingestion script to begin streaming
4. Train or load existing ML models for classification
5. Monitor results through your chosen visualization layer

## Structure

