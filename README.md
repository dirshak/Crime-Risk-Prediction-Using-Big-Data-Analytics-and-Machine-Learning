# Crime-Risk-Prediction-Using-Big-Data-Analytics-and-Machine-Learning
A scalable crime risk prediction system using Apache Spark and Random Forest, leveraging spatiotemporal analytics and interactive visualization.

### Big Data Analytics and Machine Learning using Apache Spark

---

## Overview

This project presents a scalable crime risk prediction system built using Big Data Analytics and Machine Learning.
It analyzes over 7 million crime records to identify high-risk areas and patterns, enabling data-driven decision-making.

The system combines:

* Distributed data processing (Apache Spark)
* Machine learning (Random Forest)
* Interactive visualization (Streamlit and Plotly)

---

## System Architecture

The system follows a five-layer architecture:

1. Data Source Layer

   * Chicago Crime Dataset (2001–Present)

2. Data Processing Layer

   * Data cleaning and transformation using PySpark

3. Machine Learning Layer

   * Feature engineering (density, time risk, severity)
   * Random Forest model

4. Storage Layer

   * Clean data stored in Parquet format
   * Predictions stored in CSV format

5. Visualization Layer

   * Interactive dashboard using Streamlit

---

## Features

* Handles large-scale datasets (7M+ records)
* Advanced spatiotemporal feature engineering
* Composite risk scoring system
* Scalable distributed machine learning pipeline
* Interactive geospatial dashboard
* Real-time filtering and analytics

---

## Machine Learning Approach

### Features Used

* Hour, Day, Month
* District and Community Area
* Area Crime Density
* Time Risk Factor
* Crime Severity

### Risk Score Formula

```id="m1f0jk"
Risk = 0.5 × Density + 0.3 × Time Risk + 0.2 × Severity
```

### Model

* Algorithm: Random Forest
* Trees: 50
* Max Depth: 12

### Performance

* Accuracy: ~81%
* AUC-ROC: ~0.87
* Strong generalization with minimal overfitting

---

## Visualizations

The system generates:

* Crime trends over time
* Top crime districts
* Heatmaps (District vs Hour)
* Interactive crime maps
* Risk distribution analysis

---

## Dashboard Features

Built with Streamlit, the dashboard includes:

* Risk threshold slider
* Crime type filtering
* Interactive map (Mapbox)
* Density heatmap
* Analytical charts and insights

---

## Project Structure

```id="q2j3pw"
crime-risk-prediction/
│
├── data/
│   └── Crimes_-_2001_to_Present.csv
│
├── outputs/
│   ├── clean_data/
│   ├── risk_predictions/
│   └── visualizations/
│
├── scripts
|  ├──1_data_pipeline.py
|  ├──2_analytics.py
|  └──3_ml_model.py
├── dashboard.py
│
├── notebook/
│   └── crime_prediction_pipeline.ipynb
│
├── requirements.txt
└── README.md
```

---

## How to Run

### 1. Install Dependencies

```bash id="x3n8ak"
pip install -r requirements.txt
```

### 2. Run Data Pipeline

```bash id="k4s9lp"
python 1_data_pipeline.py
```

### 3. Train Model

bash id="c8n2df"
python 3_ml_model.py

### 4. Launch Dashboard

bash id="h7t4rq"
streamlit run dashboard.py

## Tech Stack

| Category         | Tools Used                  |
| ---------------- | --------------------------- |
| Big Data         | Apache Spark (PySpark)      |
| Machine Learning | Random Forest (Spark MLlib) |
| Visualization    | Matplotlib, Seaborn, Plotly |
| Dashboard        | Streamlit                   |
| Storage          | Parquet, CSV                |

---

## Key Insights

* Crime is concentrated in specific geographic areas
* Evening and late-night hours show higher risk levels
* Engineered features contribute significantly to model performance
* Risk-based analysis is more effective than raw crime counts

---

## Future Improvements

* Real-time streaming using Kafka or Spark Streaming
* Integration of deep learning models (LSTM, Graph Neural Networks)
* Multi-city scalability
* Deployment as an API-based prediction system

---

## Authors

* Dirshak Deepak Patro
* Jata Bhavika

---

## License

This project is intended for academic and research purposes.

---

## Final Note

This project demonstrates how Big Data, Machine Learning, and Visualization can be integrated to build a practical and scalable crime analysis system.
