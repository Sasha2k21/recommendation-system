import streamlit as st
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RecommenderApp").getOrCreate()

@st.cache_data
def load_recommendations(user_id):
    df = spark.read.parquet("data/recommendations.parquet")
    user_df = df.filter(df.userId == user_id).toPandas()
    return user_df

# Streamlit UI
st.title("🎬 Movie Recommender System")
st.markdown("Get top movie recommendations based on your preferences.")

# Input user ID
user_id = st.number_input("Enter User ID:", min_value=1, step=1)

if st.button("Get Recommendations"):
    try:
        recommendations = load_recommendations(user_id)
        if recommendations.empty:
            st.warning("No recommendations found for this user.")
        else:
            st.success(f"Top recommendations for User {user_id}:")
            st.dataframe(recommendations[['movieId', 'title']])
    except Exception as e:
        st.error(f"Error loading recommendations: {e}")
