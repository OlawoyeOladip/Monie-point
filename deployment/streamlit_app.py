import streamlit as st
import pandas as pd
import joblib
import shap
import matplotlib.pyplot as plt
from datetime import datetime
import os
import sys
import re
from sklearn.preprocessing import LabelEncoder

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.anomaly_detection.utils.common import convert_to_usd
from features.feature_engineering import FeatureEngineer

# ===== SETTINGS =====
st.set_page_config(page_title="Anomaly Detector", layout="wide")
st.markdown("<h1 style='text-align: center; color: #FF4B4B;'>🔍 Anomaly Detection Dashboard</h1>", unsafe_allow_html=True)

# ===== SIDEBAR INFO =====
st.sidebar.header("📌 Instructions")
st.sidebar.write("""
1. Fill in the transaction details
2. Click **Generate Features** to create features
3. Click **Predict Anomaly** to get prediction
""")

# ===== DATA CLEANING & STANDARDIZATION =====
def standardize_string(s):
    """Standardize strings by removing special chars, lowercase, and replacing spaces with underscores"""
    if pd.isna(s) or str(s).strip().lower() in ['unknown', 'none', 'null', '']:
        return 'unknown'
    s = str(s).strip().lower()
    s = re.sub(r'[^a-z0-9]+', '_', s)  # Replace special chars with underscore
    s = re.sub(r'_+', '_', s)  # Remove duplicate underscores
    return s.strip('_')

def preprocess_data(df):
    """Clean and standardize the raw data"""
    # Standardize categorical columns
    cat_cols = ['device', 'transaction_type', 'location']
    for col in cat_cols:
        df[col] = df[col].apply(standardize_string)
    
    # Additional standardization rules
    df['transaction_type'] = df['transaction_type'].replace({
        'topup': 'top_up',
        'top-up': 'top_up',
        'topup_': 'top_up',
        'top_up_': 'top_up'
    })
    
    return df

# ===== LOAD DATA & MODEL =====
@st.cache_data
def load_data():
    raw_df = pd.read_csv(r"moniepoint\artifacts\data_ingestion\cleaned_anomaly_detection.csv")
    return FeatureEngineer(raw_df).engineer_batch()

# loading the df
df = load_data()

FEATURES = ['device', 'transaction_type', 'location', 'amount', 
           'day_of_week', 'hour_of_day', 'month', 'quarter',
           'is_weekend', 'day_of_month', 'is_business_hours',
           'transaction_count_last_7_days',
           'average_transaction_amount_last_10_days',
           'days_since_last_transaction', 'unique_locations_used', 'new_device',
           'amount_z_score_user', 'hours_since_last_transaction_user',
           'transaction_count_today_user', 'amount_percentile_user']

@st.cache_resource
def load_model():
    model = joblib.load("moniepoint/artifacts/model/isolation_forest_model.joblib")
    if isinstance(model, dict):
        return model['model']
    return model

model = load_model()

# Initialize label encoders with standardized categories
@st.cache_resource
def get_label_encoders():
    encoders = {
        'device': LabelEncoder().fit(df['device'].astype(str)),
        'transaction_type': LabelEncoder().fit(df['transaction_type'].astype(str)),
        'location': LabelEncoder().fit(df['location'].astype(str))
    }
    return encoders

encoders = get_label_encoders()

# ===== FORM FOR USER INPUT =====
with st.form("input_form"):
    # Get standardized unique values for dropdowns
    device_options = sorted(df['device'].unique())
    transaction_options = sorted(df['transaction_type'].unique())
    location_options = sorted(df['location'].unique())
    
    datetime_input = st.date_input("Transaction Date", value=datetime.today())
    time_input = st.time_input("Transaction Time", value=datetime.now().time())
    user_id = st.text_input("User ID")
    transaction_type = st.selectbox("Transaction Type", transaction_options)
    amount = st.number_input("Amount", min_value=0.0, step=0.01)
    currency = st.selectbox("Currency", ["GBP", "USD", "EUR"])
    location = st.selectbox("Location", location_options)
    device = st.selectbox("Device", device_options)
    
    submitted_features = st.form_submit_button("🛠️ Generate Features")

if submitted_features:
    # Standardize user inputs
    transaction_type = standardize_string(transaction_type)
    location = standardize_string(location)
    device = standardize_string(device)
    
    # Create single-row DataFrame
    input_df = pd.DataFrame([{
        "datetime": datetime.combine(datetime_input, time_input),
        "user_id": user_id,
        "transaction_type": transaction_type,
        "amount": amount,
        "currency": currency,
        "location": location,
        "device": device
    }])
    
    # Feature engineering
    fe = FeatureEngineer(df)
    input_df_engineered = fe.engineer_single(user_id, transaction=input_df.iloc[0].to_dict())
    
    # Type conversion and label encoding
    input_df_engineered['is_business_hours'] = input_df_engineered['is_business_hours'].astype(float)
    input_df_engineered['is_weekend'] = input_df_engineered['is_weekend'].astype(float)
    
    
    try:
        for col in ['device', 'transaction_type', 'location']:
            # Handle unseen categories by mapping to 'unknown'
            input_df_engineered[col] = input_df_engineered[col].apply(
                lambda x: x if x in encoders[col].classes_ else 'unknown'
            )
            input_df_engineered[col] = encoders[col].transform(input_df_engineered[col])
    except Exception as e:
        st.error(f"Encoding error: {str(e)}")
        st.stop()
    
    # Currency conversion and feature validation
    input_df_engineered = convert_to_usd(input_df_engineered)
    for col in FEATURES:
        if col not in input_df_engineered.columns:
            input_df_engineered[col] = 0
    
    # Store results
    st.session_state['features'] = input_df_engineered[FEATURES]
    st.success("✅ Features generated successfully!")
    st.dataframe(input_df_engineered[FEATURES])
# ===== PREDICTION SECTION =====
# ===== PREDICTION SECTION =====
if 'features' in st.session_state:
    st.markdown("---")
    
    # Create two columns for prediction results and summary plot
    col1, col2 = st.columns([1, 3])
    
    with col1:
        st.subheader("🔮 Prediction Result")
        if st.button("🔍 Predict Anomaly"):
            X = st.session_state['features']
            
            with st.spinner('Analyzing transaction...'):
                try:
                    prediction = model.predict(X)[0]
                    st.session_state['prediction'] = prediction
                    
                    if prediction == -1:
                        st.error("⚠️ Anomaly Detected", icon="🚨")
                    else:
                        st.success("✅ Normal Instance", icon="✔️")
                    
                    st.write("")
                    
                    try:
                        scores = model.decision_function(X)
                        st.metric("Anomaly Score", f"{scores[0]:.2f}", 
                                help="Higher values indicate more anomalous")
                        st.session_state['scores'] = scores
                    except:
                        pass
                
                except Exception as e:
                    st.error(f"❌ Prediction failed: {str(e)}")

    with col2:
        if 'prediction' in st.session_state:
            st.subheader("📊 Feature Importance")
            
            try:
                X = st.session_state['features']
                explainer = shap.TreeExplainer(model)
                shap_values = explainer.shap_values(X)
                
                # SHAP Summary Plot
                fig1, ax1 = plt.subplots(figsize=(10, 6))
                shap.summary_plot(shap_values, X, plot_type="bar", show=False)
                st.pyplot(fig1, clear_figure=True)
                plt.close()
                
            except Exception as e:
                st.warning(f"⚠️ Could not generate feature importance: {str(e)}")

    # Individual Feature Impact - Full width below columns
    if 'prediction' in st.session_state:
        st.markdown("---")
        st.subheader("🧩 Individual Feature Impact")
        
        try:
            X = st.session_state['features']
            explainer = shap.TreeExplainer(model)
            shap_values = explainer.shap_values(X)
            
            # Custom CSS for full-width force plot
            st.markdown(
                """
                <style>
                .full-width-force-plot {
                    width: 100% !important;
                    height: 500px !important;
                    margin: 0 auto;
                    border: 1px solid #f0f2f6;
                    border-radius: 8px;
                    padding: 20px;
                    background: white;
                }
                </style>
                """,
                unsafe_allow_html=True
            )
            
            # Create full-width force plot
            force_plot = shap.force_plot(
                explainer.expected_value,
                shap_values[0],
                X.iloc[0],
                feature_names=X.columns,
                matplotlib=False,
                plot_cmap=["#FF4B4B", "#00D4A1"],
                text_rotation=0
            )
            
            # Full width container
            with st.container():
                st.components.v1.html(
                    f"""
                    <div class="full-width-force-plot">
                        {shap.getjs()}
                        {force_plot.html()}
                    </div>
                    """,
                    height=500
                )
            
        except Exception as e:
            st.warning(f"⚠️ Could not generate individual feature impact: {str(e)}")