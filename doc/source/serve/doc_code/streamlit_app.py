# __streamlit_imports_begin__
import streamlit as st
from ray import serve
from transformers import pipeline
import requests
# __streamlit_imports_end__


# __streamlit_model_begin__
if "model" not in st.session_state:
    serve.start()

    @serve.deployment
    def model(request):
        language_model = pipeline("text-generation", model="gpt2")
        query = request.query_params["query"]
        return language_model(query, max_length=100)

    model.deploy()
    st.session_state["model"] = True
# __streamlit_model_end__

# __streamlit_test_begin__
example = "What's the meaning of life?"
response = requests.get(f"http://localhost:8000/model?query={example}")
print(response.text)
# __streamlit_test_end__


# __streamlit_gpt_begin__
def gpt2(query):
    response = requests.get(f"http://localhost:8000/model?query={query}")
    return response.json()[0]["generated_text"]
# __streamlit_gpt_end__


# __streamlit_app_begin__
st.title("Serving a GPT-2 model")

query = st.text_input(label="Input prompt", value="What's the meaning of life?")

if st.button("Run model"):
    output = gpt2(query)

    st.header("Model output")
    st.text(output)
# __streamlit_app_end__
