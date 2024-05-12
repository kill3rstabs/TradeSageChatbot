from flask import Flask, render_template, request, jsonify
import os
from flask_lt import run_with_lt
import torch
from datasets import load_dataset
from transformers import (
    AutoModelForCausalLM,
    AutoTokenizer,
    BitsAndBytesConfig,
    TrainingArguments,
    pipeline,
    logging,
)
from peft import LoraConfig, PeftConfig, PeftModel, get_peft_model, prepare_model_for_kbit_training
from trl import SFTTrainer

def load_model():
  bnb_config = BitsAndBytesConfig(
      load_in_4bit=True,
      bnb_4bit_use_double_quant=True,
      bnb_4bit_quant_type="nf4",
      bnb_4bit_compute_dtype=torch.bfloat16
  )


  PEFT_MODEL = "Lohit20/fyp"


  config = PeftConfig.from_pretrained(PEFT_MODEL,token='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')
  model = AutoModelForCausalLM.from_pretrained(
      config.base_model_name_or_path,
      token='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',
      return_dict=True,
      quantization_config=bnb_config,
      device_map="auto",
      trust_remote_code=True
  )

  tokenizer=AutoTokenizer.from_pretrained(config.base_model_name_or_path,token='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')
  tokenizer.pad_token = tokenizer.eos_token

  model = PeftModel.from_pretrained(model, PEFT_MODEL)
  return model, tokenizer


def llm_response(user_input,model,tokenizer):
  generation_config = model.generation_config
  generation_config.max_new_tokens = 1024
  generation_config.temperature = 0.95
  generation_config.top_p = 0.9
  generation_config.num_return_sequences = 1
  generation_config.pad_token_id = tokenizer.eos_token_id
  generation_config.eos_token_id = tokenizer.eos_token_id
  system_message = """You are being fine-tuned to serve as a trade analyst. Your role is to interpret trading data accurately and provide clear, informed explanations and insights based on the data.
  Follow these rules:
  1.Understand Queries:Accurately interpret user queries related to trading data, such as trends, volume, price changes, and market sentiment
  2.Data Analysis:Analyze historical data to identify patterns or trends.
  3.Explanation and Communication:
    -Provide explanations that reflect a deep understanding of trading concepts and data analysis.
    -Explain the implications of the data in terms of trading strategy and market behavior.
    -Use clear, professional language appropriate for financial analysis.
    -Incorporate relevant financial theories or models as necessary to support your analysis.
  4.Response Structure:
    -Begin with a direct answer to the user’s question.
    -Follow with a detailed explanation of the analysis you performed.
    -Conclude with practical insights or recommendations based on the data.
  5.Accuracy and Reliability:
    -Ensure all data interpretations are accurate and based on reliable data sources.
    -Clearly indicate any assumptions or limitations in your analysis.
  6.Engagement:
    -Respond to follow-up questions to clarify or delve deeper into specific aspects of the analysis.
    -Maintain an engaging and professional tone throughout the conversation."""

  # user_input = "I want to understand Chile's top 3 import suppliers in 2020. Could you provide details on these suppliers, including the countries and their respective total trade values?"

  prompt = f"<s>[INST] <<SYS>>{system_message}<</SYS>>{user_input} [/INST]"

  device = "cuda"
  encoding = tokenizer(prompt, return_tensors="pt").to(device)
  with torch.inference_mode():
    outputs = model.generate(
        input_ids = encoding.input_ids,
        attention_mask = encoding.attention_mask,
        generation_config = generation_config
    )
  response=tokenizer.batch_decode(outputs.detach().cpu().numpy(), skip_special_tokens=True)[0][len(prompt)-1:]
  return response




# Create a Flask application instance
app = Flask(__name__)
run_with_lt(app)

model, tokenizer = load_model()
# Define a route and a function to handle requests to that route
@app.route('/')
def hello_world():
    return render_template('chat.html')

@app.route('/get', methods=['GET','POST'])
def chat():
     user_input = request.form["msg"]
     return llm_response(user_input,model,tokenizer)
# Run the Flask application
if __name__ == '__main__':
    app.run(debug=True)
