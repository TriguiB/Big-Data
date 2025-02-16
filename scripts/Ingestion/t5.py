import os
import re
import logging
from datetime import datetime
from bs4 import BeautifulSoup
from langdetect import detect

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col

# ✅ Setup Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# 📁 File paths and model configuration
RAW_DATA_PATH = "data/raw/github_repos_with_issues.json"
OUTPUT_PATH = "data/processed/processed_github_issues_spark.csv"
MODEL_NAME = "t5-small"  # Using a lighter model for performance

###############################################################################
# Function: process_partition
###############################################################################
def process_partition(iterator):
    """
    Process a partition of issue rows. Loads the T5 model and tokenizer once per partition,
    then cleans, summarizes, and generates insights for each GitHub issue.
    """
    import torch
    from transformers import T5ForConditionalGeneration, T5Tokenizer

    # Set device and load model/tokenizer on this executor
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model = T5ForConditionalGeneration.from_pretrained(MODEL_NAME).to(device)
    tokenizer = T5Tokenizer.from_pretrained(MODEL_NAME)

    # Local helper function: clean_text
    def local_clean_text(text):
        if not text:
            return ""
        # Remove HTML tags, URLs, markdown headers, code blocks, import statements, etc.
        text = BeautifulSoup(text, "html.parser").get_text().strip()
        text = re.sub(r"https?://\S+", "", text)
        text = re.sub(r"#+\s*", "", text)
        text = re.sub(r"```.*?```", "", text, flags=re.DOTALL)
        text = re.sub(r"\bimport .*|from .* import .*", "", text)
        text = re.sub(r"\s+", " ", text).strip()
        text = re.sub(r"(No response|awaiting triage|What would you like to share\?)", "", text, flags=re.IGNORECASE)
        try:
            if detect(text) != "en":
                return "Non-English Issue"
        except Exception:
            return "Non-English Issue"
        return text if len(text.split()) > 5 else "No relevant details."

    # Local helper function: summarize_issue
    def local_summarize_issue(title, body, labels="N/A", status="open"):
        prompt = f"summarize: {title}. {body} Labels: {labels}. Status: {status}"
        inputs = tokenizer(prompt, return_tensors="pt", max_length=512, truncation=True).to(device)
        summary_ids = model.generate(inputs.input_ids, max_length=50, min_length=15,
                                       length_penalty=1.2, num_beams=8)
        return tokenizer.decode(summary_ids[0], skip_special_tokens=True).strip()

    # Local helper function: generate_insights
    def local_generate_insights(title, body, labels="N/A"):
        prompt = f"analyze: {title}. {body} Labels: {labels}. Provide key challenges and possible solutions."
        inputs = tokenizer(prompt, return_tensors="pt", max_length=512, truncation=True).to(device)
        response_ids = model.generate(inputs.input_ids, max_length=100, min_length=20, num_beams=5)
        return tokenizer.decode(response_ids[0], skip_special_tokens=True).strip()

    # Process each row in the partition
    for row in iterator:
        # row is a dictionary with keys from the exploded DataFrame
        repo_name = row.get("Repository")
        # Skip issues that are not closed or missing creation date
        if row.get("state") != "closed" or not row.get("created_at"):
            continue
        created_at_str = row.get("created_at")
        closed_at_str = row.get("closed_at") or created_at_str
        try:
            created_at = datetime.strptime(created_at_str, "%Y-%m-%dT%H:%M:%SZ")
            closed_at = datetime.strptime(closed_at_str, "%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            continue
        time_spent = round((closed_at - created_at).total_seconds() / 3600, 2)
        title_clean = local_clean_text(row.get("title", ""))
        body_clean = local_clean_text(row.get("body", ""))
        labels_list = row.get("labels", ["N/A"])
        labels_val = ", ".join(labels_list)
        # The original JSON does not contain a 'user' field; default contributor to "Anonymous"
        contributor = "Anonymous"
        
        if body_clean != "Non-English Issue" and len(body_clean.split()) > 5:
            description = local_summarize_issue(title_clean, body_clean, labels_val, "closed")
            insights = local_generate_insights(title_clean, body_clean, labels_val)
        else:
            description = "No relevant details or non-English issue."
            insights = "No additional insights available."
        
        yield {
            "Repository": repo_name,
            "Time Spent (hours)": time_spent,
            "Issue Type": labels_val,
            "Contributor": contributor,
            "Description": description,
            "Insights": insights
        }

###############################################################################
# Function: main_spark
###############################################################################
def main_spark():
    # Initialize SparkSession
    spark = SparkSession.builder.appName("GitHubIssuesProcessing").getOrCreate()
    
    # Read the raw JSON file (assumes a multiLine JSON array)
    df = spark.read.json(RAW_DATA_PATH, multiLine=True)
    
    # Explode the "closed_issues" array so that each issue becomes its own row
    df = df.withColumn("issue", explode(col("closed_issues")))
    
    # Select and rename columns from the nested "issue" structure
    df = df.select(
        col("full_name").alias("Repository"),
        col("issue.state").alias("state"),
        col("issue.created_at").alias("created_at"),
        col("issue.closed_at").alias("closed_at"),
        col("issue.title").alias("title"),
        col("issue.body").alias("body"),
        col("issue.labels").alias("labels")
        # Note: 'user' field is not present; we'll default contributor to "Anonymous"
    )
    
    # Filter for closed issues with a valid creation date
    df = df.filter((col("state") == "closed") & (col("created_at").isNotNull()))
    
    # Convert the DataFrame to an RDD of dictionaries
    issues_rdd = df.rdd.map(lambda row: row.asDict())
    
    # Process each partition using our custom function that loads the model once per partition
    processed_rdd = issues_rdd.mapPartitions(process_partition)
    
    # Convert the processed RDD back to a DataFrame
    processed_df = spark.createDataFrame(processed_rdd)
    
    # Write the results to CSV (Spark will write multiple part files into the output folder)
    processed_df.write.option("header", True).csv(OUTPUT_PATH)
    
    logging.info(f"✅ Successfully processed and saved issues to {OUTPUT_PATH}")
    spark.stop()

###############################################################################
# Entry Point
###############################################################################
if __name__ == "__main__":
    main_spark()
