from pyspark import SparkContext, SparkConf
import re
import json
from collections import defaultdict
import string
import pandas as pd
import os
import time


def create_cleaning_pipeline(sc, input_file, output_file):
    """
    Enhanced cleaning pipeline with additional preprocessing steps,
    error handling, and performance tuning
    """
    # Configuration for tuning
    sc.setLogLevel("WARN")  # Reduce log verbosity

    # Initialize counters for logging
    missing_values_count = defaultdict(int)
    duplicates_count = 0
    
    # Read the raw text file with optimized partition size
    raw_data = sc.textFile(input_file, minPartitions=100)

    # Step 1: MAP - Group lines into job postings with error handling
    def map_to_job_postings(lines):
        try:
            current_posting = []
            for line in lines:
                if "<<<START_JOB_POSTING>>>" in line:
                    if current_posting:  # Handle missing END marker
                        yield "\n".join(current_posting)
                    current_posting = []
                elif "<<<END_JOB_POSTING>>>" in line:
                    if current_posting:
                        yield "\n".join(current_posting)
                    current_posting = []
                else:
                    current_posting.append(line)
        except Exception as e:
            yield f"ERROR: {str(e)}"
    
    job_postings = raw_data.mapPartitions(map_to_job_postings)
    
    # Step 2: MAP - Extract and clean fields
    def clean_text(text):
        if not text:
            return ""
        # Convert to lowercase
        text = text.lower()
        # Remove punctuation and non-alphanumeric characters
        text = re.sub(r'[^\w\s]', ' ', text)
        # Replace common words/patterns
        replacements = {
            'yrs': 'years',
            'yr': 'year',
            'exp': 'experience',
            'min': 'minimum',
            'max': 'maximum'
        }
        for old, new in replacements.items():
            text = text.replace(old, new)
        return ' '.join(text.split())  # Normalize whitespace
    
    def extract_and_clean_fields(posting):
        try:
            fields = defaultdict(lambda: None)  # Handle missing values
            
            # Extract fields with error handling
            id_match = re.search(r'JOB POSTING ID: (\d+)', posting)
            fields['job_id'] = id_match.group(1) if id_match else None
            if fields['job_id'] is None:
                missing_values_count['job_id'] += 1
            
            title_match = re.search(r'\n(.*?) at (.*?)\nPosted', posting)
            if title_match:
                fields['job_title'] = clean_text(title_match.group(1))
                fields['company'] = clean_text(title_match.group(2))
            else:
                missing_values_count['job_title'] += 1
                missing_values_count['company'] += 1
            
            # Extract and clean salary information
            salary_match = re.search(r'Salary: (.*?)\n', posting)
            if salary_match:
                salary_str = salary_match.group(1)
                nums = re.findall(r'\d+', salary_str)
                if len(nums) >= 2:
                    fields['min_salary'] = int(nums[0]) * 1000
                    fields['max_salary'] = int(nums[1]) * 1000
                else:
                    missing_values_count['salary'] += 1
            else:
                missing_values_count['salary'] += 1
            
            return fields
        except Exception as e:
            return {'error': str(e), 'raw_data': posting}
    
    cleaned_postings = job_postings.map(extract_and_clean_fields)
    
    # Step 3: REDUCE - Remove duplicates and aggregate
    # Key by job_id and company to identify duplicates
    def create_key(posting):
        return (posting.get('job_id'), posting.get('company'))
    
    # Keep only the first occurrence of each duplicate
    unique_postings = (cleaned_postings
        .map(lambda p: (create_key(p), p))
        .reduceByKey(lambda x, y: x)  # This is a MapReduce operation
        .map(lambda x: x[1]))

    # Count the number of duplicates removed
    original_count = cleaned_postings.count()
    unique_count = unique_postings.count()
    duplicates_count = original_count - unique_count
    
    # Step 4: REDUCE - Calculate statistics and sort by salary
    def calculate_stats(postings_iter):
        stats = defaultdict(int)
        postings_list = list(postings_iter)
        stats['total_jobs'] = len(postings_list)
        stats['avg_min_salary'] = sum(p.get('min_salary', 0) for p in postings_list) / len(postings_list)
        return stats
    
    company_stats = (unique_postings
        .map(lambda p: (p.get('company', 'Unknown'), p))
        .groupByKey()
        .mapValues(calculate_stats))  # This is another MapReduce operation
    
    # Convert to DataFrame format (still using RDD operations)
    final_data = unique_postings.collect()
    
    # Create pandas DataFrame
    df = pd.DataFrame(final_data)
    
    # Sort by salary
    df = df.sort_values(by=['min_salary', 'max_salary'], ascending=[False, False])
    
    # Save to CSV (creates the file if it doesn't exist)
    df.to_csv(output_file, index=False)
    
    # Log the findings
    print(f"\nProcessing Summary:")
    print(f"Total job postings processed: {original_count}")
    print(f"Duplicates removed: {duplicates_count}")
    print(f"Missing values found for 'job_id': {missing_values_count['job_id']}")
    print(f"Missing values found for 'job_title': {missing_values_count['job_title']}")
    print(f"Missing values found for 'company': {missing_values_count['company']}")
    print(f"Missing values found for 'salary': {missing_values_count['salary']}")
    
    return df, company_stats

def load_processed_data(file_path):
    """
    Load the processed DataFrame from saved file
    """
    return pd.read_csv(file_path)

# Create Spark context with optimization configurations
conf = SparkConf().setAppName("JobDataCleaning") \
    .set("spark.executor.memory", "4g") \
    .set("spark.driver.memory", "2g") \
    .set("spark.default.parallelism", "100")
sc = SparkContext(conf=conf)

# Input and output files #sf_fire_file = "file:///home/ubuntu/dis_materials/dis_materials/sf-fire-calls-1.csv"
input_file = "hdfs:///user/ubuntu/job_descriptions_unstructured.txt"
output_file = "/home/ubuntu/processed_job_descriptions.csv"

# Process data and measure time
start_time = time.time()
df, stats = create_cleaning_pipeline(sc, input_file, output_file)
print(f"Processing time: {time.time() - start_time} seconds")

# Show some basic information
print("\nDataFrame Info:")
print(df.info())

print("\nMissing Values:")
print(df.isnull().sum())

# Later, load the processed data:
loaded_df = load_processed_data(output_file)
