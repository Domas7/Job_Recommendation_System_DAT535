from pyspark import SparkContext, SparkConf
import re


def create_cleaning_pipeline(sc, input_file):
    """
    Enhanced cleaning pipeline with better preprocessing
    """
    
    sc.setLogLevel("WARN")
    
    
    # Read the raw text file
    raw_data = sc.textFile(input_file, minPartitions=100)


    # Step 1: MAP - Group lines into job postings
    def map_to_job_postings(lines):
        current_posting = []
        for line in lines:
            if "<<<START_JOB_POSTING>>>" in line:
                if current_posting:
                    yield "\n".join(current_posting)
                current_posting = []
            elif "<<<END_JOB_POSTING>>>" in line:
                if current_posting:
                    yield "\n".join(current_posting)
                current_posting = []
            else:
                current_posting.append(line)
    
    job_postings = raw_data.mapPartitions(map_to_job_postings)
    
    
    # Step 2: MAP - Clean text and extract fields
    def clean_text(text):
        if not text:
            return ""
        text = text.lower()
        text = re.sub(r'[^\w\s]', ' ', text)
        replacements = {
            'yrs': 'years',
            'yr': 'year',
            'exp': 'experience',
            'min': 'minimum',
            'max': 'maximum'
        }
        for old, new in replacements.items():
            text = text.replace(old, new)
        return ' '.join(text.split())
    
    
    def extract_fields(posting):
        try:
            # Extract all required fields
            title_match = re.search(r'\n(.*?) at (.*?)\nPosted', posting)
            job_title = clean_text(title_match.group(1)) if title_match else None
            
            
            # Extract description (everything between ABOUT THE ROLE and REQUIREMENTS)
            desc_match = re.search(r'ABOUT THE ROLE\n-+\n(.*?)\n\nREQUIREMENTS', posting, re.DOTALL)
            job_description = clean_text(desc_match.group(1)) if desc_match else None
            
            
            # Extract skills
            skills_match = re.search(r'SKILLS NEEDED\n-+\n(.*?)\n\n', posting, re.DOTALL)
            job_skills = clean_text(skills_match.group(1)) if skills_match else None
            
            
            # Extract industry
            industry_match = re.search(r'"Industry":"([^"]+)"', posting)
            job_industry = clean_text(industry_match.group(1)) if industry_match else None
            
            
            # Extract location
            location_match = re.search(r'Location: (.*?)\n', posting)
            job_location = clean_text(location_match.group(1)) if location_match else None
            
            # Extract experience
            
            exp_match = re.search(r'Experience: (.*?)\n', posting)
            experience_level = clean_text(exp_match.group(1)) if exp_match else None
            
            
            # Extract salary range
            salary_match = re.search(r'Salary: (.*?)\n', posting)
            salary_str = salary_match.group(1) if salary_match else ""
            nums = re.findall(r'\d+', salary_str)
            min_salary = int(nums[0]) * 1000 if len(nums) >= 2 else None
            max_salary = int(nums[1]) * 1000 if len(nums) >= 2 else None
            
            
            # Create tuple with all fields
            result = (
                job_title,
                job_description,
                job_skills,
                job_industry,
                job_location,
                experience_level,
                min_salary,
                max_salary
            )
            
            # Check if any required fields are missing
            if any(x is None for x in result):
                return None
                
            return result
            
        except Exception as e:
            return None
    
    
    # Extract and clean fields
    cleaned_postings = job_postings.map(extract_fields).filter(lambda x: x is not None)
    
    
    # Remove duplicates based on all fields
    unique_postings = cleaned_postings.distinct()
    
    
    # Calculate statistics
    total_posts = unique_postings.count()
    posts_with_missing = cleaned_postings.filter(lambda x: None in x).count()
    
    
    # Calculate salary statistics
    salary_stats = unique_postings.map(
        lambda x: (x[6], x[7])  # min and max salary
    ).filter(
        lambda x: x[0] is not None and x[1] is not None
    ).mapValues(
        lambda x: (x, 1)
    ).reduceByKey(
        lambda x, y: (x[0] + y[0], x[1] + y[1])
    ).mapValues(
        lambda x: x[0] / x[1]  # average salary
    )
    
    
    print(f"\nProcessing Summary:")
    print(f"Total valid job postings: {total_posts}")
    print(f"Posts with missing values: {posts_with_missing}")
    
    
    return unique_postings.collect(), salary_stats.collect()


# Create a new Spark context
conf = SparkConf().setAppName("JobDataCleaning") \
    .set("spark.executor.memory", "4g") \
    .set("spark.driver.memory", "2g") \
    .set("spark.default.parallelism", "100") \
    .set("spark.network.timeout", "800s") \
    .set("spark.executor.heartbeatInterval", "60s")  # Add heartbeat interval
sc = SparkContext(conf=conf)


# Process data
input_file = "hdfs:///user/ubuntu/job_descriptions_unstructured.txt"
final_data, stats = create_cleaning_pipeline(sc, input_file)


# Define columns for the CSV header
columns = [
    "job_title",
    "job_description",
    "job_skills",
    "job_industry",
    "job_location",
    "experience_level",
    "min_salary",
    "max_salary"
]


# Save the cleaned data to CSV
def row_to_csv(row):
    # Convert each field to string and handle None values
    csv_fields = [str(field) if field is not None else '' for field in row]
    # Join fields with commas and properly escape any commas within fields
    return ','.join(f'"{field}"' for field in csv_fields)


# Create header
header = ','.join(columns)


# Convert data to CSV format and save
output_path = "cleaned_job_data.csv"
csv_rdd = sc.parallelize([header]) \
    .union(sc.parallelize(final_data).map(row_to_csv))
csv_rdd.coalesce(1).saveAsTextFile(output_path)


# Save statistics to a separate CSV
stats_header = "company,avg_salary"
stats_csv_rdd = sc.parallelize([stats_header]) \
    .union(sc.parallelize(stats).map(lambda x: f"{x[0]},{x[1]}"))
stats_csv_rdd.coalesce(1).saveAsTextFile("salary_stats.csv")


print(f"\nResults saved to:")
print(f"- Job data: {output_path}")
print(f"- Salary statistics: salary_stats.csv")
