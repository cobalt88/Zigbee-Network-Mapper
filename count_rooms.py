import re
import json

# The dataset as a multiline string
data = """
"""

# Extract all unique 4-digit numbers from the dataset
unique_numbers = set(re.findall(r'\d{4}', data))
# pretty print the unique number list
print(unique_numbers)

# Count of unique 4-digit numbers
unique_count = len(unique_numbers)

# Print the count of unique 4-digit numbers
print(unique_count)