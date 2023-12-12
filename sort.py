from collections import defaultdict

# Path to the local output file
local_output_path = 'year'

# Read the local output file
with open(local_output_path, 'r') as file:
    # Use defaultdict to count frequencies
    frequency_dict = defaultdict(int)

    for line in file:
        # Split the line into word and frequency
        word, frequency = line.strip().split('\t')
        
        # Accumulate the frequency
        frequency_dict[word] += int(frequency)

# Sort the dictionary by frequency in descending order
sorted_frequency = sorted(frequency_dict.items(), key=lambda x: x[1], reverse=True)

# Print the sorted results
for word, frequency in sorted_frequency:
    print(f"{word}: {frequency}")
