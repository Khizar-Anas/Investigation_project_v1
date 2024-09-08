import random

def generate_unique_victim_ids(num_victims, start_range, end_range):
    # Ensure that the range is large enough to generate unique numbers
    if end_range - start_range + 1 < num_victims:
        raise ValueError("Range is too small to generate the required number of unique IDs.")
    
    # Generate unique victim IDs
    unique_victim_ids = random.sample(range(start_range, end_range), num_victims)
    
    return unique_victim_ids

# Example usage:
num_victims = 307
start_range = 1000
end_range = 5000

victim_ids = generate_unique_victim_ids(num_victims, start_range, end_range)

# Printing the generated unique IDs
for variable in victim_ids:
    print(variable)
