from collections import Counter

# Threshold for filtering
SIZE_THRESHOLD = 10000

# File paths
input_path = "output/index.txt"
output_path = "output/filtered_labels.txt"

label_counter = Counter()

with open(input_path, "r", encoding="utf-8") as f:
    for line in f:
        parts = dict(part.strip().split(": ", 1) for part in line.strip().split(", "))
        size = int(parts.get("size", 0))
        
        if size > SIZE_THRESHOLD:
            label = parts.get("label")
            if label:
                label_counter[label] += 1

# Write to output file
sorted_labels = sorted(label_counter.items(), key=lambda x: x[1], reverse=True)
with open(output_path, "w", encoding="utf-8") as out:
    out.write(f"Labels with size > {SIZE_THRESHOLD}\n")
    for label, count in sorted_labels:
        out.write(f"{label}: {count}\n")

print(f"Results written to {output_path}")
