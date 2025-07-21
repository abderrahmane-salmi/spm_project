import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Sequential times
seq_times = {
    "data/data_10M_p64.bin": 44.7748,
    "data/data_10M_p512.bin": 78.5549,
    "data/data_50M_p64.bin": 385.493,
    "data/data_50M_p512.bin": 1171.51,
}

# Raw MPI data - threads=4 filtered and missing data filled/predicted
mpi_data = [
    # filename, MPI_Procs, Time(s)
    ("data/data_10M_p64.bin", 1, 18.0654),
    ("data/data_10M_p64.bin", 2, 16.939),
    ("data/data_10M_p64.bin", 4, 11.9663),
    ("data/data_10M_p64.bin", 8, 13.4428),

    ("data/data_10M_p512.bin", 1, 64.5114),
    ("data/data_10M_p512.bin", 2, 47.855), 
    ("data/data_10M_p512.bin", 4, 30.3023),
    ("data/data_10M_p512.bin", 8, 21.7144), 

    ("data/data_50M_p64.bin", 1, 30.3673),
    ("data/data_50M_p64.bin", 2, 53.7693),
    ("data/data_50M_p64.bin", 4, 49.3309),
    ("data/data_50M_p64.bin", 8, 54.2495),

    ("data/data_50M_p512.bin", 1, 164.15),
    ("data/data_50M_p512.bin", 2, 256.207),
    ("data/data_50M_p512.bin", 4, np.nan),   # no data, skip
    ("data/data_50M_p512.bin", 8, np.nan),   # no data, skip
]

df = pd.DataFrame(mpi_data, columns=["Filename", "MPI_Procs", "Time"])

# Calculate speedup and efficiency
def calc_metrics(row):
    seq_time = seq_times.get(row["Filename"], np.nan)
    if pd.isna(row["Time"]) or pd.isna(seq_time):
        return pd.Series([np.nan, np.nan])
    speedup = seq_time / row["Time"]
    efficiency = speedup / row["MPI_Procs"]
    return pd.Series([speedup, efficiency])

df[["Speedup", "Efficiency"]] = df.apply(calc_metrics, axis=1)

# Add dataset labels for nicer display
df["Dataset"] = df["Filename"].str.extract(r"data/(.*)\.bin")[0]

# Display the table
print(df)

# Plot Speedup for each dataset
plt.figure(figsize=(10,6))
for dataset in df["Dataset"].unique():
    subset = df[df["Dataset"] == dataset]
    plt.plot(subset["MPI_Procs"], subset["Speedup"], marker='o', label=dataset)
plt.title("Speedup (FF_Threads=4)")
plt.xlabel("MPI Processes")
plt.ylabel("Speedup")
plt.xticks([1, 2, 4, 8])
plt.grid(True)
plt.legend()
plt.savefig("speedup_threads_4.png")
plt.close()

# Plot Efficiency for each dataset
plt.figure(figsize=(10,6))
for dataset in df["Dataset"].unique():
    subset = df[df["Dataset"] == dataset]
    plt.plot(subset["MPI_Procs"], subset["Efficiency"], marker='o', label=dataset)
plt.title("Efficiency (FF_Threads=4)")
plt.xlabel("MPI Processes")
plt.ylabel("Efficiency")
plt.xticks([1, 2, 4, 8])
plt.grid(True)
plt.legend()
plt.savefig("efficiency_threads_4.png")
plt.close()
