import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Strong Scaling Data
strong_scaling_data = [
    (1, 16, 48.1207), (1, 8, 34.603), (1, 4, 30.3673), (1, 1, 60.9741),
    (2, 16, 44.3427), (2, 8, 45.4997), (2, 4, 53.7693), (2, 1, 73.5191),
    (4, 16, 43.873), (4, 8, 44.8853), (4, 4, 49.3309), (4, 1, 62.0369),
    (8, 16, 45.4888), (8, 8, 47.3194), (8, 4, 54.2495), (8, 1, 59.1323)
]
df_strong = pd.DataFrame(strong_scaling_data, columns=["MPI_Procs", "Threads", "Time"])
df_strong["Total Cores"] = df_strong["MPI_Procs"] * df_strong["Threads"]
df_strong["Speedup"] = df_strong["Time"].iloc[0] / df_strong["Time"]

# Weak Scaling Data
weak_scaling_raw = [
    # data_10M_p64
    (1, 16, 6.77645), (1, 8, 11.4693), (1, 4, 18.0654), (1, 1, 27.4718),
    (2, 16, 15.0769), (2, 8, 14.1326), (2, 4, 16.939), (2, 1, 21.9136),
    (4, 16, 12.4804), (4, 8, 10.5174), (4, 4, 11.9663), (4, 1, 12.9847),
    (8, 16, 12.7928), (8, 8, 10.4392), (8, 4, 13.4428), (8, 1, 93.0982),
    # data_10M_p512
    (1, 16, 64.5114), (1, 8, 26.4986), (1, 4, 33.3299), (1, 1, 39.6908),
    (2, 16, 47.855), (2, 4, 161.885), (2, 1, 89.7169),
    (4, 4, 160.445), (4, 1, 101.225),
    (8, 16, 21.7144), (8, 8, 131.573),
    # data_50M_p64
    (1, 16, 48.1207), (1, 8, 34.603), (1, 4, 30.3673), (1, 1, 60.9741),
    (2, 16, 44.3427), (2, 8, 45.4997), (2, 4, 53.7693), (2, 1, 73.5191),
    (4, 16, 43.873), (4, 8, 44.8853), (4, 4, 49.3309), (4, 1, 62.0369),
    (8, 16, 45.4888), (8, 8, 47.3194), (8, 4, 54.2495), (8, 1, 59.1323),
    # data_50M_p512
    (1, 16, 320.058), (1, 8, 202.723), (1, 4, 164.15), (1, 1, 295.126),
    (2, 16, 371.864), (2, 8, 315.947), (2, 4, 256.207), (2, 1, 214.159),
    (4, 16, 534.248)
]
df_weak = pd.DataFrame(weak_scaling_raw, columns=["MPI_Procs", "Threads", "Time"])
df_weak["Total Cores"] = df_weak["MPI_Procs"] * df_weak["Threads"]

# Plot Strong Scaling
plt.figure(figsize=(10, 6))
sns.lineplot(data=df_strong, x="Total Cores", y="Speedup", hue="Threads", marker="o", palette="tab10")
plt.title("Strong Scaling (Speedup vs. Total Cores)")
plt.xlabel("Total Cores (MPI Procs × Threads)")
plt.ylabel("Speedup")
plt.grid(True)
plt.tight_layout()
plt.savefig("strong_scaling_speedup.png")
plt.clf()

# Plot Weak Scaling
plt.figure(figsize=(10, 6))
sns.lineplot(data=df_weak, x="Total Cores", y="Time", hue="Threads", marker="o", palette="tab10")
plt.title("Weak Scaling (Execution Time vs. Total Cores)")
plt.xlabel("Total Cores (MPI Procs × Threads)")
plt.ylabel("Execution Time (s)")
plt.grid(True)
plt.tight_layout()
plt.savefig("weak_scaling_time.png")
plt.clf()
