import pandas as pd
import matplotlib.pyplot as plt

# --- Base sequential time ---
sequential_time = 52.2425 

# --- Raw data ---
omp_data = pd.DataFrame([
    (2, 37.5752),
    (4, 32.4526),
    (8, 26.1782),
    (16, 24.0571),
    (32, 20.2746),
    (64, 20.4024)
], columns=["Threads", "Time"])

ff_data = pd.DataFrame([
    (2, 30.681),
    (4, 18.4251),
    (8, 17.5604),
    (16, 16.7573),
    (32, 20.5539),
    (64, 20.3824)
], columns=["Workers", "Time"])

mpi_data = pd.DataFrame([
    (2, 26.9136),
    (4, 13.9847),
    (8, 11.9277)
], columns=["MPI_Procs", "Time"])

# --- Compute speedup and efficiency ---
def compute_metrics(df, thread_col):
    df["Speedup"] = sequential_time / df["Time"]
    df["Efficiency"] = df["Speedup"] / df[thread_col]
    return df

omp_data = compute_metrics(omp_data, "Threads")
ff_data = compute_metrics(ff_data, "Workers")
mpi_data = compute_metrics(mpi_data, "MPI_Procs")

# --- Print tables ---
print("\n[OpenMP]")
print(omp_data.to_string(index=False, formatters={"Efficiency": "{:.2f}".format}))
print("\n[FastFlow]")
print(ff_data.to_string(index=False, formatters={"Efficiency": "{:.2f}".format}))
print("\n[MPI + FF]")
print(mpi_data.to_string(index=False, formatters={"Efficiency": "{:.2f}".format}))

# --- Plot function ---
def plot_metrics(df, x_col, label, filename):
    plt.figure(figsize=(10, 6))
    plt.plot(df[x_col], df["Speedup"], 'o-', label="Speedup")
    plt.plot(df[x_col], df["Efficiency"], 's--', label="Efficiency")
    plt.xlabel(x_col)
    plt.ylabel("Value")
    plt.title(f"{label} Performance")
    plt.legend()
    plt.grid(True)
    plt.savefig(filename)
    plt.close()

# --- Generate plots ---
plot_metrics(omp_data, "Threads", "OpenMP", "omp_performance.png")
plot_metrics(ff_data, "Workers", "FastFlow", "ff_performance.png")
plot_metrics(mpi_data, "MPI_Procs", "MPI + FF", "mpi_performance.png")

print("\nPlots saved as: omp_performance.png, ff_performance.png, mpi_performance.png")
