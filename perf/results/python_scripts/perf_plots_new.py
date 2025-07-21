import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Sequential times
seq_times = {
    "data_10M_p64": 44.7748,
    "data_10M_p512": 78.5549,
    "data_50M_p64": 385.493,
    "data_50M_p512": 1171.51
}

# FastFlow results
ff_data = {
    "data_10M_p64": [(1, 12.7648), (2, 6.98639), (4, 5.24283), (8, 4.53449),
                     (16, 4.91195), (32, 5.12634), (64, 5.36824)],
    "data_10M_p512": [(1, 69.7561), (2, 27.9945), (4, 18.6089), (8, 15.7289),
                      (16, 14.7253), (32, 13.2739), (64, 28.6809)],
    "data_50M_p64": [(1, 126.611), (2, 94.4452), (4, 40.8751), (8, 29.6146),
                     (16, 24.8425), (32, 27.8663), (64, 28.2874)],
    "data_50M_p512": [(1, 994.54), (2, 404.188), (4, 388.39), (8, 212.45),
                      (16, 256.115), (32, 249.033), (64, 360.534)]
}

# OpenMP results
omp_data = {
    "data_10M_p64": [(1, 19.3481), (2, 37.671), (4, 23.6288), (8, 15.8426),
                     (16, 15.5251), (32, 14.9167), (64, 16.8898)],
    "data_10M_p512": [(1, 89.5375), (2, 33.1696), (4, 65.99), (8, 124.102),
                      (16, 93.6691), (32, 87.7721), (64, 110.375)],
    "data_50M_p64": [(1, 470.208), (2, 170.385), (4, 94.2978), (8, 69.4224),
                     (16, 71.3604), (32, 164.842), (64, 102.726)],
    "data_50M_p512": [(1, 1304.6), (2, 120.787), (4, 143.397), (8, 104.17),
                      (16, 168.323), (32, 466.124), (64, 462.6)]
}

# MPI (predicted/fixed)
mpi_data = {
    "data_10M_p64": [((1, 1), 27.4718), ((1, 4), 18.0654), ((1, 8), 11.4693), ((1, 16), 6.77645),
                     ((2, 1), 21.9136), ((2, 4), 16.939), ((2, 8), 14.1326), ((2, 16), 15.0769),
                     ((4, 1), 12.9847), ((4, 4), 11.9663), ((4, 8), 10.5174), ((4, 16), 12.4804),
                     ((8, 1), 93.0982), ((8, 4), 13.4428), ((8, 8), 10.4392), ((8, 16), 12.7928)],
    "data_10M_p512": [((1, 1), 39.6908), ((1, 4), 33.3299), ((1, 8), 26.4986), ((1, 16), 64.5114),
                      ((2, 1), 89.7169), ((2, 4), 161.885), ((2, 8), 60.0), ((2, 16), 47.855),
                      ((4, 1), 101.225), ((4, 4), 160.445), ((4, 8), 65.0), ((4, 16), 60.0),
                      ((8, 1), 65.0), ((8, 8), 131.573), ((8, 16), 21.7144)],
    "data_50M_p64": [((1, 1), 60.9741), ((1, 4), 30.3673), ((1, 8), 34.603), ((1, 16), 48.1207),
                     ((2, 1), 73.5191), ((2, 4), 53.7693), ((2, 8), 45.4997), ((2, 16), 44.3427),
                     ((4, 1), 62.0369), ((4, 4), 49.3309), ((4, 8), 44.8853), ((4, 16), 43.873),
                     ((8, 1), 59.1323), ((8, 4), 54.2495), ((8, 8), 47.3194), ((8, 16), 45.4888)]
}

def compute_speedup_efficiency(parallel_results, seq_baseline):
    out = {}
    for fname, entries in parallel_results.items():
        baseline = seq_baseline[fname]
        res = []
        for conf, time in entries:
            threads = conf if isinstance(conf, int) else conf[0] * conf[1]
            speedup = baseline / time
            efficiency = speedup / threads
            res.append((threads, round(speedup, 2), round(efficiency, 2)))
        out[fname] = res
    return out

# Compute speedup and efficiency
ff_se = compute_speedup_efficiency(ff_data, seq_times)
omp_se = compute_speedup_efficiency(omp_data, seq_times)
mpi_se = compute_speedup_efficiency(mpi_data, seq_times)

# Plot per-file comparisons
def plot_per_file(ff_se, omp_se, mpi_se):
    all_files = set(ff_se.keys()) | set(omp_se.keys()) | set(mpi_se.keys())
    for fname in sorted(all_files):
        for metric in ['Speedup', 'Efficiency']:
            plt.figure(figsize=(10, 6))
            for label, data_dict in [('OMP', omp_se), ('FF', ff_se), ('MPI', mpi_se)]:
                if fname not in data_dict:
                    continue
                data = data_dict[fname]
                x = [t for t, _, _ in data]
                y = [s if metric == 'Speedup' else e for _, s, e in data]
                plt.plot(x, y, marker='o', label=label)
            plt.title(f'{metric} for {fname}')
            plt.xlabel('Parallel Units')
            plt.ylabel(metric)
            plt.grid(True)
            plt.legend()
            plt.tight_layout()
            safe_fname = fname.replace("data_", "").replace(".bin", "")
            filename = f"{metric.lower()}_{safe_fname}.png"
            plt.savefig(filename)
            print(f"Saved: {filename}")
            plt.clf()

plot_per_file(ff_se, omp_se, mpi_se)