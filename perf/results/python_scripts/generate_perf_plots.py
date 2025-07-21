import matplotlib.pyplot as plt

datasets = {
    "10M_64B": {
        "filename": "data_10M_p64.bin",
        "ff_times": [27.39, 24.52, 17.68, 16.82, 17.42],
        "omp_times": [29.04, 24.56, 20.81, 19.43, 18.58],
    },
    "10M_512B": {
        "filename": "data_10M_p512.bin",
        "ff_times": [55.72, 37.09, 31.74, 28.70, 30.42],
        "omp_times": [47.89, 39.12, 33.27, 29.23, 31.01],
    },
    "50M_64B": {
        "filename": "data_50M_p64.bin",
        "ff_times": [133.96, 105.32, 89.25, 88.63, 95.40],
        "omp_times": [148.21, 124.93, 103.47, 97.33, 99.89],
    },
    "50M_512B": {
        "filename": "data_50M_p512.bin",
        "ff_times": [347.01, 306.78, 248.09, 220.37, 246.67],
        "omp_times": [544.01, 409.98, 342.87, 311.94, 319.47],
    }
}

threads = [1, 2, 4, 8, 16]

for key, data in datasets.items():
    ff_base = data["ff_times"][0]
    omp_base = data["omp_times"][0]

    ff_speedup = [ff_base / t for t in data["ff_times"]]
    omp_speedup = [omp_base / t for t in data["omp_times"]]

    ff_eff = [s / p for s, p in zip(ff_speedup, threads)]
    omp_eff = [s / p for s, p in zip(omp_speedup, threads)]

    # ---- Plot Speedup ----
    plt.figure()
    plt.plot(threads, ff_speedup, marker='o', label='FastFlow')
    plt.plot(threads, omp_speedup, marker='s', label='OpenMP')
    plt.xlabel("Threads / Workers")
    plt.ylabel("Speedup")
    plt.title(f"Speedup - {key}")
    plt.grid(True)
    plt.legend()
    plt.savefig(f"plots/speedup_{key}.png")

    # ---- Plot Efficiency ----
    plt.figure()
    plt.plot(threads, ff_eff, marker='o', label='FastFlow')
    plt.plot(threads, omp_eff, marker='s', label='OpenMP')
    plt.xlabel("Threads / Workers")
    plt.ylabel("Efficiency")
    plt.title(f"Efficiency - {key}")
    plt.grid(True)
    plt.legend()
    plt.savefig(f"plots/efficiency_{key}.png")

print("✅ Plots saved to report/images/")
