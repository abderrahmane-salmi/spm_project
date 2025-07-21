import matplotlib.pyplot as plt

procs = [1, 2, 4, 8]
times_64 = [172.98, 64.86, 33.12, 16.13]
times_512 = [534.87, 290.00, 150.00, 80.00]

speedup_64 = [times_64[0]/t for t in times_64]
speedup_512 = [times_512[0]/t for t in times_512]

plt.plot(procs, speedup_64, label='50M, 64B payload', marker='o')
plt.plot(procs, speedup_512, label='50M, 512B payload', marker='s')
plt.plot(procs, procs, label='Ideal Linear Speedup', linestyle='--', color='gray')

plt.xlabel('Number of MPI Processes')
plt.ylabel('Speedup')
plt.title('Strong Scalability - MPI+OMP')
plt.legend()
plt.grid(True)
plt.savefig("strong_scalability.png")
plt.show()

### Code to Plot Weak Scalability

# Data from your measurements (data_10M_p64.bin)
mpi_procs = [1, 2, 4, 8]
execution_time_p64 = [44.1319, 23.9762, 11.4849, 6.2443]  # p64 payload

# Optional: Simulate ideal weak scaling (constant time)
ideal_time = [execution_time_p64[0]] * len(mpi_procs)

# Plot
plt.figure(figsize=(8, 5))
plt.plot(mpi_procs, execution_time_p64, marker='o', label='Measured (64B payload)')
plt.plot(mpi_procs, ideal_time, linestyle='--', color='gray', label='Ideal (constant time)')

plt.xlabel("Number of MPI Processes")
plt.ylabel("Execution Time (s)")
plt.title("Weak Scalability - data_10M_p64.bin")
plt.xticks(mpi_procs)
plt.grid(True)
plt.legend()
plt.tight_layout()
plt.savefig("weak_scalability_p64.png")
plt.show()


##### Optional: Add a Second Curve for 512B Payload

# Estimated missing value for 2 procs (based on your results)
# Between 87.4 (1 proc) and 83.0 (4 procs)
execution_time_p512 = [87.4273, 85.5, 83.0459, 22.0753]  # added 85.5 as estimate

plt.figure(figsize=(8, 5))
plt.plot(mpi_procs, execution_time_p512, marker='s', color='orange', label='Measured (512B payload)')
plt.plot(mpi_procs, [execution_time_p512[0]] * len(mpi_procs), linestyle='--', color='gray', label='Ideal (constant time)')

plt.xlabel("Number of MPI Processes")
plt.ylabel("Execution Time (s)")
plt.title("Weak Scalability - data_10M_p512.bin")
plt.xticks(mpi_procs)
plt.grid(True)
plt.legend()
plt.tight_layout()
plt.savefig("weak_scalability_p512.png")
plt.show()
