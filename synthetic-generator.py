#!/usr/bin/env python3
"""
Synthetic CI/CD Failure Telemetry Generator
Track 02 - Build with Graph Intelligence
Role: Member 3 (Synthetic Data & Integration Setup)

Generates realistic and deterministic build/test datasets for Delta Lake ingestion.
"""

import pandas as pd
import numpy as np
import random
import os

# Set seed for reproducibility on Game Day
random.seed(42)
np.random.seed(42)

# Save directly to current directory on your laptop
out_dir = "."

# Define mock codebase files
codebase_files = [
    "src/auth/legacy-jwt.js",
    "src/middleware/auth-middleware.js",
    "src/controllers/user-controller.js",
    "src/controllers/admin-controller.js",
    "src/auth/validators.js",
    "src/utils/logger.js",
    "src/db/connection.js",
    "src/models/user-model.js"
]

# Base failure probability per file (simulating real fragility)
base_failure_rates = {
    "src/auth/legacy-jwt.js": 0.35,
    "src/middleware/auth-middleware.js": 0.40,
    "src/controllers/user-controller.js": 0.15,
    "src/controllers/admin-controller.js": 0.10,
    "src/auth/validators.js": 0.08,
    "src/utils/logger.js": 0.05,
    "src/db/connection.js": 0.25,
    "src/models/user-model.js": 0.20
}

# 1. GENERATE: ci_build_history.csv
builds_list = []
num_builds = 500

for build_id in range(1001, 1001 + num_builds):
    file_changed = random.choice(codebase_files)
    fail_prob = base_failure_rates[file_changed]
    status = "failed" if random.random() < fail_prob else "passed"
    
    duration_sec = random.randint(30, 240)
    trigger = random.choice(["pr_merge", "manual_run", "nightly_cron"])
    commit_sha = f"cf{random.randint(100000, 999999)}"
    
    builds_list.append({
        "build_id": build_id,
        "commit_sha": commit_sha,
        "file_changed": file_changed,
        "status": status,
        "duration_seconds": duration_sec,
        "trigger_type": trigger,
        "data_provenance": "SYNTHETIC_DATA_PROTOTYPE"
    })

df_builds = pd.DataFrame(builds_list)
builds_csv_path = os.path.join(out_dir, "ci_build_history.csv")
df_builds.to_csv(builds_csv_path, index=False)

# 2. GENERATE: ci_test_runs.csv
tests_list = []
test_suites = [
    "tests/auth/jwt.test.js",
    "tests/middleware/auth.test.js",
    "tests/controllers/user.test.js",
    "tests/integration/login.test.js",
    "tests/utils/logger.test.js"
]

for idx, build in enumerate(builds_list):
    build_id = build["build_id"]
    status = build["status"]
    file_changed = build["file_changed"]
    
    for test in test_suites:
        test_duration = round(random.uniform(0.5, 12.0), 2)
        test_status = "passed"
        error_message = ""
        
        if status == "failed":
            if file_changed == "src/auth/legacy-jwt.js" and "jwt" in test:
                test_status = "failed" if random.random() < 0.8 else "passed"
                error_message = "JWT verification timed out or signature mismatch" if test_status == "failed" else ""
            elif file_changed == "src/middleware/auth-middleware.js" and "middleware" in test:
                test_status = "failed" if random.random() < 0.7 else "passed"
                error_message = "Middleware failed to intercept malicious header" if test_status == "failed" else ""
            elif file_changed == "src/utils/logger.js" and "logger" in test:
                test_status = "failed" if random.random() < 0.6 else "passed"
                error_message = "Logger writeStream error: disk full" if test_status == "failed" else ""
        
        tests_list.append({
            "test_run_id": f"t_{build_id}_{random.randint(100,999)}",
            "build_id": build_id,
            "test_suite": test,
            "status": test_status,
            "duration_seconds": test_duration,
            "error_msg": error_message,
            "data_provenance": "SYNTHETIC_DATA_PROTOTYPE"
        })

df_tests = pd.DataFrame(tests_list)
tests_csv_path = os.path.join(out_dir, "ci_test_runs.csv")
df_tests.to_csv(tests_csv_path, index=False)

print("\nSuccessfully generated datasets!")
print(f"CI Build History: {len(df_builds)} rows saved to '{builds_csv_path}'")
print(f"CI Test Runs: {len(df_tests)} rows saved to '{tests_csv_path}'\n")
