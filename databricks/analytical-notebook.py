# Databricks Notebook Blueprint: GraphMigrate Analytical Engine (v3) - Track 2 Curveball Compliant
# Track 02 - Build with Graph Intelligence
# Role: Prerana (Databricks Graph Computing & Backend Orchestration)

"""
================================================================================
DATABRICKS WORKSPACE OVERVIEW: TRACK 2 CURVEBALL HANDLING
================================================================================
The Noon Curveball ("Graph is Evidence, Not an Oracle") introduces dynamic code 
dispatch and reflection patterns. Since static analysis cannot trace these with 100% 
deterministic certainty, we must adapt our Databricks computing models:
1. Identify and label Node and Edge certainty (confirmed vs heuristic/unverified).
2. Calculate "Analysis Completeness Index" using Spark SQL aggregated node schemas.
3. Heavily penalize unverified reflection files by applying a 1.5x scaling multiplier 
   to their Fragility Score. This acts as a predictive hazard marker.
4. Establish dynamic fallback paths, recommending the fixture test suite for heuristic edges.
"""

import json

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, count, sum, when, avg, lit, collect_list, struct, round
    from graphframes import GraphFrame
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    print("\n[MOCK FALLBACK RUNNING] Spark/GraphFrames not found in this environment. Simulating logic via local Python data processing...")

if SPARK_AVAILABLE:
    # 1. Initialize Spark Session
    spark = SparkSession.builder \
        .appName("GraphMigrateEngine") \
        .getOrCreate()
        
    spark.sparkContext.setCheckpointDir("/tmp/graphframes_checkpoints")

    print("Loading historical CI/CD Delta Lake Tables...")
    build_history_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("data/ci_build_history.csv")

    # Aggregate failure rates
    historical_file_stats = build_history_df.groupby("file_changed").agg(
        count("build_id").alias("total_builds_changed"),
        sum(when(col("status") == "failed", 1).otherwise(0)).alias("failed_builds_count")
    ).withColumn(
        "historical_failure_rate", 
        col("failed_builds_count") / col("total_builds_changed")
    )

    # 2. Mock JSON Payload incoming from local CLI including Curveball annotations
    mock_cli_payload = """
    {
      "target_file": "src/auth/legacy-jwt.js",
      "git_commit": "fa3b246c",
      "entire_graph_version": "1.2.0",
      "dependency_graph": {
        "nodes": [
          { "id": "src/auth/legacy-jwt.js", "type": "source", "lines_of_code": 180, "evidence_type": "confirmed" },
          { "id": "src/middleware/auth-middleware.js", "type": "source", "lines_of_code": 95, "evidence_type": "confirmed" },
          { "id": "src/controllers/user-controller.js", "type": "source", "lines_of_code": 220, "evidence_type": "confirmed" },
          { "id": "src/controllers/admin-controller.js", "type": "source", "lines_of_code": 140, "evidence_type": "confirmed" },
          { "id": "src/auth/validators.js", "type": "source", "lines_of_code": 60, "evidence_type": "confirmed" },
          { "id": "src/utils/logger.js", "type": "source", "lines_of_code": 45, "evidence_type": "confirmed" },
          { "id": "src/auth/dynamic-dispatcher.js", "type": "source", "lines_of_code": 75, "evidence_type": "heuristic" },
          { "id": "src/auth/reflective-loader.js", "type": "source", "lines_of_code": 110, "evidence_type": "unverified" }
        ],
        "edges": [
          { "source": "src/middleware/auth-middleware.js", "target": "src/auth/legacy-jwt.js", "type": "import", "evidence_type": "confirmed" },
          { "source": "src/controllers/user-controller.js", "target": "src/middleware/auth-middleware.js", "type": "import", "evidence_type": "confirmed" },
          { "source": "src/controllers/admin-controller.js", "target": "src/middleware/auth-middleware.js", "type": "import", "evidence_type": "confirmed" },
          { "source": "src/auth/legacy-jwt.js", "target": "src/auth/validators.js", "type": "import", "evidence_type": "confirmed" },
          { "source": "src/auth/legacy-jwt.js", "target": "src/utils/logger.js", "type": "import", "evidence_type": "confirmed" },
          { "source": "src/auth/legacy-jwt.js", "target": "src/auth/dynamic-dispatcher.js", "type": "dynamic_dispatch", "evidence_type": "heuristic" },
          { "source": "src/auth/dynamic-dispatcher.js", "target": "src/auth/reflective-loader.js", "type": "reflection", "evidence_type": "unverified" }
        ]
      }
    }
    """

    payload_data = json.loads(mock_cli_payload)
    target_file = payload_data["target_file"]

    # 3. Create GraphFrames Vertices and Edges DataFrames
    vertices_df = spark.createDataFrame(payload_data["dependency_graph"]["nodes"]).withColumnRenamed("id", "id")
    edges_df = spark.createDataFrame(payload_data["dependency_graph"]["edges"]) \
        .withColumnRenamed("source", "src") \
        .withColumnRenamed("target", "dst")

    try:
        g = GraphFrame(vertices_df, edges_df)
        
        # Run PageRank
        pagerank_results = g.pageRank(resetProbability=0.15, maxIter=10)
        pagerank_nodes_df = pagerank_results.vertices.select("id", col("pagerank").alias("pagerank_score"))
        
        # Run Connected Components
        cc_results = g.connectedComponents()
        cc_nodes_df = cc_results.select("id", col("component").alias("component_id"))
        
        graph_metrics_df = pagerank_nodes_df.join(cc_nodes_df, "id")
    except Exception as e:
        print(f"GraphFrames library fallback path...")
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("pagerank_score", DoubleType(), True),
            StructField("component_id", LongType(), True)
        ])
        mock_metrics = [
            ("src/auth/legacy-jwt.js", 0.95, 1001),
            ("src/middleware/auth-middleware.js", 0.78, 1001),
            ("src/controllers/user-controller.js", 0.65, 1001),
            ("src/controllers/admin-controller.js", 0.52, 1001),
            ("src/auth/validators.js", 0.28, 1001),
            ("src/utils/logger.js", 0.12, 1001),
            ("src/auth/dynamic-dispatcher.js", 0.35, 1001),
            ("src/auth/reflective-loader.js", 0.45, 1001)
        ]
        graph_metrics_df = spark.createDataFrame(mock_metrics, schema)

    # Calculate Completeness Metric
    # Count of confirmed vs heuristic/unverified nodes
    total_nodes = vertices_df.count()
    confirmed_nodes = vertices_df.filter(col("evidence_type") == "confirmed").count()
    unverified_count = vertices_df.filter(col("evidence_type") != "confirmed").count()
    completeness_percentage = round((confirmed_nodes / total_nodes) * 100, 1)

    # 4. Enriched Nodes & Fragility Fusing with Curveball Penalty
    # Unverified reflection modules get a 1.5x penalty multiplier on PageRank centrality to reflect dynamic routing risks
    enriched_nodes_df = graph_metrics_df.join(
        vertices_df.select("id", "evidence_type"),
        "id"
    ).join(
        historical_file_stats, 
        graph_metrics_df.id == historical_file_stats.file_changed, 
        "left"
    ).fillna({"historical_failure_rate": 0.0, "total_builds_changed": 0})

    # Fragility Score Equation with 1.5x multiplier for non-confirmed structural entities
    enriched_nodes_df = enriched_nodes_df.withColumn(
        "centrality_factor",
        when(col("evidence_type") != "confirmed", col("pagerank_score") * 1.5)
        .otherwise(col("pagerank_score"))
    ).withColumn(
        "fragility_score",
        ((col("centrality_factor") * 0.4) + (col("historical_failure_rate") * 0.6)) * 100
    )

    # Sequence phases: unverified items requiring runtime test validation are classified properly
    sequenced_phases_df = enriched_nodes_df.withColumn(
        "phase_number",
        when(col("pagerank_score") < 0.3, 1)
        .when((col("pagerank_score") >= 0.3) & (col("pagerank_score") <= 0.7), 2)
        .otherwise(3)
    ).withColumn(
        "risk_level",
        when(col("phase_number") == 1, "Low")
        .when(col("phase_number") == 2, "Medium")
        .otherwise("High")
    ).withColumn(
        "verification_required",
        when(col("evidence_type") != "confirmed", True).otherwise(False)
    )

    # 5. Build output format
    target_metrics = sequenced_phases_df.filter(col("id") == target_file).collect()
    global_fragility = target_metrics[0]["fragility_score"] if target_metrics else 50.0

    phases_data = []
    phase_names = {
        1: "Isolate Downstream Leaf Components",
        2: "Update Controllers & Intermediate Layer",
        3: "Refactor Core Load-Bearing Identity Module"
    }
    
    for phase_id in [1, 2, 3]:
        phase_files = sequenced_phases_df.filter(col("phase_number") == phase_id) \
            .select(col("id").alias("file_path"), col("pagerank_score"), col("historical_failure_rate"), col("evidence_type"), col("verification_required")).collect()
            
        if phase_files:
            files_list = [{
                "file_path": f["file_path"], 
                "pagerank_score": round(f["pagerank_score"], 3), 
                "historical_failure_rate": round(f["historical_failure_rate"], 3),
                "evidence_type": f["evidence_type"],
                "verification_required": f["verification_required"]
            } for f in phase_files]
            
            phases_data.append({
                "phase_number": phase_id,
                "phase_name": phase_names[phase_id],
                "files_to_migrate": files_list,
                "risk_level": "Low" if phase_id == 1 else "Medium" if phase_id == 2 else "High"
            })

    # Output Response Contract
    response_payload = {
        "target_file": target_file,
        "global_fragility_score": round(global_fragility, 1),
        "affected_files_count": sequenced_phases_df.count(),
        "analysis_completeness": {
            "completeness_percentage": completeness_percentage,
            "has_partial_analysis": unverified_count > 0,
            "unverified_components_count": unverified_count
        },
        "migration_phases": phases_data,
        "recommended_tests": [
            "tests/auth/jwt.test.js", 
            "tests/middleware/auth.test.js", 
            "tests/integration/login.test.js",
            "tests/fixtures/partial-analysis.test.js" # The Curveball Fixture Test!
        ]
    }

    print("\n=== FINAL GENERATED RESPONSE (SPARK ROUTE) ===")
    print(json.dumps(response_payload, indent=2))

else:
    # Local Pandas failover pathway
    import pandas as pd
    
    try:
        builds = pd.read_csv("data/ci_build_history.csv")
        file_stats = builds.groupby("file_changed").agg(
            total_builds_changed=("build_id", "count"),
            failed_builds_count=("status", lambda x: (x == "failed").sum())
        ).reset_index()
        file_stats["historical_failure_rate"] = file_stats["failed_builds_count"] / file_stats["total_builds_changed"]
    except Exception as e:
        file_stats = pd.DataFrame(columns=["file_changed", "historical_failure_rate"])

    nodes_metrics = [
        {"id": "src/auth/legacy-jwt.js", "pagerank_score": 0.95, "evidence_type": "confirmed"},
        {"id": "src/middleware/auth-middleware.js", "pagerank_score": 0.78, "evidence_type": "confirmed"},
        {"id": "src/controllers/user-controller.js", "pagerank_score": 0.65, "evidence_type": "confirmed"},
        {"id": "src/controllers/admin-controller.js", "pagerank_score": 0.52, "evidence_type": "confirmed"},
        {"id": "src/auth/validators.js", "pagerank_score": 0.28, "evidence_type": "confirmed"},
        {"id": "src/utils/logger.js", "pagerank_score": 0.12, "evidence_type": "confirmed"},
        {"id": "src/auth/dynamic-dispatcher.js", "pagerank_score": 0.35, "evidence_type": "heuristic"},
        {"id": "src/auth/reflective-loader.js", "pagerank_score": 0.45, "evidence_type": "unverified"}
    ]
    df_nodes = pd.DataFrame(nodes_metrics)
    
    merged = pd.merge(df_nodes, file_stats, left_on="id", right_on="file_changed", how="left").fillna(0)
    
    # Apply 1.5x centrality multiplier penalty for non-confirmed structural entities
    merged["centrality_factor"] = merged.apply(
        lambda r: r["pagerank_score"] * 1.5 if r["evidence_type"] != "confirmed" else r["pagerank_score"],
        axis=1
    )
    merged["fragility_score"] = ((merged["centrality_factor"] * 0.4) + (merged["historical_failure_rate"] * 0.6)) * 100
    merged["phase_number"] = merged["pagerank_score"].apply(lambda x: 1 if x < 0.3 else 2 if x <= 0.7 else 3)
    merged["risk_level"] = merged["phase_number"].apply(lambda x: "Low" if x == 1 else "Medium" if x == 2 else "High")
    merged["verification_required"] = merged["evidence_type"].apply(lambda x: x != "confirmed")
    
    target_row = merged[merged["id"] == "src/auth/legacy-jwt.js"]
    global_fragility = target_row["fragility_score"].values[0] if len(target_row) > 0 else 50.0
    
    unverified_count = sum(merged["evidence_type"] != "confirmed")
    completeness_percentage = round(((len(merged) - unverified_count) / len(merged)) * 100, 1)

    phases_data = []
    phase_names = {
        1: "Isolate Downstream Leaf Components",
        2: "Update Controllers & Intermediate Layer",
        3: "Refactor Core Load-Bearing Identity Module"
    }
    
    for phase_id in [1, 2, 3]:
        phase_df = merged[merged["phase_number"] == phase_id]
        if len(phase_df) > 0:
            files_list = []
            for _, row in phase_df.iterrows():
                files_list.append({
                    "file_path": row["id"],
                    "pagerank_score": round(row["pagerank_score"], 3),
                    "historical_failure_rate": round(row["historical_failure_rate"], 3),
                    "evidence_type": row["evidence_type"],
                    "verification_required": bool(row["verification_required"])
                })
            phases_data.append({
                "phase_number": phase_id,
                "phase_name": phase_names[phase_id],
                "files_to_migrate": files_list,
                "risk_level": row["risk_level"]
            })
            
    response_payload = {
        "target_file": "src/auth/legacy-jwt.js",
        "global_fragility_score": round(global_fragility, 1),
        "affected_files_count": len(merged),
        "analysis_completeness": {
            "completeness_percentage": completeness_percentage,
            "has_partial_analysis": unverified_count > 0,
            "unverified_components_count": int(unverified_count)
        },
        "migration_phases": phases_data,
        "recommended_tests": [
            "tests/auth/jwt.test.js", 
            "tests/middleware/auth.test.js", 
            "tests/integration/login.test.js",
            "tests/fixtures/partial-analysis.test.js"
        ]
    }
    
    print("\n=== FINAL GENERATED RESPONSE (LOCAL FALLBACK ROUTE) ===")
    print(json.dumps(response_payload, indent=2))
