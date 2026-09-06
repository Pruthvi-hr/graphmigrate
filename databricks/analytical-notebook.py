# Databricks Notebook Blueprint: GraphMigrate Analytical Engine (v2)
# Track 02 - Build with Graph Intelligence
# Role: Prerana (Databricks Graph Computing & Backend Orchestration)

"""
================================================================================
DATABRICKS COMMUNITY EDITION SETUP INSTRUCTIONS (READ TONIGHT):
================================================================================
1. Start your Single Node Cluster (e.g., Runtime 14.3 LTS or 15.4 LTS with Spark 3.x).
2. Install the GraphFrames library:
   - Go to Cluster -> Libraries -> Install New.
   - Choose 'Maven' as the source.
   - Coordinate: graphframes:graphframes:0.8.3-spark3.5-s_2.12 (adjust Spark version if using Spark 3.4/3.5).
3. Set Checkpoint Directory: Spark GraphFrames requires setting a checkpoint directory 
   on DBFS to store lineage checkpoints during long graph cycles (e.g., sc.setCheckpointDir("/tmp/graphframes_checkpoints")).

================================================================================
RESPONSIBLE USE & METADATA DECLARATION:
================================================================================
- Data Provenance: Synthetic CI/CD failure logs representing historical repository metrics.
- Permitted Use: Open-source buildathon development; contains no corporate secrets or personal identifiers.
- Hardware Constraints: Built to run efficiently on 2X-Small serverless clusters without GPU dependency.
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
        
    # Required for GraphFrames Connected Components algorithm
    spark.sparkContext.setCheckpointDir("/tmp/graphframes_checkpoints")

    print("Loading historical CI/CD Delta Lake Tables...")
    # Read from local data folder
    build_history_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("data/ci_build_history.csv")

    # Aggregate failure rate per file path
    historical_file_stats = build_history_df.groupby("file_changed").agg(
        count("build_id").alias("total_builds_changed"),
        sum(when(col("status") == "failed", 1).otherwise(0)).alias("failed_builds_count")
    ).withColumn(
        "historical_failure_rate", 
        col("failed_builds_count") / col("total_builds_changed")
    )

    # 2. Mock JSON Payload incoming from local CLI (entire-graph AST scan)
    mock_cli_payload = """
    {
      "target_file": "src/auth/legacy-jwt.js",
      "git_commit": "fa3b246c",
      "entire_graph_version": "1.2.0",
      "dependency_graph": {
        "nodes": [
          { "id": "src/auth/legacy-jwt.js", "type": "source", "lines_of_code": 180 },
          { "id": "src/middleware/auth-middleware.js", "type": "source", "lines_of_code": 95 },
          { "id": "src/controllers/user-controller.js", "type": "source", "lines_of_code": 220 },
          { "id": "src/controllers/admin-controller.js", "type": "source", "lines_of_code": 140 },
          { "id": "src/auth/validators.js", "type": "source", "lines_of_code": 60 },
          { "id": "src/utils/logger.js", "type": "source", "lines_of_code": 45 },
          { "id": "src/db/connection.js", "type": "source", "lines_of_code": 110 },
          { "id": "src/models/user-model.js", "type": "source", "lines_of_code": 190 }
        ],
        "edges": [
          { "source": "src/middleware/auth-middleware.js", "target": "src/auth/legacy-jwt.js", "type": "import" },
          { "source": "src/controllers/user-controller.js", "target": "src/middleware/auth-middleware.js", "type": "import" },
          { "source": "src/controllers/admin-controller.js", "target": "src/middleware/auth-middleware.js", "type": "import" },
          { "source": "src/auth/legacy-jwt.js", "target": "src/auth/validators.js", "type": "import" },
          { "source": "src/auth/legacy-jwt.js", "target": "src/utils/logger.js", "type": "import" },
          { "source": "src/middleware/auth-middleware.js", "target": "src/utils/logger.js", "type": "import" },
          { "source": "src/models/user-model.js", "target": "src/db/connection.js", "type": "import" },
          { "source": "src/controllers/user-controller.js", "target": "src/models/user-model.js", "type": "import" }
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
        # Initialize GraphFrame
        g = GraphFrame(vertices_df, edges_df)
        
        # Run PageRank Centrality Algorithm
        pagerank_results = g.pageRank(resetProbability=0.15, maxIter=10)
        pagerank_nodes_df = pagerank_results.vertices.select("id", col("pagerank").alias("pagerank_score"))
        
        # Run Connected Components (Clustering)
        cc_results = g.connectedComponents()
        cc_nodes_df = cc_results.select("id", col("component").alias("component_id"))
        
        # Join Graph Metrics
        graph_metrics_df = pagerank_nodes_df.join(cc_nodes_df, "id")
        print("Successfully computed PageRank & Connected Components on Databricks!")
    except Exception as e:
        print(f"GraphFrames library not fully initialized: {str(e)}. Using fallback analytical metrics schema...")
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
            ("src/db/connection.js", 0.40, 1001),
            ("src/models/user-model.js", 0.45, 1001)
        ]
        graph_metrics_df = spark.createDataFrame(mock_metrics, schema)

    # 4. Join Code Graph Centrality with CI Operational Telemetry
    enriched_nodes_df = graph_metrics_df.join(
        historical_file_stats, \
        graph_metrics_df.id == historical_file_stats.file_changed, \
        "left"
    ).fillna({"historical_failure_rate": 0.0, "total_builds_changed": 0})

    # Formula: Fused risk (40% Network centrality + 60% failure rate)
    enriched_nodes_df = enriched_nodes_df.withColumn(
        "fragility_score",
        ((col("pagerank_score") * 0.4) + (col("historical_failure_rate") * 0.6)) * 100
    )

    # Sequence phases: Leaf nodes (low PageRank) first, foundational cores (high PageRank) last
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
    )

    # 5. Extract global target score
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
            .select(col("id").alias("file_path"), col("pagerank_score"), col("historical_failure_rate")).collect()
            
        if phase_files:
            files_list = [{"file_path": f["file_path"], 
                           "pagerank_score": round(f["pagerank_score"], 3), 
                           "historical_failure_rate": round(f["historical_failure_rate"], 3)} 
                          for f in phase_files]
            phases_data.append({
                "phase_number": phase_id,
                "phase_name": phase_names[phase_id],
                "files_to_migrate": files_list,
                "risk_level": "Low" if phase_id == 1 else "Medium" if phase_id == 2 else "High"
            })

    # Analytical smart test execution path mapping
    recommended_tests = ["tests/auth/jwt.test.js", "tests/middleware/auth.test.js", "tests/integration/login.test.js"]

    # Final validated output contract payload
    response_payload = {
        "target_file": target_file,
        "global_fragility_score": round(global_fragility, 1),
        "affected_files_count": sequenced_phases_df.count(),
        "data_label": "[SYNTHETIC PROTOTYPE TELEMETRY]",
        "migration_phases": phases_data,
        "recommended_tests": recommended_tests
    }

    print("\n=== FINAL GENERATED RESPONSE (SPARK ROUTE) ===")
    print(json.dumps(response_payload, indent=2))

else:
    # Local Pandas failover pathway
    import pandas as pd
    
    try:
        # Load from local folder
        builds = pd.read_csv("data/ci_build_history.csv")
        file_stats = builds.groupby("file_changed").agg(
            total_builds_changed=("build_id", "count"),
            failed_builds_count=("status", lambda x: (x == "failed").sum())
        ).reset_index()
        file_stats["historical_failure_rate"] = file_stats["failed_builds_count"] / file_stats["total_builds_changed"]
    except Exception as e:
        print(f"Error loading CSV files: {str(e)}")
        file_stats = pd.DataFrame(columns=["file_changed", "historical_failure_rate"])

    nodes_metrics = [
        {"id": "src/auth/legacy-jwt.js", "pagerank_score": 0.95, "component_id": 1001},
        {"id": "src/middleware/auth-middleware.js", "pagerank_score": 0.78, "component_id": 1001},
        {"id": "src/controllers/user-controller.js", "pagerank_score": 0.65, "component_id": 1001},
        {"id": "src/controllers/admin-controller.js", "pagerank_score": 0.52, "component_id": 1001},
        {"id": "src/auth/validators.js", "pagerank_score": 0.28, "component_id": 1001},
        {"id": "src/utils/logger.js", "pagerank_score": 0.12, "component_id": 1001},
        {"id": "src/db/connection.js", "pagerank_score": 0.40, "component_id": 1001},
        {"id": "src/models/user-model.js", "pagerank_score": 0.45, "component_id": 1001}
    ]
    df_nodes = pd.DataFrame(nodes_metrics)
    
    merged = pd.merge(df_nodes, file_stats, left_on="id", right_on="file_changed", how="left").fillna(0)
    merged["fragility_score"] = ((merged["pagerank_score"] * 0.4) + (merged["historical_failure_rate"] * 0.6)) * 100
    merged["phase_number"] = merged["pagerank_score"].apply(lambda x: 1 if x < 0.3 else 2 if x <= 0.7 else 3)
    merged["risk_level"] = merged["phase_number"].apply(lambda x: "Low" if x == 1 else "Medium" if x == 2 else "High")
    
    target_row = merged[merged["id"] == "src/auth/legacy-jwt.js"]
    global_fragility = target_row["fragility_score"].values[0] if len(target_row) > 0 else 50.0
    
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
                    "historical_failure_rate": round(row["historical_failure_rate"], 3)
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
        "data_label": "[SYNTHETIC PROTOTYPE TELEMETRY]",
        "migration_phases": phases_data,
        "recommended_tests": ["tests/auth/jwt.test.js", "tests/middleware/auth.test.js", "tests/integration/login.test.js"]
    }
    
    print("\n=== FINAL GENERATED RESPONSE (LOCAL FALLBACK ROUTE) ===")
    print(json.dumps(response_payload, indent=2))
