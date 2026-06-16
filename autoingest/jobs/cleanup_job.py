from autoingest.graphs.cleanup_graph import sweep_graph

cleanup_job = sweep_graph.to_job(
    name="cleanup_job",
    description="Sweep completed files and clean up source. Runs in-process on DATA15.",
)
