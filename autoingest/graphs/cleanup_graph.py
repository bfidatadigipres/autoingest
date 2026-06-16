from dagster import graph

from autoingest.ops.local.cleanup_sweep import sweep_completed_files


@graph
def sweep_graph():
    sweep_completed_files()
