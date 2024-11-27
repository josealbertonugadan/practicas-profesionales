import os

from airflow import DAG
from op_dags.daf_operators.common import FlowVars, get_all_vars_files
from op_dags.dag_templates.batch_dynamic import create_dag


def main():
    vars_folder = "u_ids_pp"
    dags_folder = os.getenv("DAGS_FOLDER", "dags")
    vars_folder = os.getenv("TEST_FOLDER", vars_folder)
    
    folder = os.path.join(dags_folder, f"vars/{vars_folder}")

    for vars_ in get_all_vars_files(folder):
        fv = FlowVars(folder, vars_)
        globals()[fv.dag.dag_id] = create_dag(fv)


main()
