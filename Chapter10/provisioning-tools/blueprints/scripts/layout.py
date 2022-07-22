import argparse
import sys
import os
import json
from awsglue.blueprint.workflow import Workflow, Entities
from awsglue.blueprint.job import Job


def generate_layout(user_params, system_params):
    etl_job = Job(
        Name="ch10_5_example_bp",
        Command={
            "Name": "glueetl", 
            "ScriptLocation": user_params['ScriptLocation'], 
            "PythonVersion": "3"},
        Role=user_params['GlueJobRoleName'],
        WorkerType="G.1X",
        NumberOfWorkers=5,
        GlueVersion="3.0")

    return Workflow(Name=user_params['WorkflowName'], Entities=Entities(Jobs=[etl_job]))