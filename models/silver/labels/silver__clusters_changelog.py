import pandas as pd
import networkx as nx
import numpy as np
import datetime


def model(dbt, session):

    dbt.config(
        materialized="table",
        packages=["networkx"],
        unique_key="ADDRESS",
        tags=['entity_cluster'],
        enabled=False
    )

    # final_btc_clusters = dbt.source("bitcoin_bronze", "entity_clusters")

    return ''
