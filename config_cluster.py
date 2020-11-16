"""
Config file for clusters creation and search
"""
from itertools import chain

root_name = "01-2020_09-2020"
num_cpu = 5
list_type_txt = ["txt_wo_entities"]
list_datasets = ["filtered_wo_rt"]
list_n_dim = [5, 10, 25]
list_cluster_size = list(
    chain(
        # range(3, 20, 1),
        # range(10, 50, 5),
        range(50, 100, 10),
        range(100, 501, 10),
    )
)
list_min_size = list(
    chain(
        range(1, 25),
        range(25, 50, 5),
        # range(50, 100, 10),
        # range(100, 201, 50),
    )
)


umap_n_neighbours = 15
umap_min_dist = 0.1
