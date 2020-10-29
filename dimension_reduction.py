#!/usr/bin/env python
# coding: utf-8

# # Clustering dataset
import pickle
import logging

from pathlib import Path

import umap
import config_cluster

from numpy import load, save

logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO)


def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i : i + n]


if __name__ == "__main__":

    root_name = config_cluster.root_name
    num_cpu = config_cluster.num_cpu
    list_clustering_method = config_cluster.list_clustering_method
    # list_type_txt = ["raw", "clean"]
    list_type_txt = config_cluster.list_type_txt
    list_datasets = config_cluster.list_datasets
    list_n_dim = config_cluster.list_n_dim
    Path("./{}/dim_reduction/".format(root_name)).mkdir(parents=True, exist_ok=True)

    jobs = []
    for type_txt in list_type_txt:
        for dataset in list_datasets:
            logging.info("Getting the sentence to pass into cluster")
            data = load(
                "./{}/sentences_emb/sent_embeddings_{}_{}.npy".format(
                    root_name, type_txt, dataset
                )
            )
            data = data.tolist()
            for n_dim in list_n_dim:
                try:
                    logging.info(
                        "Try to load existing model to reduce in {} dim".format(n_dim)
                    )
                    filename = "./{}/dim_reduction/umap_{}_{}_{}.pkl".format(
                        root_name, n_dim, type_txt, dataset
                    )
                    model_dim_red = pickle.load(open(filename, "rb",))
                    logging.info("Success in loading model")
                except FileNotFoundError:
                    logging.info("No model existing, fitting a new one")
                    model_dim_red = umap.UMAP(
                        # low_memory=True,
                        n_neighbors=15,
                        min_dist=0.1,
                        n_components=n_dim,
                        verbose=True,
                    ).fit(data)
                    logging.info("New model fitted, record it")

                    pickle.dump(
                        model_dim_red, open(filename, "wb",),
                    )

        del data
