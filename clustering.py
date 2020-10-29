#!/usr/bin/env python
# coding: utf-8

# # Clustering dataset

# # Imports

# General import
import os
import re
import csv
import time
import pickle
import logging
import multiprocessing

from pathlib import Path
from itertools import chain
from collections import Counter

# Data imports
import hdbscan
import numpy as np
import umap.umap_ as umap

from tqdm import tqdm
from numpy import load, save

import config_cluster

logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO)


def write_report(queue, root_name, clustering_method, type_txt, dataset):
    """
    """

    csv_col = [
        "min_cluster_size",
        "min_sample",
        "n_dim",
        "validity_index_score",
        "noise",
        "top_10",
        "top_1",
        "n_clusters",
        "total",
    ]
    csv_file = "./{}/outputs/grid_search_cluster_{}_{}_{}.csv".format(
        root_name, clustering_method, type_txt, dataset
    )
    # with open(csv_file, "w") as f:
    #     writer = csv.DictWriter(f, fieldnames=csv_col)
    #     writer.writeheader()
    while True:
        report = queue.get()
        if report == "END":
            break
        with open(csv_file, "a") as f:
            writer = csv.DictWriter(f, fieldnames=csv_col)
            writer.writerow(report)


def get_stats_cluster(data, cluster):
    """
    """
    # Get the total
    labels = cluster.labels_
    total = len(labels)
    # validity_index_score = hdbscan.validity.validity_index(
    #     data.astype("double"), labels
    # )
    validity_index_score = cluster.relative_validity_

    # Get the string to be sure to remove the label '-1' and not the position index -1
    count_labels = Counter([str(x) for x in labels])
    # Get the noise
    noise = count_labels["-1"]
    # Remove the noise to be able to get the others infos
    del count_labels["-1"]
    # Top 10
    top_10 = sum([x[1] for x in count_labels.most_common(10)])
    # First cluster
    top_1 = [x[1] for x in count_labels.most_common(1)][0]
    # Get the number of clusters
    n_clusters = len(list(count_labels))

    return {
        "validity_index_score": validity_index_score,
        "noise": noise,
        "top_10": top_10,
        "top_1": top_1,
        "n_clusters": n_clusters,
        "total": total,
    }


def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i : i + n]


def getting_hdscan(
    root_name,
    type_txt,
    data,
    n_dim,
    dataset,
    list_cluster_size,
    clustering_method,
    min_sample,
    queue_report,
):

    start_time = time.time()

    memory_filename = "./{}/clusters/cache/{}-{}".format(root_name, min_sample, n_dim)
    for cluster_size in list_cluster_size:

        clusterer = hdbscan.HDBSCAN(
            min_cluster_size=cluster_size,
            min_samples=min_sample,
            gen_min_span_tree=True,
            # allow_single_cluster=True,
            memory=memory_filename,
        )

        filename = "./{}/clusters/{}_{}_{}_cluster-size_{}_min-samples_{}_n-dim_{}.pkl".format(
            root_name,
            clustering_method,
            type_txt,
            dataset,
            cluster_size,
            min_sample,
            n_dim,
        )
        log_filename = " ".join(filename.split("/")[-1].split("_")[7:])[
            :-4
        ]  # .split(".")[:-1]  # .replace("_", " ")
        try:
            logging.info("Check if existing: {}".format(log_filename))
            clusterer = pickle.load(open(filename, "rb"))
            logging.info(
                " Exists -- skip".format(
                    "hdbscan", type_txt, dataset, cluster_size, n_dim
                )
            )
        except FileNotFoundError:

            clusterer.fit(data)
            dict_report = get_stats_cluster(data, clusterer)
            dict_report["min_cluster_size"] = cluster_size
            dict_report["min_sample"] = min_sample
            dict_report["n_dim"] = n_dim
            queue_report.put(dict_report)
            pickle.dump(
                clusterer, open(filename, "wb"),
            )

            end_time = time.time()
            logging.info(
                "Finished to fin in {:.2f} : {}".format(
                    end_time - start_time, log_filename
                )
            )
        except Exception as e:
            logging.error("{} : {}".format(filename, e))
            raise


if __name__ == "__main__":

    root_name = config_cluster.root_name
    num_cpu = config_cluster.num_cpu
    list_clustering_method = config_cluster.list_clustering_method
    # list_type_txt = ["raw", "clean"]
    list_type_txt = config_cluster.list_type_txt
    list_datasets = config_cluster.list_datasets
    list_n_dim = config_cluster.list_n_dim
    # list_n_dim = ["original"]
    # list_cluster_size = range(10, 20)
    list_cluster_size = config_cluster.list_cluster_size
    list_min_size = config_cluster.list_min_size
    # Create the folders
    Path("./{}/sentences_emb/".format(root_name)).mkdir(parents=True, exist_ok=True)
    Path("./{}/dim_reduction/".format(root_name)).mkdir(parents=True, exist_ok=True)
    Path("./{}/clusters/".format(root_name)).mkdir(parents=True, exist_ok=True)

    jobs = []

    queue_report = multiprocessing.Queue()

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
                if n_dim != "original":
                    try:
                        logging.info(
                            "Try to load existing model to reduce in {} dim".format(
                                n_dim
                            )
                        )
                        model_dim_red = pickle.load(
                            open(
                                "./{}/dim_reduction/umap_{}_{}_{}.pkl".format(
                                    root_name, n_dim, type_txt, dataset
                                ),
                                "rb",
                            )
                        )
                        logging.info("Success in loading model")
                    except FileNotFoundError:
                        logging.info("No model existing, fitting a new one")
                        raise
                    logging.info("Reducing the sent_emb to {} dim".format(n_dim))
                    data_reduced = model_dim_red.transform(data)
                    logging.info("Create the jobs")
                else:
                    data_reduced = data
                for clustering_method in list_clustering_method:
                    for min_sample in list_min_size:
                        p = multiprocessing.Process(
                            target=getting_hdscan,
                            args=(
                                root_name,
                                type_txt,
                                data_reduced,
                                n_dim,
                                dataset,
                                list_cluster_size,
                                clustering_method,
                                min_sample,
                                queue_report,
                            ),
                        )
                        jobs.append(p)
            del data_reduced
        del data

        logging.info("Number of jobs: {}".format(len(jobs)))

        # Start the process that wrote the dictionary into the csv file
        writer = multiprocessing.Process(
            target=write_report,
            args=(queue_report, root_name, clustering_method, type_txt, dataset,),
        )
        writer.start()
        for i in tqdm(chunks(jobs, num_cpu), total=len(jobs)):
            for j in i:
                j.start()
            for j in i:
                j.join()
        queue_report.put("END")
