#!/usr/bin/env python
# coding: utf-8

"""
Grid search implementation for HDBSCAN.
Perform clustering using different min_cluster_size and mim_sample and number of dimension using UMAP

It outputs the results in a csv file that can be used to rerun this script and skip the already run clusters.
Used the relative_validity_ measure to select the best cluster methods
Take advantage of the cached implementation to play with the cluster size and use multiprocessing to spam several
clustering at the same time
"""

# General import
import os
import re
import csv
import time

# import pickle
import logging
import multiprocessing

from pathlib import Path
from itertools import chain
from collections import Counter

import joblib

# Data imports
import umap
import hdbscan
import numpy as np

import config_cluster

# import umap.umap_ as umap
from tqdm import tqdm
from numpy import load, save

logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO)


def write_report(queue, root_name, type_txt, dataset):
    """
    """

    csv_col = [
        "n_dim",
        "min_sample",
        "min_cluster_size",
        "validity_index_score",
        "noise",
        "top_10",
        "top_1",
        "n_clusters",
        "total",
    ]
    csv_file = "./{}/outputs/grid_search_hdbscan_{}_{}.csv".format(
        root_name, type_txt, dataset
    )
    while True:
        report = queue.get()
        if report == "END":
            break
        try:
            with open(csv_file, "a") as f:
                writer = csv.DictWriter(f, fieldnames=csv_col)
                writer.writerow(report)
        except FileNotFoundError:
            with open(csv_file, "w") as f:
                writer = csv.DictWriter(f, fieldnames=csv_col)
                writer.writeheader()
                writer.writerow(report)


def get_stats_cluster(data, cluster, relative_validity=True):
    """
    """
    # Get the total
    labels = cluster.labels_
    total = len(labels)
    if relative_validity is True:
        validity_index_score = cluster.relative_validity_
    else:
        validity_index_score = hdbscan.validity.validity_index(
            X=cluster._raw_data.astype("double"), labels=cluster.labels_
        )

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
    foldername,
    type_txt,
    data,
    n_dim,
    dataset,
    list_cluster_size,
    min_sample,
    queue_report,
    record=False,
):

    start_time = time.time()

    memory_filename = "./{}/cache/{}-{}".format(foldername, min_sample, n_dim)
    for cluster_size in list_cluster_size:

        clusterer = hdbscan.HDBSCAN(
            min_cluster_size=cluster_size,
            min_samples=min_sample,
            gen_min_span_tree=True,
            allow_single_cluster=True,
            # metric="cosine",
            memory=memory_filename,
        )

        filename = "./{}/{}_{}_cluster-size_{}_min-samples_{}_n-dim_{}.pkl".format(
            foldername, type_txt, dataset, cluster_size, min_sample, n_dim,
        )
        log_filename = " ".join(filename.split("/")[-1].split("_")[7:])[
            :-4
        ]  # .split(".")[:-1]  # .replace("_", " ")
        # try:
        #     logging.info("Check if existing: {}".format(log_filename))
        #     clusterer =joblib.load(open(filename, "rb"))
        #     logging.info(
        #         " Exists -- skip".format(
        #             "hdbscan", type_txt, dataset, cluster_size, n_dim
        #         )
        #     )
        # except FileNotFoundError:

        try:
            clusterer.fit(data)
            dict_report = get_stats_cluster(data, clusterer)
            dict_report["min_cluster_size"] = cluster_size
            dict_report["min_sample"] = min_sample
            dict_report["n_dim"] = n_dim
            queue_report.put(dict_report)
            if record == True:
                joblib.dump(
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
    # list_type_txt = ["raw", "clean"]
    type_txt = config_cluster.list_type_txt[0]
    dataset = config_cluster.list_datasets[0]
    list_n_dim = config_cluster.list_n_dim
    # list_n_dim = ["original"]
    # list_cluster_size = range(10, 20)
    list_cluster_size = config_cluster.list_cluster_size
    list_min_size = config_cluster.list_min_size

    umap_n_neighbours = config_cluster.umap_n_neighbours
    umap_min_dist = config_cluster.umap_min_dist

    # Header variable for the csv file to ensure consistency

    # Create the folders
    cluster_foldername = "./{}/clusters/".format(root_name)
    dim_red_foldername = "./{}/dim_reduction/".format(root_name)
    sent_emb_foldername = "./{}/sentences_emb/".format(root_name)
    Path(cluster_foldername).mkdir(parents=True, exist_ok=True)
    Path(dim_red_foldername).mkdir(parents=True, exist_ok=True)
    Path(sent_emb_foldername).mkdir(parents=True, exist_ok=True)

    jobs = []

    # Check which clusters have already recorded results to avoid running them another time

    all_clusters_to_do = dict()
    for n_dim in list_n_dim:
        for min_sample in list_min_size:
            for min_cluster_size in list_cluster_size:
                all_clusters_to_do.setdefault(n_dim, {}).setdefault(
                    min_sample, []
                ).append(min_cluster_size)
                # original_cluster_list.append((n_dim, min_cluster_size, min_sample))

    # print(all_clusters_to_do)
    try:

        csv_file = "./{}/outputs/grid_search_hdbscan_{}_{}.csv".format(
            root_name, type_txt, dataset
        )
        with open(csv_file, "r") as f:
            csv_reader = csv.reader(f)
            # Get the list of rows, skipping the headers
            # The order of the values is within the headers_variable
            next(csv_reader)  # To avoid header
            for n_dim, min_sample, cluster_size, *_ in csv_reader:
                try:
                    all_clusters_to_do[n_dim][min_sample].remove(cluster_size)
                    if len(all_clusters_to_do) == 0:
                        del all_clusters_to_do[n_dim][min_sample]
                        if all_clusters_to_do[n_dim]:
                            del all_clusters_to_do[n_dim]
                except KeyError:
                    pass

        # cluster_to_do = set(original_cluster_list) - set(done_clusters)

    # In case no records found
    except FileNotFoundError:
        pass

    queue_report = multiprocessing.Queue()

    logging.info("Getting the sentence to pass into cluster")
    data = load(
        "./{}/sentences_emb/sent_embeddings_{}_{}.npy".format(
            root_name, type_txt, dataset
        )
    )
    data = data.tolist()
    for n_dim in all_clusters_to_do:
        if n_dim != "original":
            try:
                logging.info(
                    "Try to load existing model to reduce in {} dim".format(n_dim)
                )
                model_dim_red = joblib.load(
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

                model_dim_red = umap.UMAP(
                    # low_memory=True,
                    n_neighbors=umap_n_neighbours,
                    min_dist=umap_min_dist,
                    n_components=n_dim,
                    metric="cosine",
                    verbose=True,
                ).fit(data)
                logging.info("New model fitted, record it")

                filename = "./{}/dim_reduction/umap_{}_{}_{}.pkl".format(
                    root_name, n_dim, type_txt, dataset
                )
                joblib.dump(
                    model_dim_red, open(filename, "wb",),
                )
            logging.info("Reducing the sent_emb to {} dim".format(n_dim))
            data_reduced = model_dim_red.transform(data)
            logging.info("Create the jobs")
        else:
            data_reduced = data
        for min_sample in all_clusters_to_do[n_dim]:
            list_cluster_size = all_clusters_to_do[n_dim][min_sample]
            p = multiprocessing.Process(
                target=getting_hdscan,
                args=(
                    cluster_foldername,
                    type_txt,
                    data_reduced,
                    n_dim,
                    dataset,
                    list_cluster_size,
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
        target=write_report, args=(queue_report, root_name, type_txt, dataset,),
    )
    writer.start()
    for i in tqdm(chunks(jobs, num_cpu), total=len(jobs)):
        for j in i:
            j.start()
        for j in i:
            j.join()
    queue_report.put("END")
