"""
This file is used to visualize the trajectory data.
Source: https://heremaps.github.io/pptk/tutorials/viewer/geolife.html
"""

import os
import os.path
import glob
import pandas as pd


def read_plt(plt_file: str):
    """
    Read a PLT file into a Pandas dataframe.

    Parameters
    ----------
    plt_file: str
        Path to the PLT file to parse
    """
    points = pd.read_csv(
        plt_file,
        skiprows=6,
        header=None,
        parse_dates=[[5, 6]],
        infer_datetime_format=True,
    )

    # for clarity rename columns
    points.rename(inplace=True, columns={"5_6": "time", 0: "lat", 1: "lon", 3: "alt"})

    # remove unused columns
    points.drop(inplace=True, columns=[2, 4])

    return points


mode_names = [
    "walk",
    "bike",
    "bus",
    "car",
    "subway",
    "train",
    "airplane",
    "boat",
    "run",
    "motorcycle",
    "taxi",
]
mode_ids = {s: i + 1 for i, s in enumerate(mode_names)}


def read_labels(labels_file: str):
    """
    Read a labels file into a Pandas dataframe.

    Parameters
    ----------
    labels_file: str
        Path to the labels file to parse
    """
    labels = pd.read_csv(
        labels_file,
        skiprows=1,
        header=None,
        parse_dates=[[0, 1], [2, 3]],
        infer_datetime_format=True,
        delim_whitespace=True,
    )

    # for clarity rename columns
    labels.columns = ["start_time", "end_time", "label"]

    # replace 'label' column with integer encoding
    labels["label"] = [mode_ids[i] for i in labels["label"]]

    return labels


def apply_labels(points: pd.DataFrame, labels: pd.DataFrame):
    """
    Apply labels to points dataframe.

    Parameters
    ----------
    points: pd.DataFrame
        Dataframe of points
    labels: pd.DataFrame
        Dataframe of labels
    """
    indices = labels["start_time"].searchsorted(points["time"], side="right") - 1
    no_label = (indices < 0) | (
        points["time"].values >= labels["end_time"].iloc[indices].values
    )
    points["label"] = labels["label"].iloc[indices].values
    points["label"][no_label] = 0


def read_user(user_folder: str) -> pd.DataFrame:
    """
    Read a user folder into a Pandas dataframe.

    Parameters
    ----------
    user_folder: str
        Path to the user folder to parse

    Returns
    -------
    pd.DataFrame
        Dataframe of points
    """
    labels = None

    plt_files = glob.glob(os.path.join(user_folder, "Trajectory", "*.plt"))

    try:
        df = pd.concat([read_plt(f) for f in plt_files])
    except ValueError as e:
        print("ValueError", e, "in", user_folder)
        return None

    labels_file = os.path.join(user_folder, "labels.txt")
    if os.path.exists(labels_file):
        labels = read_labels(labels_file)
        apply_labels(df, labels)
    else:
        df["label"] = 0

    return df


def read_all_users(folder):
    """
    Read all users into a single Pandas dataframe.

    Parameters
    ----------
    folder: str
        Path to the parent folder containing all user folders
    """
    subfolders = os.listdir(folder)
    dfs = []
    for i, sf in enumerate(subfolders):
        df = read_user(os.path.join(folder, sf))
        if df is None:
            print("skipping user", sf)
            continue
        print(f"[{i + 1}/{len(subfolders)}] processing user {sf}")
        df["user"] = int(sf)
        dfs.append(df)
    return pd.concat(dfs)
