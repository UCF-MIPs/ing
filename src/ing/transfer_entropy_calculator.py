import datetime
import multiprocessing
from typing import Dict, List, Tuple, Union
import pyinform
import pandas as pd
import numpy as np

from .data_manager import DataManager


def compute_super_class_timeseries(in_class_to_timeseries):
    superclass_to_timeseries = {"T": np.array(np.logical_or(in_class_to_timeseries["TF"], in_class_to_timeseries["TM"]),
                                              dtype=np.int32),
                                "U": np.array(np.logical_or(in_class_to_timeseries["UF"], in_class_to_timeseries["UM"]),
                                              dtype=np.int32),
                                "F": np.array(np.logical_or(in_class_to_timeseries["TF"], in_class_to_timeseries["UF"]),
                                              dtype=np.int32),
                                "M": np.array(np.logical_or(in_class_to_timeseries["TM"], in_class_to_timeseries["UM"]),
                                              dtype=np.int32)}
    return superclass_to_timeseries


def get_actor_time_series(in_actor_id: str, in_data_manager: DataManager, in_datetime_index: pd.DatetimeIndex,
                          in_frequency, in_add_superclasses: bool) -> Dict[str, np.ndarray]:
    """
    Given an actor_id returns the timeseries for each class for the actor.
    Parameters
    ----------
    in_actor_id :
        The actor_id
    in_frequency :
        Frequency stirng. examples: 'D', '6H'
    in_datetime_index :
        A datetime index generated by pd.date_range for fixing the index and length issues due to data variations.
    in_data_manager :
        The DataManager object that provides all required data.
    in_add_superclasses :
        Calculate the superclasses T, U, F, M as well.
    Returns
    -------
        Dictionary containing timeseries of the actor for each class.
    """
    print(f" E:{in_actor_id} ", end=" ")
    actor_events_df = in_data_manager.get_actors_msgs(in_actor_id, True)
    class_to_timeseries = {}
    for this_class in ["TF", "TM", "UF", "UM"]:
        class_column = f"class_{this_class}"
        actors_binary_timeseries = actor_events_df[actor_events_df[class_column] > 0].set_index(
            "datetime").resample(in_frequency).size().apply(lambda x: 1 if x > 0 else 0).rename("events").reindex(
            in_datetime_index, fill_value=0).values
        class_to_timeseries[this_class] = actors_binary_timeseries
    if in_add_superclasses:
        class_to_timeseries.update(compute_super_class_timeseries(class_to_timeseries))
    class_to_timeseries["*"] = actor_events_df.set_index("datetime").resample(in_frequency).size().apply(
        lambda x: 1 if x > 0 else 0).rename("events").reindex(in_datetime_index, fill_value=0).values
    return class_to_timeseries


def calculate_transfer_entropy_data(in_src_idx: int, in_src_actor_id: str,
                                    in_tgt_idx: int, in_tgt_actor_id: str,
                                    in_comparison_pairs_list: List[Tuple[str]],
                                    in_actor_timeseries_dict_list: List[Dict[str, np.ndarray]]) -> List[Union[str, float]]:
    """
    Calculates the comparison pair values using transfer entorpy and prepares it for directly using as a row on
    actor_te_edges_df dataframe.

    Parameters
    ----------
    in_src_idx :
        Index of the source actor in the in_actor_timeseries_dict_list
    in_src_actor_id :
        actor_id of the source actor
    in_tgt_idx :
        Index of the target actor in the in_actor_timeseries_dict_list
    in_tgt_actor_id :
        actor_id of the target actor
    in_comparison_pairs_list :
        comparison pairs list of classes
    in_actor_timeseries_dict_list :
        A list which contains "actor timeseries dicts"

    Returns
    -------
        A list which contains [src_actor_id, tgt_actor_id, te_val0, te_val1, ...., te_valN]
        Where src_actor_id, and tgt_actor_id are actor ids of source and target actors, and
         te_valN is the corresponding TE value for the given comparison pair at Nth index in in_comparison_pairs_list.
    """
    print(f" {in_src_idx}->{in_tgt_idx} ", end=" ")
    te_values_list = [pyinform.transfer_entropy(in_actor_timeseries_dict_list[in_src_idx][src_class],
                                                in_actor_timeseries_dict_list[in_tgt_idx][tgt_class], 1) for
                      src_class, tgt_class in in_comparison_pairs_list]
    data_row = [in_src_actor_id, in_tgt_actor_id]
    data_row.extend(te_values_list)
    return data_row


class TransferEntropyCalculator:

    def __init__(self, in_data_manager: DataManager, in_sub_classes: List[str] = None,
                 in_add_superclasses: bool = True):
        if in_sub_classes is None:
            if in_add_superclasses:
                in_sub_classes = ["TF", "TM", "UF", "UM", "T", "U", "F", "M", "*"]
            else:
                in_sub_classes = ["TF", "TM", "UF", "UM", "*"]
        self.data_manager = in_data_manager
        self.add_superclasses = in_add_superclasses
        self.start_date = None
        self.end_date = None
        self.frequency = None
        self.datetime_index = None
        self.__init_comparison_pairs_list(in_sub_classes)

    @staticmethod
    def calculate_a_date_series(in_start_date: datetime.datetime, in_end_date: datetime.datetime,
                                in_shift_days: int, in_init_window_days: int, in_as_growing: bool):
        # Get the moving/growing window start points for the given duration by the given shift size
        print(f"Start Date : {in_start_date}")
        print(f"End Date : {in_end_date}")
        print(f"Window Shift By : {in_shift_days}")
        print(f"Init Window Size : {in_init_window_days}")
        dynamic_windows = pd.date_range(start=in_start_date, end=in_end_date, freq=f"{in_shift_days}D",
                                        inclusive='left')
        # print(dynamic_windows)
        init_window = datetime.timedelta(days=in_init_window_days)
        # Moving/Growing windows start and end dates
        dwindows_df = pd.DataFrame({
            'start_date': pd.Series(dynamic_windows[0] if in_as_growing else dw for dw in dynamic_windows),
            'end_date': pd.Series([dw + init_window for dw in dynamic_windows])
        })
        # remove days that are out of end date boundary
        dwindows_df = dwindows_df[dwindows_df['end_date'] < in_end_date + datetime.timedelta(days=in_shift_days)]
        return dwindows_df

    # def calculate_te_network_series(self, in_actor_id_list: List[str],
    #                                 in_start_date: datetime.datetime,
    #                                 in_end_date: datetime.datetime,
    #                                 in_frequency: str,
    #                                 in_window_shift_by_days):
    #     # Get the moving/growing window start points for the given duration by the given shift size
    #     print(f"Start Date : {in_start_date}")
    #     print(f"End Date : {in_end_date}")
    #     print(f"Window Shift By : {WINDOW_SHIFT_BY_DAYS}")
    #     print(f"Init Window Size : {INIT_WINDOW_SIZE}")
    #     dynamic_windows = pd.date_range(start=in_start_date, end=in_end_date, freq=f"{WINDOW_SHIFT_BY_DAYS}D",
    #                                     inclusive='left')
    #     print(dynamic_windows)
    #
    #     # Moving/Growing windows start and end dates
    #     IS_GROWING_WINDOW = True
    #     dwindows_df = pd.DataFrame({
    #         'start_date': pd.Series(dynamic_windows[0] if IS_GROWING_WINDOW else dw for dw in dynamic_windows),
    #         'end_date': pd.Series([dw + INIT_WINDOW_SIZE for dw in dynamic_windows])
    #     })
    #     # remove days that are out of end date boundary
    #     dwindows_df = dwindows_df[dwindows_df['end_date'] < in_end_date + datetime.timedelta(days=WINDOW_SHIFT_BY_DAYS)]
    #     dwindows_df

    def calculate_te_network_step1(self, in_actor_id_list: List[str], in_start_date: datetime.datetime, in_end_date: datetime.datetime, in_frequency: str):
        self.start_date = in_start_date
        self.end_date = in_end_date
        self.frequency = in_frequency
        self.datetime_index = pd.date_range(start=self.start_date, end=self.end_date, freq=self.frequency)

        self.data_manager.filter_osn_msgs_view(self.start_date, self.end_date)

        print("calculating actor timeseries dictionaries...")
        actor_timeseries_dict_list = self._calculate_actor_to_timeseries_dict_list(in_actor_id_list)
        return actor_timeseries_dict_list

    def calculate_te_network_step2(self, in_actor_id_list: List[str], actor_timeseries_dict_list):
        print("calculating te sets...")
        all_te_data = self._calculate_transfer_entropy_sets(in_actor_id_list, actor_timeseries_dict_list)
        print("creating dataframe...")
        return pd.DataFrame(all_te_data,
                            columns=["Source", "Target"] + [f"{src}_{tgt}" for src, tgt in self.comparison_pairs_list])

    def __init_comparison_pairs_list(self, in_classes: List[str]):
        self.comparison_pairs_list = []
        # c = 0
        for src in in_classes:
            for tgt in in_classes:
                if (src == "*" and tgt == "*") or \
                        (src in {"T", "U"} and tgt in {"T", "U"}) or \
                        (src in {"F", "M"} and tgt in {"F", "M"}) or \
                        (src in {"TF", "TM", "UF", "UM"} and tgt in {"TF", "TM", "UF", "UM"}):
                    self.comparison_pairs_list.append((src, tgt))
                    # print(c, src, tgt, f"{src}->{tgt}")
                    # c += 1

    def _calculate_transfer_entropy_sets(self, in_actor_id_list: List[str], in_actor_timeseries_dict_list: List[Dict[str, np.ndarray]]):
        params_list = [[src_idx, in_actor_id_list[src_idx],
                        tgt_idx, in_actor_id_list[tgt_idx],
                        self.comparison_pairs_list, in_actor_timeseries_dict_list]
                       for src_idx in range(len(in_actor_id_list))
                       for tgt_idx in range(len(in_actor_id_list))
                       if src_idx != tgt_idx]
        with multiprocessing.Pool(multiprocessing.cpu_count() - 1) as p:
            results = p.starmap(calculate_transfer_entropy_data, params_list)
        return results

    def _calculate_actor_to_timeseries_dict_list(self, in_actor_id_list: List[str]) -> List[Dict[str, np.ndarray]]:
        """
        Calculates a list of dictionaries.
        Order of the list is correspondent to the index of actors_df of DataManager
        Each dictionary is an output of the get_actor_time_series function.
        Returns
        -------
            A list containing the timeseries dicts of each actor
        """
        params_list = [[actor_id, self.data_manager, self.datetime_index, self.frequency, self.add_superclasses]
                       for actor_id in in_actor_id_list]
        with multiprocessing.Pool(multiprocessing.cpu_count() - 1) as p:
            results = p.starmap(get_actor_time_series, params_list)
        return results
