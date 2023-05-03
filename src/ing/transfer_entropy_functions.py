"""
MIT License

Copyright (c) 2022 Chathura Jeewaka Jayalath Don Dimungu Arachchige (deamonpog)
                    Complex Adaptive Systems Laboratory
                    University of Central Florida

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

"""

import pandas as pd
import numpy as np
import datetime
import pyinform
import multiprocessing
import typing


def get_events_of_actor(actor_id, dataset_df, actors_df, indv_actors_df, comm_actors_df, plat_actors_df):
    """
    Returns the event list of the actor from the dataset.

    Parameters
    ----------
    actor_id: str
        actor id
    dataset_df: pd.DataFrame
        pandas dataframe containing the OSN messages as defined in Table 3.
    actors_df: pd.DataFrame
        actors dataframe which contains the actor_type. Index should be actor_id. Defined in Table 6.
    indv_actors_df : pd.DataFrame
        individual actors dataframe with actor_id as index. Defined in Table 7.
    comm_actors_df : pd.DataFrame
        community (or group) actors dataframe with actor_id as index. Defined in Table 8.
    plat_actors_df : pd.DataFrame
        platform actors dataframe with actor_id as index. Defined in Table 9.

    Returns
    -------
    pd.DataFrame
        a pandas dataframe that has same columns as Table 3 but filtered with only the events of the given actor_id
    """
    actor_type = actors_df.loc[actor_id]['actor_type']
    if actor_type == 'plat':
        plat = plat_actors_df.loc[actor_id][0]
        # print(f"{actor_id} is Platform: {plat}")
        return dataset_df[dataset_df['platform'] == plat]
    elif actor_type == 'indv':
        user_id = indv_actors_df.loc[actor_id][0]
        # print(f"{actor_id} is Individual: {user_id}")
        return dataset_df[dataset_df['user_id'] == user_id]
    elif actor_type == 'comm':
        user_list = comm_actors_df.loc[actor_id]['user_id']
        # if community has only one user dont run the code for it 
        # (detected by making sure we get a series for the user_list)
        # in such case we return a dataframe with 0 records
        if type(user_list) is pd.Series:
            user_list = user_list.values
            msg = f"{actor_id} is a Community of (size = {len(user_list)}) "
            print_limit = 10
            if len(user_list) <= print_limit:
                msg = f"{msg} : {user_list}"
            else:
                users = ' '.join([f"{u}" for u in np.random.choice(user_list, print_limit, replace=False)])
                msg = f"{msg} : [{users} ...]"
            #print(msg)
            return dataset_df[dataset_df['user_id'].isin(user_list)]
        else:
            return dataset_df[0:0]  # return 0 records
    else:
        raise Exception(f'Unknown actor type : {actor_id} -> {actor_type}\n{actors_df}')


def generate_timeseries_index(start_time, end_time, frequency):
    """
    Generates an index for a timeseries for a given start time, end time, and a time interval (frequency).

    Parameters
    ----------
    start_time: datetime.date
        start time of timeseries
    end_time: datetime.date
        end time of timeseries
    frequency: str
        Frequency value as a string. e.g. "12H", "D", "30min"

    Returns
    -------
    pd.DatetimeIndex
        generated datetime index
    """
    return pd.DatetimeIndex(pd.date_range(start=start_time, end=end_time, freq=frequency))


def resample_binary_timeseries(timeseries, time_index, frequency, classes):
    """
    Resamples the given timeseries (index must be a datetime) by the given frequency and fills values accordingly
     to match the given time_index series. The value column of the returned series will be binary
     (contain either 0 or 1). 0 says that nothing happened at that time interval, 1 says that something happened in
      that interval.

    Parameters
    ----------
    timeseries: pd.DataFrame
        a pandas dataframe with index set to a datetime.
    time_index: pd.DatetimeIndex
        The timeseries index for given frequency. This will be the index of the returned series
    frequency: str
        a string value representing the frequency of resampling. e.g. 'D', '12H', '15min'
    classes: typing.List[str]
        unique values of the 'class' column. e.g. ['UF','UM','TF','TM']

    Returns
    -------
    typing.Dict[str, np.ndarray]
        a dictionary containing resampled timeseries with time_index as its index for each class. (i.e. Class is the key and resampled timeseries is the value for each key.)
    """
    retval = {}
    for this_class in classes:
        retval[this_class] = timeseries[timeseries['class'] == this_class].resample(frequency).apply(
            lambda x: 1 if len(x) > 0 else 0).iloc[:, 0].rename('events').reindex(time_index, fill_value=0).values
    return retval


def multiprocess_resample_actor_binary_timeseries(ordered_actor_id_events_list, time_index, frequency, classes):
    """
    Calculates a binary timeseries for each event list in the given ordered_actor_id_events_list.
     Utilize the Multiprocessing Pools for fast execution over CPUs.
     Results contain an ordered list of binary timeservers of each respective actor event list in the input
     ordered_actor_id_events_list parameter
     (i.e. the order of ordered_actor_id_events_list corresponds to the order of results).

    Parameters
    ----------
    ordered_actor_id_events_list: typing.List[pd.DataFrame]
        a list that holds the events DataFrame of each actor_id. Order of results
         is correspondent to the order of event lists in this list.
    time_index: pd.DatetimeIndex
        a pandas series that will be used as the index for resampling the data
    frequency: str
        a string value representing the frequency of resampling. e.g. 'D', '12H', '15min'
    classes: typing.List[str]
        unique values of the 'class' column. e.g. ['UF','UM','TF','TM']

    Returns
    -------
    typing.List[typing.Dict[str, np.ndarray]]
        a list of dictionaries where each element is an array of binary values (0s and 1s) which represents
        the binary timeseries value that corresponds to each index in the time_index.
        For more information check the resample_binary_timeseries function that is being called by this function.
    """
    with multiprocessing.Pool(multiprocessing.cpu_count()) as p:
        results = p.starmap(resample_binary_timeseries,
                            [(actor_id_events.set_index('datetime'), time_index, frequency, classes) for actor_id_events
                             in
                             ordered_actor_id_events_list])
    return results


def calculate_te_values(src_actor_id, tgt_actor_id, src_timeseries_dict, tgt_timeseries_dict, classes):
    """
    Calculates the transfer entropy from the given source and target timeseries and returns a list that contains
     [Source, Target, TransferEntropy].

    Parameters
    ----------
    src_actor_id: str
        actor_id of Source
    tgt_actor_id: str
        actor_id of Target
    src_timeseries_dict: typing.Dict[str, np.ndarray]
        a dictionary containing, for each class: a binary timeseries of Source
    tgt_timeseries_dict: typing.Dict[str, np.ndarray]
        a dictionary containing, for each class: a binary timeseries of Target
    classes: typing.List[str]
        unique values of the 'class' column. e.g. ['UF','UM','TF','TM']

    Returns
    -------
    typing.List
        the list [Source actor_id, Target actor_id, TransferEntropyValueClass1ToClass2, TransferEntropyValueClass1ToClass3, ..., TotalTransferEntropy]
        Will have a typing of [ str, str, float, float, ..., float]
    """
    #print(f"[{src_actor_id} ==> {tgt_actor_id}]")
    # print(f"{src_timeseries.shape} ==> {tgt_timeseries.shape}")
    te_values_list = []
    total_te = 0
    for src_class in classes:
        for tgt_class in classes:
            this_te = pyinform.transfer_entropy(src_timeseries_dict[src_class], tgt_timeseries_dict[tgt_class], 1)
            te_values_list.append(this_te)
            total_te += this_te
    te_values_list.append(total_te)
    return [src_actor_id, tgt_actor_id] + te_values_list


def multiprocess_run_calculate_te_edge_list(ordered_actor_id_list, ordered_actor_timeseries_dict_list, classes):
    """
    Utilize multiprocessing Pool for calculating all transfer entropy values using the calculate_te_values function.
    Returns a list of calculate_te_values function returns for the provided input parameters.

    Parameters
    ----------
    ordered_actor_id_list: typing.List[str]
        the ordered list of actor_id values.
    ordered_actor_timeseries_dict_list: typing.Dict[str, np.ndarray]
        a dictionary of lists where each element is an array of binary values (0s and 1s)
        which represent the binary timeseries. keys are the classes.
    classes: typing.List[str]
        unique values of the 'class' column. e.g. ['UF','UM','TF','TM']
    Returns
    -------
    typing.List[typing.List]
        list of actor_id interactions with their corresponding transfer entropy values. (A list of outputs from the calculate_te_values function)
    """
    param_list = []
    for src_idx in range(len(ordered_actor_id_list)):
        for tgt_idx in range(len(ordered_actor_id_list)):
            if src_idx == tgt_idx:
                continue
            param_list.append((ordered_actor_id_list[src_idx], ordered_actor_id_list[tgt_idx],
                               ordered_actor_timeseries_dict_list[src_idx], ordered_actor_timeseries_dict_list[tgt_idx], classes))
    print(f"params ready. Count: {len(param_list)}")
    # with multiprocessing.Pool(multiprocessing.cpu_count() - 1) as p:
    #    results = p.starmap(calculate_te_values, param_list)
    results = []
    for param in param_list:
        r = calculate_te_values(param[0], param[1], param[2], param[3], param[4])
        results.append(r)
    print("mult proc done")
    return results


def generate_te_edge_list(actor_id_list, all_events_df, actors_df, indv_actors_df, comm_actors_df, plat_actors_df,
                          frequency, start_date, end_date, classes=['UF', 'UM', 'TF', 'TM']):
    """
    Calculates the transfer entropy based edge weights for the given set of actors.

    Parameters
    ----------
    actor_id_list :
    all_events_df :
    actors_df :
    indv_actors_df :
    comm_actors_df :
    plat_actors_df :
    frequency :
    start_date :
    end_date :
    classes :

    Returns
    -------

    """
    # generate time_index
    #start_date = all_events_df['datetime'].dt.date.min()
    #end_date = all_events_df['datetime'].dt.date.max() + datetime.timedelta(days=1)
    print(f"Filtering data available from {start_date} to {end_date}")
    filtered_events_df = all_events_df[ (start_date <= all_events_df['datetime']) & (all_events_df['datetime'] <= end_date) ]
    datetime_index = generate_timeseries_index(start_date, end_date, frequency)
    print("Running resampling timeseries calc...")
    # resample actor timeseries
    actor_timeseries_dict_list = multiprocess_resample_actor_binary_timeseries(
        [get_events_of_actor(actor_id, filtered_events_df, actors_df, indv_actors_df, comm_actors_df, plat_actors_df) for
         actor_id in actor_id_list],
        datetime_index, frequency, classes)
    print("Running TE edge list calc...")
    #print(actor_timeseries_dict_list)
    # calculate te values
    src_tgt_te_list = multiprocess_run_calculate_te_edge_list(actor_id_list, actor_timeseries_dict_list, classes)
    print("Calculation done. Creating dataframe...")
    te_col_names = []
    for src_class in classes:
        for tgt_class in classes:
            te_col_names.append(f"{src_class}_{tgt_class}")
    te_col_names.append("total_te")
    result_df = pd.DataFrame(src_tgt_te_list, columns=['Source', 'Target'] + te_col_names)
    return result_df
