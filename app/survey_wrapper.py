import time

from processor import *


def count_occurrences(row):
    """
    This Function is to get the value which is repeated maximum in each row of the dataframe
    :param row: Each row of dataframe
    :return: pd.Series with max repeated values
    """
    counts = {
        'a': row.str.count('a').sum(),
        'b': row.str.count('b').sum(),
        'c': row.str.count('c').sum(),
        'd': row.str.count('d').sum()
    }
    # Determine the character with the maximum count
    tmp = max(counts.values())
    res = [key for key in counts if counts[key] == tmp]
    value = ', '.join(res)
    return pd.Series(value, index=['max_val'])


def response_processor_vark(response_2):
    logger.info("Calculating Max values : [a,b,c,d]")
    row_counts = response_2.iloc[:, 5:17].apply(count_occurrences, axis=1)

    # Combine row counts with the original DataFrame
    df1 = pd.concat([response_2, row_counts], axis=1)

    return df1


if __name__ == "__main__":
    """
    This program is for initiating wrapper workflow for joining two survey datasets
    """
    # Flow executor to wrap two medical survey datasets.
    logger.info("Starting Wrapper Job")
    resource_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'resources', 'input') + os.path.sep

    Exec = Execute(resource_path)
    Exec.flow_executor()

    logger.info("Processing Vark Survey Data")
    df = response_processor_vark(Exec.survey_2)
    temp = df.loc[:, ['unique_id', 'max_val']]

    Exec.set_survey_df(temp)
    Exec.join_df()

    logger.info("Writing to output.xlsx")
    Exec.df_writer()
    time.sleep(3)
    logger.info("Job Completed. Please check output.xlsx")
