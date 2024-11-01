import json
import logging
import os

import pandas as pd

logging.basicConfig(filename="job.log", format='%(asctime)s %(message)s', filemode='w')
logger = logging.getLogger("Survey Wrapper")
logger.setLevel(logging.DEBUG)


class Execute:
    """
    This Class is for executing survey percentage calculations and join 2 survey data.
    """

    def __init__(self, resource_path):
        self.survey_1 = pd.read_excel("prakriti.xlsx")

        self.survey_2 = pd.read_excel("vark.xlsx")

        self.conf = json.load(open("survey_config.json"))
        self.df = None
        self.output_df = None

    def set_survey_df(self, df):
        self.survey_2 = df

    @staticmethod
    def generate_unique_id(df, col_1, col_2):
        logger.info("Generating Unique Id")
        df['unique_id'] = df[col_1] + df['Batch'] + df[col_2].astype(str)
        df['unique_id'] = df['unique_id'].str.lower()

    def join_df(self):
        self.output_df = pd.merge(self.df, self.survey_2, how="inner", on=["unique_id"])
        self.output_df = self.output_df.drop('unique_id', axis=1)

        self.output_df = self.output_df.sort_values(by=['Roll number '], ascending=True)

    def response_processor_prakriti(self):
        logger.info("Calculating percentage for segments")
        self.survey_1 = self.survey_1.fillna('no')
        for ind, col in enumerate(self.survey_1.columns[9:]):
            index = ind + 1
            self.survey_1[col] = self.survey_1[col].str.lower()
            self.survey_1[col] = self.survey_1[col].apply(lambda x: self.conf[str(index)][x])

        self.df = self.survey_1.iloc[:, 2:8]
        self.df['sum_kapha'] = self.survey_1.iloc[:, 9:32].sum(axis=1)
        self.df['sum_pitta'] = self.survey_1.iloc[:, 32:48].sum(axis=1)
        self.df['sum_vata'] = self.survey_1.iloc[:, 48:].sum(axis=1)
        self.df['final_kapha%'] = (self.df['sum_kapha'] / self.conf['total']['kapha']) * 100
        self.df['final_pitta%'] = (self.df['sum_pitta'] / self.conf['total']['pitta']) * 100
        self.df['final_vata%'] = (self.df['sum_vata'] / self.conf['total']['vata']) * 100

    def df_writer(self):
        sheet_name = 'Sheet1'
        # self.output_df.to_excel("output.xlsx")
        if os.path.exists("output.xlsx"):
            # Create a Pandas Excel writer using openpyxl as the engine
            with pd.ExcelWriter("output.xlsx", engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
                # Write the DataFrame to the specified sheet, appending to existing data
                self.output_df.to_excel(writer, sheet_name=sheet_name, startrow=writer.sheets[sheet_name].max_row,
                                        index=False, header=False)
        else:
            # If the file does not exist, create a new one
            with pd.ExcelWriter("output.xlsx", engine='openpyxl') as writer:
                self.output_df.to_excel(writer, sheet_name=sheet_name, index=False)

    def flow_executor(self):
        logger.info("Processing Prakriti Survey Data")
        self.response_processor_prakriti()
        self.generate_unique_id(self.df, 'Name of the college ', 'Roll number ')
        self.generate_unique_id(self.survey_2, 'Name of the college ', 'Roll Number')
